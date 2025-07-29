package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.cdc.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import it.pagopa.ecommerce.commons.documents.PaymentTransferInformation
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionActivatedData
import it.pagopa.ecommerce.commons.domain.Confidential
import it.pagopa.ecommerce.commons.domain.v2.*
import java.time.Duration
import kotlinx.coroutines.reactor.mono
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.util.retry.Retry

/**
 * Service responsible for dispatching and processing transaction status change events.
 *
 * Currently implements logging-only functionality as per initial requirements. Future iterations
 * will add queue/topic publishing capabilities
 */
@Component
class EcommerceCDCEventDispatcherService(
    @Autowired private val viewRepository: TransactionsViewRepository,
    private val retrySendPolicyConfig: RetrySendPolicyConfig,
) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    /**
     * Dispatches a collection change event for processing. Currently logs the event details in
     * structured format.
     *
     * @param event The MongoDB change stream document containing eventstore data
     * @return Mono<Document> The processed document
     */
    fun dispatchEvent(event: Document): Mono<Document> =
        Mono.defer {
                // extract document fields
                val transactionId = event.getString("transactionId") ?: "unknown"
                val eventClass = event.getString("_class") ?: "unknown"
                val creationDate = event.getString("creationDate") ?: "unknown"

                logger.info(
                    "Handling new change stream event: transactionId: [{}], eventType: [{}], creationDate: [{}]",
                    transactionId,
                    eventClass,
                    creationDate,
                )

                // TODO tracing + send event to queue/topic
                processTransactionEvent(viewRepository, event)
            }
            .retryWhen(
                Retry.fixedDelay(
                        retrySendPolicyConfig.maxAttempts,
                        Duration.ofMillis(retrySendPolicyConfig.intervalInMs),
                    )
                    .filter { t -> t is Exception }
                    .doBeforeRetry { signal ->
                        logger.warn(
                            "Retrying writing event on CDC queue due to: [{}]",
                            signal.failure().message,
                        )
                    }
            )
            .doOnError { e -> logger.error("Failed to send event after retries", e) }
            .map { event }

    /**
     * Processes the transaction event based on its type and content. Currently implements
     * structured logging for all event types.
     *
     * @param event The transaction change document
     * @return Mono<Document> The processed document
     */
    private fun processTransactionEvent(
        viewRepository: TransactionsViewRepository,
        event: Document,
    ): Mono<Document> {
        return Mono.fromCallable {
                val eventId = event.getString("_id")
                val transactionId = event.getString("transactionId")
                val eventCode = event.getString("eventCode")
                val creationDate = event.getString("creationDate")

                // extract data from nested 'data' field if present
                val data = event.get("data") as? TransactionActivatedData
                val paymentNotices = data!!.paymentNotices
                val clientId = data.clientId

                logger.info(
                    "CDC Event Details: transactionId: [{}], eventId: [{}], eventCode: [{}], creationDate: [{}], clientId: [{}], paymentNotices: [{}]",
                    transactionId,
                    eventId,
                    eventCode,
                    creationDate,
                    clientId,
                    paymentNotices?.size ?: 0,
                )

                when (eventCode) {
                    "ACTIVATED" -> {
                        val id = TransactionId(transactionId)
                        val paymentNoticeList: MutableList<PaymentNotice?> =
                            data.paymentNotices
                                .stream()
                                .map {
                                    paymentNoticeData:
                                        it.pagopa.ecommerce.commons.documents.PaymentNotice? ->
                                    PaymentNotice(
                                        PaymentToken(paymentNoticeData!!.paymentToken),
                                        RptId(paymentNoticeData.rptId),
                                        TransactionAmount(paymentNoticeData.amount),
                                        TransactionDescription(paymentNoticeData.description),
                                        PaymentContextCode(paymentNoticeData.paymentContextCode),
                                        paymentNoticeData.transferList
                                            .stream()
                                            .map { transfer: PaymentTransferInformation? ->
                                                PaymentTransferInfo(
                                                    transfer!!.paFiscalCode,
                                                    transfer.digitalStamp,
                                                    transfer.transferAmount,
                                                    transfer.transferCategory,
                                                )
                                            }
                                            .toList(),
                                        paymentNoticeData.isAllCCP,
                                        CompanyName(paymentNoticeData.companyName),
                                        paymentNoticeData.creditorReferenceId,
                                    )
                                }
                                .toList()
                        val email: Confidential<Email?>? = data.email
                        val faultCode: String? = data.faultCode
                        val faultCodeString: String? = data.faultCodeString
                        val clientId: Transaction.ClientId? = data.clientId
                        val idCart: String? = data.idCart
                        val paymentTokenValiditySeconds: Int = data.paymentTokenValiditySeconds
                        val userId = data.userId

                        viewRepository
                            .findByTransactionId(transactionId)
                            .switchIfEmpty(
                                mono {
                                        TransactionActivated(
                                            id,
                                            paymentNoticeList,
                                            email,
                                            faultCode,
                                            faultCodeString,
                                            clientId,
                                            idCart,
                                            paymentTokenValiditySeconds,
                                            data.transactionGatewayActivationData,
                                            data.userId,
                                        )
                                    }
                                    .map { it -> Transaction.from(it) }
                            )
                            .map { it -> it as Transaction }
                            .map { it ->
                                it.paymentNotices = paymentNotices
                                it.clientId = clientId
                                it.email = email
                                it.creationDate = creationDate
                                it.userId = userId
                                it.idCart = idCart
                            }
                            .cast(BaseTransactionView::class.java)
                            .flatMap { t -> viewRepository.save(t) }
                    }
                }

                // TODO future iterations will add:
                // - Queue/topic publishing
                // - View updates with Redis caching

                event
            }
            .doOnSuccess {
                logger.debug(
                    "Successfully processed eventstore event: [{}]",
                    event.getString("_id"),
                )
            }
    }
}
