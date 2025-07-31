package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import java.time.Duration
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.util.retry.Retry

/**
 * Service responsible for dispatching and processing transaction status change events.
 *
 * Processes CDC events by updating transaction views with event data using upsert operations.
 * Handles event payload updates while maintaining retry logic and error handling.
 */
@Component
class EcommerceCDCEventDispatcherService(
    private val retrySendPolicyConfig: RetrySendPolicyConfig,
    private val transactionViewUpsertService: TransactionViewUpsertService,
) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    /**
     * Dispatches a collection change event for processing. Logs event details and performs upsert
     * operations on the transaction view.
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

                processTransactionEvent(event)
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
     * Processes the transaction event by performing upsert operations on the transaction view.
     * Updates event payload data and logs detailed event information.
     *
     * @param event The transaction change document
     * @return Mono<Document> The processed document
     */
    private fun processTransactionEvent(event: Document): Mono<Document> {
        val eventId = event.getString("_id")
        val transactionId = event.getString("transactionId")
        val eventCode = event.getString("eventCode")
        val creationDate = event.getString("creationDate")

        // extract data from nested 'data' field if present
        val data = event.get("data") as? Document
        val paymentNotices = data?.get("paymentNotices") as? List<*>
        val clientId = data?.getString("clientId")

        logger.info(
            "CDC Event Details: transactionId: [{}], eventId: [{}], eventCode: [{}], creationDate: [{}], clientId: [{}], paymentNotices: [{}]",
            transactionId,
            eventId,
            eventCode,
            creationDate,
            clientId,
            paymentNotices?.size ?: 0,
        )

        // upsert operation if the transactionId is valid
        return if (transactionId != null && transactionId != "unknown") {
            transactionViewUpsertService
                .upsertEventData(transactionId, event)
                .doOnSuccess {
                    logger.debug(
                        "Successfully upserted transaction view for eventId: [{}], transactionId: [{}]",
                        eventId,
                        transactionId,
                    )
                }
                .doOnError { error ->
                    logger.error(
                        "Failed to upsert transaction view for eventId: [{}], transactionId: [{}]",
                        eventId,
                        transactionId,
                        error,
                    )
                }
                .then(Mono.just(event))
        } else {
            logger.warn(
                "Skipping upsert for event with invalid transactionId: [{}], eventId: [{}]",
                transactionId,
                eventId,
            )
            Mono.just(event)
        }
    }
}
