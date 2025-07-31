package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import java.time.ZonedDateTime
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Service for performing upsert operations on transaction views with efficient atomic upsert
 * operations using native MongoDB queries.
 */
@Service
class TransactionViewUpsertService(
    private val mongoTemplate: ReactiveMongoTemplate,
    @Value($$"${ecommerce.transactionView.collection.name}") private val transactionViewName: String,
) {

    private val logger = LoggerFactory.getLogger(TransactionViewUpsertService::class.java)

    /**
     * Performs an upsert operation for transaction view data based on the event content. Uses a
     * single atomic upsert operation without additional database reads for optimal performance.
     *
     * @param transactionId The transaction identifier
     * @param event The MongoDB change stream event document
     * @return Mono<Void> Completes when the upsert operation succeeds
     */
    fun upsertEventData(transactionId: String, event: TransactionEvent<*>): Mono<Unit> {
        return Mono.defer {
            val eventCode = event.eventCode

            logger.debug(
                "Upserting transaction view data for _id: [{}], eventCode: [{}]",
                transactionId,
                eventCode,
            )

            val queryByTransactionId =
                Query.query(Criteria.where("transactionId").`is`(transactionId))

            val queryByTransactionAndLastProcessedEventAtCondition =
                Query.query(Criteria.where("transactionId").`is`(transactionId))
                    .addCriteria(
                        Criteria.where("lastProcessedEventAt")
                            .exists(false)
                            .orOperator(
                                Criteria.where("lastProcessedEventAt")
                                    .lt(
                                        ZonedDateTime.parse(event.creationDate)
                                            .toInstant()
                                            .toEpochMilli()
                                    )
                            )
                    )

            buildUpdateFromEvent(event)
                .flatMap { (update, updateStatus) ->
                    mongoTemplate
                        .upsert(
                            queryByTransactionAndLastProcessedEventAtCondition,
                            updateStatus,
                            BaseTransactionView::class.java,
                            transactionViewName,
                        )
                        .filter { it -> it.matchedCount > 0 }
                        .switchIfEmpty(
                            Mono.justOrEmpty(update).flatMap { upd ->
                                mongoTemplate.upsert(
                                    queryByTransactionId,
                                    upd!!,
                                    BaseTransactionView::class.java,
                                    transactionViewName,
                                )
                            }
                        )
                }
                .doOnNext { updateResult ->
                    logger.debug(
                        "Upsert completed for transactionId: [{}], eventCode: [{}] - matched: {}, modified: {}, upserted: {}",
                        transactionId,
                        eventCode,
                        updateResult.matchedCount,
                        updateResult.modifiedCount,
                        updateResult.upsertedId != null,
                    )
                }
                .thenReturn(Unit)
        }
            .doOnSuccess { _ ->
                logger.info("Successfully upserted transaction view for _id: [{}]", transactionId)
            }
            .doOnError { error ->
                logger.error(
                    "Failed to upsert transaction view for _id: [{}]",
                    transactionId,
                    error,
                )
            }
    }

    /**
     * Builds MongoDB Update object based on the event type and content. Different events update
     * different portions of the transaction view document.
     *
     * @param event The MongoDB change stream event document
     * @return Update object with field updates based on event type
     */
    private fun buildUpdateFromEvent(event: TransactionEvent<*>): Mono<Pair<Update?, Update>> {
        val update = Update()
        val eventCode = event.eventCode
        // apply updates based on specific event types
        val result =
            when (event) {
                is TransactionActivatedEvent -> updateActivationData(update, event)
                is TransactionAuthorizationRequestedEvent -> updateAuthRequestData(update, event)
                is TransactionAuthorizationCompletedEvent -> updateAuthCompletedData(update, event)
                is TransactionUserReceiptRequestedEvent -> updateUserReceiptData(update, event)
                is TransactionClosedEvent -> updateClosedData(update, event)
                is TransactionClosureErrorEvent -> updateClosureErrorData(update, event)
                is TransactionClosureRetriedEvent -> updateClosureRetriedData(update, event)
                is TransactionExpiredEvent -> updateExpiredData(update, event)
                is TransactionRefundRequestedEvent -> updateRefundRequestData(update, event)
                is TransactionUserCanceledEvent -> updateUserCanceledData(update, event)
                is TransactionClosureRequestedEvent -> updateClosureRequestData(update, event)
                is TransactionRefundErrorEvent -> updateRefundErrorData(update, event)
                is TransactionUserReceiptAddedEvent -> updateUserReceiptAddedData(update, event)
                is TransactionUserReceiptAddErrorEvent -> updateUserReceiptErrorData(update, event)
                is TransactionClosureFailedEvent -> updateClosureFailedData(update, event)
                is TransactionRefundedEvent -> updateRefundedData(update, event)
                is TransactionRefundRetriedEvent -> updateRefundRetriedData(update, event)
                is TransactionUserReceiptAddRetriedEvent ->
                    updateUserReceiptRetryData(update, event)

                else -> {
                    logger.warn(
                        "Unhandled event with code: [{}]. Event class: [{}]",
                        eventCode,
                        event.javaClass,
                    )
                    null
                }
            }

        return mono { result }
                as Mono<Pair<Update?, Update>> // FIXME Verificare perchè restituisce Any
    }

    /**
     * Updates fields for TRANSACTION_ACTIVATED_EVENT. This creates the initial transaction view
     * document with all basic transaction information.
     */
    private fun updateActivationData(
        update: Update,
        event: TransactionActivatedEvent,
    ): Pair<Update?, Update> {
        val data = event.data
        update["email"] = data.email.opaqueData
        update["paymentNotices"] = data.paymentNotices
        update["clientId"] = data.clientId
        update["creationDate"] = event.creationDate
        update["_class"] = Transaction::class.java.canonicalName

        val statusUpdate = update
        statusUpdate["status"] = TransactionStatusDto.ACTIVATED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(update, statusUpdate)
    }

    /**
     * Updates fields for TRANSACTION_AUTHORIZATION_REQUESTED_EVENT. Adds payment gateway
     * information and authorization details.
     */
    private fun updateAuthRequestData(
        update: Update,
        event: TransactionAuthorizationRequestedEvent,
    ): Pair<Update?, Update> {
        val authorizationRequestedData = event.data
        update["paymentGateway"] = authorizationRequestedData.paymentGateway
        update["paymentTypeCode"] = authorizationRequestedData.paymentTypeCode
        update["pspId"] = authorizationRequestedData.pspId
        update["feeTotal"] = authorizationRequestedData.fee

        val statusUpdate = update
        statusUpdate["status"] = TransactionStatusDto.AUTHORIZATION_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(update, statusUpdate)
    }

    /**
     * Updates fields for TRANSACTION_AUTHORIZATION_COMPLETED_EVENT. Adds authorization results and
     * gateway response information.
     */
    private fun updateAuthCompletedData(
        update: Update,
        event: TransactionAuthorizationCompletedEvent,
    ): Pair<Update?, Update> {
        val data = event.data

        update["rrn"] = data.rrn
        update["authorizationCode"] = data.authorizationCode

        val gatewayAuthData = data.transactionGatewayAuthorizationData

        when (gatewayAuthData) {
            is NpgTransactionGatewayAuthorizationData -> {
                update["gatewayAuthorizationStatus"] = gatewayAuthData.operationResult
                update["authorizationErrorCode"] = gatewayAuthData.errorCode
            }

            is RedirectTransactionGatewayAuthorizationData -> {
                update["gatewayAuthorizationStatus"] = gatewayAuthData.outcome
                update["authorizationErrorCode"] = gatewayAuthData.errorCode
            }

            else ->
                logger.warn(
                    "Unhandled transaction gateway authorization data: [{}]",
                    gatewayAuthData::class.java,
                )
        }

        val statusUpdate = update
        statusUpdate["status"] = TransactionStatusDto.AUTHORIZATION_COMPLETED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(update, statusUpdate)
    }

    /**
     * Updates fields for TRANSACTION_CLOSURE_REQUESTED_EVENT. Adds closure information and outcome
     * details.
     */
    private fun updateClosureRequestData(
        update: Update,
        event: TransactionClosureRequestedEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.CLOSURE_REQUESTED
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /**
     * Updates fields for TRANSACTION_USER_RECEIPT_REQUESTED_EVENT. Adds user receipt information.
     */
    private fun updateUserReceiptData(
        update: Update,
        event: TransactionUserReceiptRequestedEvent,
    ): Pair<Update?, Update> {
        update["sendPaymentResultOutcome"] = event.data.responseOutcome

        val statusUpdate = update
        statusUpdate["status"] = TransactionStatusDto.NOTIFICATION_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(update, statusUpdate)
    }

    /** Updates fields for TRANSACTION_EXPIRED_EVENT. Adds expiration information. */
    private fun updateExpiredData(
        update: Update,
        event: TransactionExpiredEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.EXPIRED
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /** Updates fields for TRANSACTION_REFUND_REQUESTED_EVENT. Adds refund request information. */
    private fun updateRefundRequestData(
        update: Update,
        event: TransactionRefundRequestedEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.REFUND_REQUESTED
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /**
     * Updates fields for TRANSACTION_CLOSED_EVENT. Sets sendPaymentResultOutcome and closure
     * timestamp.
     */
    private fun updateClosedData(update: Update, event: TransactionClosedEvent): Update {
        if (event.data.responseOutcome == TransactionClosureData.Outcome.OK) {
            update["sendPaymentResultOutcome"] = TransactionUserReceiptData.Outcome.NOT_RECEIVED
            update["closureErrorData"] = null
        }
        // FIXME Ci manca la reduce degli eventi o la view o l'evento precedente, valutare find
        // apposita
        return update
    }

    /** Updates fields for TRANSACTION_USER_CANCELED_EVENT. Adds cancellation timestamp. */
    private fun updateUserCanceledData(
        update: Update,
        event: TransactionUserCanceledEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.CANCELLATION_REQUESTED
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /** Updates fields for TRANSACTION_REFUND_ERROR_EVENT. Adds refund error information. */
    private fun updateRefundErrorData(
        update: Update,
        event: TransactionRefundErrorEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.REFUND_ERROR
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /** Updates fields for TRANSACTION_CLOSURE_ERROR_EVENT. Adds closure error timestamp. */
    private fun updateClosureErrorData(
        update: Update,
        event: TransactionClosureErrorEvent,
    ): Pair<Update?, Update> {
        update["closureErrorData"] = event.data
        update["status"] = TransactionStatusDto.CLOSURE_ERROR
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /** Updates fields for TRANSACTION_USER_RECEIPT_ADDED_EVENT. Sets notification outcome. */
    private fun updateUserReceiptAddedData(
        update: Update,
        event: TransactionUserReceiptAddedEvent,
    ): Pair<Update?, Update> {
        when (event.data.responseOutcome) {
            TransactionUserReceiptData.Outcome.OK ->
                update["status"] = TransactionStatusDto.NOTIFIED_OK
            TransactionUserReceiptData.Outcome.KO ->
                update["status"] = TransactionStatusDto.NOTIFIED_KO
            else -> {} // throw exception
        }
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(null, update)
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT. Adds receipt error information.
     */
    private fun updateUserReceiptErrorData(
        update: Update,
        event: TransactionUserReceiptAddErrorEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.NOTIFICATION_ERROR
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /** Updates fields for TRANSACTION_CLOSURE_RETRIED_EVENT. Adds closure retry information. */
    private fun updateClosureRetriedData(
        update: Update,
        event: TransactionClosureRetriedEvent,
    ): Pair<Update?, Update> {
        update["sendPaymentResultOutcome"] = TransactionUserReceiptData.Outcome.NOT_RECEIVED
        if (event.data.closureErrorData != null) {
            update["closureErrorData"] = event.data.closureErrorData
        }
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(
            null,
            update,
        ) // Doesn't update the state but it has to be processed coditionally on its timestamp
    }

    /** Updates fields for TRANSACTION_CLOSURE_FAILED_EVENT. Adds closure failure information. */
    private fun updateClosureFailedData(
        update: Update,
        event: TransactionClosureFailedEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.UNAUTHORIZED
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /** Updates fields for TRANSACTION_REFUNDED_EVENT. Adds refund completion information. */
    private fun updateRefundedData(
        update: Update,
        event: TransactionRefundedEvent,
    ): Pair<Update?, Update> {
        update["status"] = TransactionStatusDto.REFUNDED
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, update)
    }

    /** Updates fields for TRANSACTION_REFUND_RETRIED_EVENT. Adds refund retry information. */
    private fun updateRefundRetriedData(
        update: Update,
        event: TransactionRefundRetriedEvent,
    ): Pair<Update?, Update> {
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(
            null,
            update,
        ) // Doesn't update the state but it has to be processed coditionally on its timestamp.
        // Maybe it could be skipped
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_RETRY_EVENT. Adds receipt retry information.
     */
    private fun updateUserReceiptRetryData(
        update: Update,
        event: TransactionUserReceiptAddRetriedEvent,
    ): Pair<Update?, Update> {
        update["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(
            null,
            update,
        ) // Doesn't update the state but it has to be processed coditionally on its timestamp.
        // Maybe it could be skipped
    }
}
