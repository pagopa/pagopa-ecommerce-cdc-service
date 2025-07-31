package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
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

            val query = Query.query(Criteria.where("transactionId").`is`(transactionId))
            val update = buildUpdateFromEvent(event)
            Mono.justOrEmpty(update)
                .flatMap { updateDefinition ->
                    mongoTemplate.upsert(
                        query,
                        updateDefinition!!,
                        BaseTransactionView::class.java,
                        transactionViewName,
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
    private fun buildUpdateFromEvent(event: TransactionEvent<*>): Update? {
        val update = Update()
        val eventCode = event.eventCode
        // apply updates based on specific event types
        when (event) {
            is TransactionActivatedEvent -> updateActivationData(update, event)
            is TransactionAuthorizationRequestedEvent -> updateAuthRequestData(update, event)
            is TransactionAuthorizationCompletedEvent -> updateAuthCompletedData(update, event)
            is TransactionUserReceiptRequestedEvent -> updateUserReceiptData(update, event)
            is TransactionClosedEvent -> updateClosedData(update, event)
            is TransactionClosureErrorEvent -> updateClosureErrorData(update, event)
            is TransactionClosureRetriedEvent -> updateClosureRetriedData(update, event)
            is TransactionExpiredEvent,
            is TransactionRefundRequestedEvent,
            is TransactionUserCanceledEvent,
            is TransactionClosureRequestedEvent,
            is TransactionRefundErrorEvent,
            is TransactionUserReceiptAddedEvent,
            is TransactionUserReceiptAddErrorEvent,
            is TransactionClosureFailedEvent,
            is TransactionRefundedEvent,
            is TransactionRefundRetriedEvent,
            is TransactionUserReceiptAddRetriedEvent -> return null

            else -> {
                logger.warn(
                    "Unhandled event with code: [{}]. Event class: [{}]",
                    eventCode,
                    event.javaClass,
                )
            }
        }

        return update
    }

    /**
     * Updates fields for TRANSACTION_ACTIVATED_EVENT. This creates the initial transaction view
     * document with all basic transaction information.
     */
    private fun updateActivationData(update: Update, event: TransactionActivatedEvent): Update {
        val data = event.data
        update["email"] = data.email.opaqueData
        update["paymentNotices"] = data.paymentNotices
        update["clientId"] = data.clientId
        update["creationDate"] = event.creationDate
        update["_class"] = Transaction::class.java.canonicalName
        return update
    }

    /**
     * Updates fields for TRANSACTION_AUTHORIZATION_REQUESTED_EVENT. Adds payment gateway
     * information and authorization details.
     */
    private fun updateAuthRequestData(
        update: Update,
        event: TransactionAuthorizationRequestedEvent,
    ): Update {
        val authorizationRequestedData = event.data
        update["paymentGateway"] = authorizationRequestedData.paymentGateway
        update["paymentTypeCode"] = authorizationRequestedData.paymentTypeCode
        update["pspId"] = authorizationRequestedData.pspId
        update["feeTotal"] = authorizationRequestedData.fee
        return update
    }

    /**
     * Updates fields for TRANSACTION_AUTHORIZATION_COMPLETED_EVENT. Adds authorization results and
     * gateway response information.
     */
    private fun updateAuthCompletedData(
        update: Update,
        event: TransactionAuthorizationCompletedEvent,
    ): Update {
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

        return update
    }

    /**
     * Updates fields for TRANSACTION_CLOSURE_REQUESTED_EVENT. Adds closure information and outcome
     * details.
     */
    private fun updateClosureRequestData(
        update: Update,
        event: TransactionClosureRequestedEvent,
    ): Update {
        // no view field to be updated
        return update
    }

    /**
     * Updates fields for TRANSACTION_USER_RECEIPT_REQUESTED_EVENT. Adds user receipt information.
     */
    private fun updateUserReceiptData(
        update: Update,
        event: TransactionUserReceiptRequestedEvent,
    ): Update {
        update["sendPaymentResultOutcome"] = event.data.responseOutcome
        return update
    }

    /** Updates fields for TRANSACTION_EXPIRED_EVENT. Adds expiration information. */
    private fun updateExpiredData(update: Update, event: TransactionExpiredEvent): Update {
        // no view field to be updated
        return update
    }

    /** Updates fields for TRANSACTION_REFUND_REQUESTED_EVENT. Adds refund request information. */
    private fun updateRefundRequestData(
        update: Update,
        event: TransactionRefundRequestedEvent,
    ): Update {
        // no view field to be updated
        return update
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
        return update
    }

    /** Updates fields for TRANSACTION_USER_CANCELED_EVENT. Adds cancellation timestamp. */
    private fun updateUserCanceledData(
        update: Update,
        event: TransactionUserCanceledEvent,
    ): Update {
        // no view field to be updated
        return update
    }

    /** Updates fields for TRANSACTION_REFUND_ERROR_EVENT. Adds refund error information. */
    private fun updateRefundErrorData(update: Update, event: TransactionRefundErrorEvent): Update {
        // no view field to be updated
        return update
    }

    /** Updates fields for TRANSACTION_CLOSURE_ERROR_EVENT. Adds closure error timestamp. */
    private fun updateClosureErrorData(
        update: Update,
        event: TransactionClosureErrorEvent,
    ): Update {
        update["closureErrorData"] = event.data
        return update
    }

    /** Updates fields for TRANSACTION_USER_RECEIPT_ADDED_EVENT. Sets notification outcome. */
    private fun updateUserReceiptAddedData(
        update: Update,
        event: TransactionUserReceiptAddedEvent,
    ): Update {
        // no view field to be updated
        return update
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT. Adds receipt error information.
     */
    private fun updateUserReceiptErrorData(
        update: Update,
        event: TransactionUserReceiptAddErrorEvent,
    ): Update {
        // no view field to be updated
        return update
    }

    /** Updates fields for TRANSACTION_CLOSURE_RETRIED_EVENT. Adds closure retry information. */
    private fun updateClosureRetriedData(
        update: Update,
        event: TransactionClosureRetriedEvent,
    ): Update {
        update["sendPaymentResultOutcome"] = TransactionUserReceiptData.Outcome.NOT_RECEIVED
        if (event.data.closureErrorData != null) {
            update["closureErrorData"] = event.data.closureErrorData
        }
        return update
    }

    /** Updates fields for TRANSACTION_CLOSURE_FAILED_EVENT. Adds closure failure information. */
    private fun updateClosureFailedData(
        update: Update,
        event: TransactionClosureFailedEvent,
    ): Update {
        // no view field to be updated
        return update
    }

    /** Updates fields for TRANSACTION_REFUNDED_EVENT. Adds refund completion information. */
    private fun updateRefundedData(update: Update, event: TransactionRefundedEvent): Update {
        // no view field to be updated
        return update
    }

    /** Updates fields for TRANSACTION_REFUND_RETRIED_EVENT. Adds refund retry information. */
    private fun updateRefundRetriedData(
        update: Update,
        event: TransactionRefundRetriedEvent,
    ): Update {
        // no view field to be updated
        return update
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_RETRY_EVENT. Adds receipt retry information.
     */
    private fun updateUserReceiptRetryData(
        update: Update,
        event: TransactionUserReceiptAddRetriedEvent,
    ): Update {
        // no view field to be updated
        return update
    }
}
