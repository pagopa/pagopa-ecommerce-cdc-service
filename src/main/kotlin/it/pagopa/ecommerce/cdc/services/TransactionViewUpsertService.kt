package it.pagopa.ecommerce.cdc.services

import com.mongodb.client.result.UpdateResult
import it.pagopa.ecommerce.cdc.exceptions.CdcEventTypeException
import it.pagopa.ecommerce.cdc.exceptions.CdcQueryMatchException
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
     * Processes a transaction event and updates the corresponding view with conditional logic. The
     * operation uses timestamp-based event ordering to ensure only newer events modify the view. If
     * no existing document is modified, performs an upsert for new transactions.
     *
     * The process involves:
     * 1. Building update operations based on the specific event type
     * 2. Attempting conditional update with timestamp validation (lastProcessedEventAt)
     * 3. Falling back to upsert when no modification occurs for new transactions
     * 4. Throwing CdcQueryMatchException if no update conditions are met
     *
     * @param event The transaction event containing update data and metadata
     * @return Mono<UpdateResult> The result of the update/upsert operation
     */
    fun upsertEventData(event: TransactionEvent<*>): Mono<UpdateResult> {
        val eventCode = event.eventCode
        val transactionId = event.transactionId

        logger.debug(
            "Upserting transaction view data for _id: [{}], eventCode: [{}]",
            transactionId,
            eventCode,
        )

        return buildUpdateFromEvent(event)
            .flatMap { (update, updateStatus) ->
                tryToUpdateExistingView(event, updateStatus, update).flatMap { updateResult ->
                    if (updateResult.modifiedCount == 0L) {
                        isTransactionPresent(event).flatMap {
                            if (!it) {
                                upsertTransaction(event, updateStatus).filter {
                                    it.upsertedId != null
                                }
                            } else {
                                Mono.empty()
                            }
                        }
                    } else {
                        Mono.just(updateResult)
                    }
                }
            }
            .switchIfEmpty(
                Mono.error {
                    CdcQueryMatchException("Query didn't match any condition to update the view")
                }
            )
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
    }

    /**
     * Build query helper methods. Search by transactionId and, if needed, check also the
     * lastProcessedEventAt
     *
     * @param event The MongoDB change stream event document
     * @param addLastProcessedEventAtCondition flag to enable the lastProcessedEventAt criteria
     * @return Query the query built with the requested criteria
     */
    private fun buildQuery(
        event: TransactionEvent<*>,
        addLastProcessedEventAtCondition: Boolean = false,
    ): Query {
        val criteria = Criteria.where("transactionId").`is`(event.transactionId)

        if (addLastProcessedEventAtCondition) {
            criteria.orOperator(
                Criteria.where("lastProcessedEventAt").exists(false),
                Criteria.where("lastProcessedEventAt")
                    .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
            )
        }

        return Query.query(criteria)
    }

    /**
     * Checks if a given transaction is already present in the db
     *
     * @param event The MongoDB change stream event document
     * @return Boolean true iff the transaction exists
     */
    private fun isTransactionPresent(event: TransactionEvent<*>) =
        mongoTemplate.exists(
            buildQuery(event),
            BaseTransactionView::class.java,
            transactionViewName,
        )

    /**
     * Updates transaction status only if the event is newer than the last processed event. This
     * prevents out-of-order events from overwriting current transaction state.
     *
     * @param event The MongoDB change stream event document
     * @param updateStatus the update data to save on the db
     * @return Mono<UpdateResult> the update operation status
     */
    private fun updateStatusIfEventIsNewer(
        event: TransactionEvent<*>,
        updateStatus: Update,
    ): Mono<UpdateResult> {
        val queryWithTimestamp =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        return mongoTemplate.updateFirst(
            queryWithTimestamp,
            updateStatus,
            BaseTransactionView::class.java,
            transactionViewName,
        )
    }

    /**
     * @param event The MongoDB change stream event document
     * @param updateStatus the update data to save on the db
     * @return Mono<UpdateResult> the update operation status
     */
    private fun upsertTransaction(event: TransactionEvent<*>, updateStatus: Update) =
        mongoTemplate.upsert(
            buildQuery(event, true),
            updateStatus,
            BaseTransactionView::class.java,
            transactionViewName,
        )

    /**
     * Updates view/enrichment data without timestamp constraints. View data can always be updated
     * as it doesn't affect transaction state.
     *
     * @param event The MongoDB change stream event document
     * @param status the update data to save on the db
     * @return Mono<UpdateResult> the update operation status
     */
    private fun updateDataWithoutConstraints(
        event: TransactionEvent<*>,
        status: Update,
    ): Mono<UpdateResult> {
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))
        return mongoTemplate.updateFirst(
            queryByTransactionId,
            status,
            BaseTransactionView::class.java,
            transactionViewName,
        )
    }

    /**
     * Method to try to update an existing view
     *
     * @param event The MongoDB change stream event document
     * @param updateStatus the update data to save on the db
     * @return Mono<UpdateResult> the update operation status
     */
    private fun tryToUpdateExistingView(
        event: TransactionEvent<*>,
        updateStatus: Update,
        update: Update?,
    ): Mono<UpdateResult> {
        return when (update) {
            null -> updateStatusIfEventIsNewer(event, updateStatus)
            // if only the status should be updated and it is going to fail because
            // the lastProcessedEventAt is after the event creation date the filter
            // should exclude all the existing document from the subsequent upsert.
            // So the filter should return false if the document doesn't exist.
            else ->
                updateStatusIfEventIsNewer(event, updateStatus).flatMap {
                    if (it.modifiedCount == 0L) {
                        updateDataWithoutConstraints(event, update)
                    } else {
                        Mono.just(it)
                    }
                }
        }
    }

    /**
     * Builds a pair of MongoDB Update object based on the event type and content. Different events
     * update different portions of the transaction view document. The first member can be null and
     * contains update that doesn't need tobe matched against lastProcessedEventAt field in the
     * view. The second one is not nullable and contains all updates about info to save and also
     * status and lastProcessedEventAt field to update in the view.
     *
     * @param event The MongoDB change stream event document
     * @return Mono of a pair of update object with field updates based on event type.
     */
    private fun buildUpdateFromEvent(event: TransactionEvent<*>): Mono<Pair<Update?, Update>> {
        val eventCode = event.eventCode
        // apply updates based on specific event types
        val result: Pair<Update?, Update> =
            when (event) {
                is TransactionActivatedEvent -> updateActivationData(event)
                is TransactionAuthorizationRequestedEvent -> updateAuthRequestData(event)
                is TransactionAuthorizationCompletedEvent -> updateAuthCompletedData(event)
                is TransactionUserReceiptRequestedEvent -> updateUserReceiptData(event)
                is TransactionClosedEvent -> updateClosedData(event)
                is TransactionClosureErrorEvent -> updateClosureErrorData(event)
                is TransactionClosureRetriedEvent -> updateClosureRetriedData(event)
                is TransactionExpiredEvent -> updateExpiredData(event)
                is TransactionRefundRequestedEvent -> updateRefundRequestData(event)
                is TransactionUserCanceledEvent -> updateUserCanceledData(event)
                is TransactionClosureRequestedEvent -> updateClosureRequestData(event)
                is TransactionRefundErrorEvent -> updateRefundErrorData(event)
                is TransactionUserReceiptAddedEvent -> updateUserReceiptAddedData(event)
                is TransactionUserReceiptAddErrorEvent -> updateUserReceiptErrorData(event)
                is TransactionClosureFailedEvent -> updateClosureFailedData(event)
                is TransactionRefundedEvent -> updateRefundedData(event)
                is TransactionRefundRetriedEvent -> updateRefundRetriedData(event)
                is TransactionUserReceiptAddRetriedEvent -> updateUserReceiptRetryData(event)

                else -> {
                    logger.warn(
                        "Unhandled event with code: [{}]. Event class: [{}]",
                        eventCode,
                        event.javaClass,
                    )
                    return Mono.error {
                        CdcEventTypeException(
                            "Cannot handle event with eventCode: $eventCode Event class: ${event.javaClass}"
                        )
                    }
                }
            }

        return mono { result }
    }

    private fun buildCommonUpdate(): Update {
        val commonUpdate = Update()
        commonUpdate["_class"] = Transaction::class.java.canonicalName
        return commonUpdate
    }

    /**
     * Updates fields for TRANSACTION_ACTIVATED_EVENT. This creates the initial transaction view
     * document with all basic transaction information.
     */
    private fun updateActivationData(event: TransactionActivatedEvent): Pair<Update?, Update> {
        val update = buildCommonUpdate()
        val statusUpdate = buildCommonUpdate()
        val data = event.data
        update["email"] = data.email.opaqueData
        update["paymentNotices"] = data.paymentNotices
        update["clientId"] = data.clientId
        update["creationDate"] = event.creationDate

        statusUpdate["email"] = data.email.opaqueData
        statusUpdate["paymentNotices"] = data.paymentNotices
        statusUpdate["clientId"] = data.clientId
        statusUpdate["creationDate"] = event.creationDate
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
        event: TransactionAuthorizationRequestedEvent
    ): Pair<Update?, Update> {
        val update = buildCommonUpdate()
        val statusUpdate = buildCommonUpdate()
        val authorizationRequestedData = event.data
        update["paymentGateway"] = authorizationRequestedData.paymentGateway
        update["paymentTypeCode"] = authorizationRequestedData.paymentTypeCode
        update["pspId"] = authorizationRequestedData.pspId
        update["feeTotal"] = authorizationRequestedData.fee

        statusUpdate["paymentGateway"] = authorizationRequestedData.paymentGateway
        statusUpdate["paymentTypeCode"] = authorizationRequestedData.paymentTypeCode
        statusUpdate["pspId"] = authorizationRequestedData.pspId
        statusUpdate["feeTotal"] = authorizationRequestedData.fee
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
        event: TransactionAuthorizationCompletedEvent
    ): Pair<Update?, Update> {
        val update = buildCommonUpdate()
        val statusUpdate = buildCommonUpdate()
        val data = event.data

        update["authorizationCode"] = data.authorizationCode

        statusUpdate["authorizationCode"] = data.authorizationCode

        val gatewayAuthData = data.transactionGatewayAuthorizationData

        if (data.rrn != null) {
            update["rrn"] = data.rrn
            statusUpdate["rrn"] = data.rrn
        } else {
            update.unset("rrn")
            statusUpdate.unset("rrn")
        }

        when (gatewayAuthData) {
            is NpgTransactionGatewayAuthorizationData -> {
                update["gatewayAuthorizationStatus"] = gatewayAuthData.operationResult.toString()

                statusUpdate["gatewayAuthorizationStatus"] = gatewayAuthData.operationResult
                if (gatewayAuthData.errorCode != null) {
                    update["authorizationErrorCode"] = gatewayAuthData.errorCode
                    statusUpdate["authorizationErrorCode"] = gatewayAuthData.errorCode
                } else {
                    update.unset("authorizationErrorCode")
                    statusUpdate.unset("authorizationErrorCode")
                }
            }

            is RedirectTransactionGatewayAuthorizationData -> {
                update["gatewayAuthorizationStatus"] = gatewayAuthData.outcome.toString()

                statusUpdate["gatewayAuthorizationStatus"] = gatewayAuthData.outcome

                if (gatewayAuthData.errorCode != null) {
                    update["authorizationErrorCode"] = gatewayAuthData.errorCode
                    statusUpdate["authorizationErrorCode"] = gatewayAuthData.errorCode
                } else {
                    update.unset("authorizationErrorCode")
                    statusUpdate.unset("authorizationErrorCode")
                }
            }

            else ->
                logger.warn(
                    "Unhandled transaction gateway authorization data: [{}]",
                    gatewayAuthData::class.java,
                )
        }

        statusUpdate["status"] = TransactionStatusDto.AUTHORIZATION_COMPLETED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(update, statusUpdate)
    }

    /**
     * Updates fields for TRANSACTION_USER_RECEIPT_REQUESTED_EVENT. Adds user receipt information.
     */
    private fun updateUserReceiptData(
        event: TransactionUserReceiptRequestedEvent
    ): Pair<Update?, Update> {
        val update = buildCommonUpdate()
        val statusUpdate = buildCommonUpdate()
        update["sendPaymentResultOutcome"] = event.data.responseOutcome
        statusUpdate["sendPaymentResultOutcome"] = event.data.responseOutcome

        statusUpdate["status"] = TransactionStatusDto.NOTIFICATION_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(update, statusUpdate)
    }

    /** Updates fields for TRANSACTION_EXPIRED_EVENT. Adds expiration information. */
    private fun updateExpiredData(event: TransactionExpiredEvent): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.EXPIRED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUND_REQUESTED_EVENT. Adds refund request information. */
    private fun updateRefundRequestData(
        event: TransactionRefundRequestedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.REFUND_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /**
     * Updates fields for TRANSACTION_CLOSED_EVENT. Sets sendPaymentResultOutcome and closure
     * timestamp.
     */
    private fun updateClosedData(event: TransactionClosedEvent): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["sendPaymentResultOutcome"] = TransactionUserReceiptData.Outcome.NOT_RECEIVED
        statusUpdate.unset("closureErrorData")
        statusUpdate["status"] =
            when (event.data.wasCanceledByUser) {
                true -> TransactionStatusDto.CANCELED
                else ->
                    when (event.data.responseOutcome) {
                        TransactionClosureData.Outcome.OK -> TransactionStatusDto.CLOSED
                        TransactionClosureData.Outcome.KO -> TransactionStatusDto.UNAUTHORIZED
                    }
            }

        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_USER_CANCELED_EVENT. Adds cancellation timestamp. */
    private fun updateUserCanceledData(event: TransactionUserCanceledEvent): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.CANCELLATION_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUND_ERROR_EVENT. Adds refund error information. */
    private fun updateRefundErrorData(event: TransactionRefundErrorEvent): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.REFUND_ERROR
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /**
     * Updates fields for TRANSACTION_CLOSURE_REQUESTED_EVENT. Adds closure information and outcome
     * details.
     */
    private fun updateClosureRequestData(
        event: TransactionClosureRequestedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.CLOSURE_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_CLOSURE_ERROR_EVENT. Adds closure error timestamp. */
    private fun updateClosureErrorData(event: TransactionClosureErrorEvent): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["closureErrorData"] = event.data
        statusUpdate["status"] = TransactionStatusDto.CLOSURE_ERROR
        statusUpdate["sendPaymentResultOutcome"] = TransactionUserReceiptData.Outcome.NOT_RECEIVED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_USER_RECEIPT_ADDED_EVENT. Sets notification outcome. */
    private fun updateUserReceiptAddedData(
        event: TransactionUserReceiptAddedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        when (event.data.responseOutcome) {
            TransactionUserReceiptData.Outcome.OK ->
                statusUpdate["status"] = TransactionStatusDto.NOTIFIED_OK
            TransactionUserReceiptData.Outcome.KO ->
                statusUpdate["status"] = TransactionStatusDto.NOTIFIED_KO
            else -> {} // throw exception
        }
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(null, statusUpdate)
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT. Adds receipt error information.
     */
    private fun updateUserReceiptErrorData(
        event: TransactionUserReceiptAddErrorEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.NOTIFICATION_ERROR
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_CLOSURE_RETRIED_EVENT. Adds closure retry information. */
    private fun updateClosureRetriedData(
        event: TransactionClosureRetriedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["sendPaymentResultOutcome"] = TransactionUserReceiptData.Outcome.NOT_RECEIVED
        if (event.data.closureErrorData != null) {
            statusUpdate["closureErrorData"] = event.data.closureErrorData
        }
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(null, statusUpdate)
        // Doesn't update the state, but it has to be processed coditionally on its timestamp
    }

    /** Updates fields for TRANSACTION_CLOSURE_FAILED_EVENT. Adds closure failure information. */
    private fun updateClosureFailedData(
        event: TransactionClosureFailedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.UNAUTHORIZED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUNDED_EVENT. Adds refund completion information. */
    private fun updateRefundedData(event: TransactionRefundedEvent): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["status"] = TransactionStatusDto.REFUNDED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUND_RETRIED_EVENT. Adds refund retry information. */
    private fun updateRefundRetriedData(
        event: TransactionRefundRetriedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
        // Doesn't update the state, but it has to be processed coditionally on its timestamp.
        // Maybe it could be skipped
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_RETRY_EVENT. Adds receipt retry information.
     */
    private fun updateUserReceiptRetryData(
        event: TransactionUserReceiptAddRetriedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = buildCommonUpdate()
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
        // Doesn't update the state but it has to be processed coditionally on its timestamp.
        // Maybe it could be skipped
    }
}
