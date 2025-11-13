package it.pagopa.ecommerce.cdc.services

import com.mongodb.client.result.UpdateResult
import it.pagopa.ecommerce.cdc.exceptions.CdcEventTypeException
import it.pagopa.ecommerce.cdc.exceptions.CdcQueryMatchException
import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.time.ZonedDateTime

/**
 * Service for performing upsert operations on transaction views with efficient atomic upsert
 * operations using native MongoDB queries.
 *
 * This service implements the core CDC logic for maintaining materialized transaction views by
 * processing transaction events and updating view documents with conditional logic based on event
 * timestamps. The service ensures data consistency through:
 * - Conditional status updates that respect event chronological order (prevents out-of-order
 *   overwrites)
 * - Atomic upsert operations that handle both new document creation and existing document updates
 * - Event-specific field mapping that populates view documents with appropriate transaction data
 * - Error handling and logging for debugging and monitoring CDC operations
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
     * 4. Throwing CdcQueryMatchException if no update conditions are met - this error triggers retry
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
            .flatMap { (dataUpdate, statusUpdate) ->
                tryToUpdateExistingView(event, statusUpdate, dataUpdate)
                    .flatMap { updateResult ->
                        if (updateResult.modifiedCount == 0L) {
                            logger.warn(
                                "No document updated for transactionId: [{}] processing event with id: [{}], trying upsert",
                                event.transactionId,
                                event.id,
                            )
                            upsertTransaction(
                                event,
                                statusUpdate.set(
                                    "_class",
                                    Transaction::class.java.canonicalName,
                                ),
                            )
                                .onErrorResume { Mono.empty() }
                                .filter { result -> result.upsertedId != null }
                        } else {
                            Mono.just(updateResult)
                        }
                    }
                    .switchIfEmpty {
                        /* @formatter:off
                           here update query have failed because of no document matched update criteria (aka where conditions on update)
                           this can be caused by racing condition processing view update from concurrent event processing.
                           Those errors are transitory and can be retried in order to allow for event write operation to be processed successfully also
                           for skipped event.
                           |A|      |B|     |DB|    case A, concurrency between A and B update, write happens in right order
                            |------->|------->| t0 -> both A and B events updates are processed simultaneously
                            |   OK   |   X    | t1 -> A update is processed correctly, B no
                            |   OK   |------->| t2 -> B operation is retried and status update saved successfully into view
                            |   OK   |   OK   |
                            |        |        |      case B, concurrency between A and B update, write happens in inverted order
                            |------->|------->| t0 -> both A and B events updates are processed simultaneously
                            |   X    |   OK   | t1 -> B update is processed correctly, A no
                            |        |------->| t2 -> A operation is retried. If event A have dataUpdate (that is, update for fields that are not overridden by other events)
                            |   X    |   OK   |       this update will be done, otherwise update will fail
                            |   X    |   OK   |       (meaning that all fields that fields update query A want to write are overwritten by subsequent event, such as status)
                            |   X    |   OK   |       this can lead to data loss
                            | (ko on |        |
                            | retry) |   OK   |
                           @formatter:on
                        */
                        Mono.error {
                            CdcQueryMatchException(
                                message = "Query didn't match any condition to update the view"
                            )
                        }
                    }
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
     * Updates transaction status only if the event is newer than the last processed event. This
     * prevents out-of-order events from overwriting current transaction state.
     *
     * @param event The MongoDB change stream event document
     * @param statusUpdate the update status and data to save on the db, if the lastProcessedEventAt
     *   in the view is before the creation date
     * @return Mono<UpdateResult> the update operation status
     */
    private fun updateStatusIfEventIsNewer(
        event: TransactionEvent<*>,
        statusUpdate: Update,
    ): Mono<UpdateResult> {
        return mongoTemplate.updateFirst(
            buildQuery(event, true),
            statusUpdate,
            BaseTransactionView::class.java,
            transactionViewName,
        )
    }

    /**
     * This method perform an upsert operation where update query is performed with
     * lastProcessedEventAt where condition (to ensure event write ordering) saving a new document
     * if no document matches the where condition (so when no document exists for given
     * transactionId)
     *
     * @param event The MongoDB change stream event document
     * @param statusUpdate the update status and data to save on the db, if the lastProcessedEventAt
     *   in the view is before the creation date
     * @return Mono<UpdateResult> the update operation status
     */
    private fun upsertTransaction(event: TransactionEvent<*>, statusUpdate: Update) =
        mongoTemplate.upsert(
            buildQuery(event, true),
            statusUpdate,
            BaseTransactionView::class.java,
            transactionViewName,
        )

    /**
     * Updates view/enrichment data without timestamp constraints. View data can always be updated
     * in any order since updated fields are not overridden by other events, so order does not
     * count.
     *
     * @param event The MongoDB change stream event document
     * @param dataUpdate the update data to save on the db, regardless the lastProcessedEventAt in
     *   the view
     * @return Mono<UpdateResult> the update operation status
     */
    private fun updateDataWithoutConstraints(
        event: TransactionEvent<*>,
        dataUpdate: Update,
    ): Mono<UpdateResult> {
        return mongoTemplate.updateFirst(
            buildQuery(event, false),
            dataUpdate,
            BaseTransactionView::class.java,
            transactionViewName,
        )
    }

    /**
     * Method to perform update query against view. Based on input parameter different update
     * strategies are attempt:
     * * both status update and data update parameters values -> first attempt to perform status
     *   update (that is, update that contains also fields that are overridden by other events, so
     *   update must be done in chronological order) in case of failure an attempt to update data
     *   update is performed (that is, update that contains field that are not overridden by other
     *   events, so resulting query will not have where for ordered write operations)
     * * status update only values -> as above but only status update query is performed
     *
     * @param event The MongoDB change stream event document
     * @param statusUpdate the update status and data to save on the db, if the lastProcessedEventAt
     *   in the view is before the creation date
     * @param dataUpdate the update data to save on the db, regardless the lastProcessedEventAt in
     *   the view
     * @return Mono<UpdateResult> the update operation status
     */
    private fun tryToUpdateExistingView(
        event: TransactionEvent<*>,
        statusUpdate: Update,
        dataUpdate: Update?,
    ): Mono<UpdateResult> {
        return when (dataUpdate) {
            null -> updateStatusIfEventIsNewer(event, statusUpdate)
            // if only the status should be updated and it is going to fail because
            // the lastProcessedEventAt is after the event creation date the filter
            // should exclude all the existing document from the subsequent upsert.
            // So the filter should return false if the document doesn't exist.
            else ->
                updateStatusIfEventIsNewer(event, statusUpdate).flatMap {
                    if (it.modifiedCount == 0L) {
                        updateDataWithoutConstraints(event, dataUpdate)
                    } else {
                        Mono.just(it)
                    }
                }
        }
    }

    /**
     * Builds a pair of MongoDB Update objects based on the event type and content. Different events
     * update different portions of the transaction view document. The first member can be null and
     * contains updates that don't need to be matched against the lastProcessedEventAt field in the
     * view. The second member is not nullable and contains all updates about info to save and also
     * status and lastProcessedEventAt field to update in the view.
     *
     * This method implements the conditional update strategy where:
     * - Event data is updated regardless of timestamp if the event being processed contains more
     *   info than the current one
     * - Status updates are conditionally based on event chronological order
     *
     * @param event The transaction event containing update data and metadata
     * @return Mono of a pair of update objects with field updates based on event type
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

    /**
     * Updates fields for TRANSACTION_ACTIVATED_EVENT. This creates the initial transaction view
     * document with all basic transaction information.
     */
    private fun updateActivationData(event: TransactionActivatedEvent): Pair<Update?, Update> {
        val update = Update()
        val statusUpdate = Update()
        val data = event.data
        update["email"] = data.email
        update["paymentNotices"] = data.paymentNotices
        update["clientId"] = data.clientId
        update["creationDate"] = event.creationDate
        if (data.userId != null) {
            update["userId"] = data.userId
            statusUpdate["userId"] = data.userId
        }
        statusUpdate["email"] = data.email
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
        val update = Update()
        val statusUpdate = Update()
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
        val update = Update()
        val statusUpdate = Update()
        val data = event.data

        val gatewayAuthData = data.transactionGatewayAuthorizationData

        if (data.authorizationCode != null) {
            update["authorizationCode"] = data.authorizationCode
            statusUpdate["authorizationCode"] = data.authorizationCode
        } else {
            update.unset("authorizationCode")
            statusUpdate.unset("authorizationCode")
        }

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
        val update = Update()
        val statusUpdate = Update()
        update["sendPaymentResultOutcome"] = event.data.responseOutcome
        statusUpdate["sendPaymentResultOutcome"] = event.data.responseOutcome

        statusUpdate["status"] = TransactionStatusDto.NOTIFICATION_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(update, statusUpdate)
    }

    /** Updates fields for TRANSACTION_EXPIRED_EVENT. Adds expiration information. */
    private fun updateExpiredData(event: TransactionExpiredEvent): Pair<Update?, Update> {
        val statusUpdate = Update()
        statusUpdate["status"] = TransactionStatusDto.EXPIRED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUND_REQUESTED_EVENT. Adds refund request information. */
    private fun updateRefundRequestData(
        event: TransactionRefundRequestedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = Update()
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
        val statusUpdate = Update()
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
        val statusUpdate = Update()
        statusUpdate["status"] = TransactionStatusDto.CANCELLATION_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUND_ERROR_EVENT. Adds refund error information. */
    private fun updateRefundErrorData(event: TransactionRefundErrorEvent): Pair<Update?, Update> {
        val statusUpdate = Update()
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
        val statusUpdate = Update()
        statusUpdate["status"] = TransactionStatusDto.CLOSURE_REQUESTED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_CLOSURE_ERROR_EVENT. Adds closure error timestamp. */
    private fun updateClosureErrorData(event: TransactionClosureErrorEvent): Pair<Update?, Update> {
        val statusUpdate = Update()
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
        val statusUpdate = Update()
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
        val statusUpdate = Update()
        statusUpdate["status"] = TransactionStatusDto.NOTIFICATION_ERROR
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_CLOSURE_RETRIED_EVENT. Adds closure retry information. */
    private fun updateClosureRetriedData(
        event: TransactionClosureRetriedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = Update()
        statusUpdate["sendPaymentResultOutcome"] = TransactionUserReceiptData.Outcome.NOT_RECEIVED
        if (event.data.closureErrorData != null) {
            statusUpdate["closureErrorData"] = event.data.closureErrorData
        }
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()

        return Pair(null, statusUpdate)
        // Doesn't update the state, but it has to be processed conditionally on its timestamp
    }

    /** Updates fields for TRANSACTION_CLOSURE_FAILED_EVENT. Adds closure failure information. */
    private fun updateClosureFailedData(
        event: TransactionClosureFailedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = Update()
        statusUpdate["status"] = TransactionStatusDto.UNAUTHORIZED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUNDED_EVENT. Adds refund completion information. */
    private fun updateRefundedData(event: TransactionRefundedEvent): Pair<Update?, Update> {
        val statusUpdate = Update()
        statusUpdate["status"] = TransactionStatusDto.REFUNDED
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
    }

    /** Updates fields for TRANSACTION_REFUND_RETRIED_EVENT. Adds refund retry information. */
    private fun updateRefundRetriedData(
        event: TransactionRefundRetriedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = Update()
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
        // Doesn't update the state, but it has to be processed conditionally on its timestamp.
        // Maybe it could be skipped
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_RETRY_EVENT. Adds receipt retry information.
     */
    private fun updateUserReceiptRetryData(
        event: TransactionUserReceiptAddRetriedEvent
    ): Pair<Update?, Update> {
        val statusUpdate = Update()
        statusUpdate["lastProcessedEventAt"] =
            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        return Pair(null, statusUpdate)
        // Doesn't update the state but it has to be processed conditionally on its timestamp.
        // Maybe it could be skipped
    }
}
