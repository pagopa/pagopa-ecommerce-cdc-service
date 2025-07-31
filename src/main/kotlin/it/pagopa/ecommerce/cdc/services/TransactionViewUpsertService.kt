package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import it.pagopa.ecommerce.commons.documents.PaymentNotice
import it.pagopa.ecommerce.commons.documents.PaymentTransferInformation
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.domain.Confidential
import it.pagopa.ecommerce.commons.domain.v2.Email
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import java.time.ZonedDateTime
import org.bson.Document
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
    fun upsertEventData(transactionId: String, event: Document): Mono<Unit> {
        return Mono.defer {
                val eventCode = event.getString("eventCode") ?: "UNKNOWN"

                logger.debug(
                    "Upserting transaction view data for _id: [{}], eventCode: [{}]",
                    transactionId,
                    eventCode,
                )

                // TODO verify collection targeting for prod deployment
                val query = Query.query(Criteria.where("transactionId").`is`(transactionId))
                val update = buildUpdateFromEvent(event)

                (mongoTemplate.upsert(
                        query,
                        update,
                        BaseTransactionView::class.java,
                        transactionViewName,
                    ))
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
    private fun buildUpdateFromEvent(event: Document): Update {
        val update = Update()
        val eventCode = event.getString("eventCode") ?: "UNKNOWN"
        val creationDate = event.getString("creationDate")

        // update lastProcessedEventAt field
        if (creationDate != null) {
            update.set(
                "lastProcessedEventAt",
                ZonedDateTime.parse(creationDate).toInstant().toEpochMilli(),
            )
        }

        // TODO CHK-4353: Status updates with conditional timestamp logic

        // set transaction status based on event type
        val transactionStatus =
            when (eventCode) {
                "TRANSACTION_ACTIVATED_EVENT" -> "ACTIVATED"
                "TRANSACTION_AUTHORIZATION_REQUESTED_EVENT" -> "AUTHORIZATION_REQUESTED"
                "TRANSACTION_AUTHORIZATION_COMPLETED_EVENT" -> "AUTHORIZATION_COMPLETED"
                "TRANSACTION_CLOSURE_REQUESTED_EVENT" -> "CLOSURE_REQUESTED"
                "TRANSACTION_CLOSED_EVENT" -> "CLOSED"
                "TRANSACTION_EXPIRED_EVENT" -> "EXPIRED"
                "TRANSACTION_USER_CANCELED_EVENT" -> "CANCELLATION_REQUESTED"
                "TRANSACTION_REFUND_REQUESTED_EVENT" -> "REFUND_REQUESTED"
                "TRANSACTION_REFUND_ERROR_EVENT" -> "REFUND_ERROR"
                "TRANSACTION_CLOSURE_ERROR_EVENT" -> "CLOSURE_ERROR"
                "TRANSACTION_USER_RECEIPT_REQUESTED_EVENT" -> "NOTIFICATION_REQUESTED"
                "TRANSACTION_USER_RECEIPT_ADDED_EVENT" -> "NOTIFIED_OK"
                "TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT" -> "NOTIFICATION_ERROR"
                // TODO check if we need more
                else -> null // keep status for unmapped events
            }
        transactionStatus?.let { update.set("status", it) }

        // apply updates based on specific event types
        when (eventCode) {
            "TRANSACTION_ACTIVATED_EVENT" -> updateActivationData(update, event)
            "TRANSACTION_AUTHORIZATION_REQUESTED_EVENT" -> updateAuthRequestData(update, event)
            "TRANSACTION_AUTHORIZATION_COMPLETED_EVENT" -> updateAuthCompletedData(update, event)
            "TRANSACTION_CLOSURE_REQUESTED_EVENT" -> updateClosureRequestData(update, event)
            "TRANSACTION_USER_RECEIPT_REQUESTED_EVENT" -> updateUserReceiptData(update, event)
            "TRANSACTION_EXPIRED_EVENT" -> updateExpiredData(update, event)
            "TRANSACTION_REFUND_REQUESTED_EVENT" -> updateRefundRequestData(update, event)
            "TRANSACTION_USER_CANCELED_EVENT" -> updateUserCanceledData(update, event)
            "TRANSACTION_CLOSED_EVENT" -> updateClosedData(update, event)
            "TRANSACTION_REFUND_ERROR_EVENT" -> updateRefundErrorData(update, event)
            "TRANSACTION_CLOSURE_ERROR_EVENT" -> updateClosureErrorData(update, event)
            "TRANSACTION_USER_RECEIPT_ADDED_EVENT" -> updateUserReceiptAddedData(update, event)
            "TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT" -> updateUserReceiptErrorData(update, event)
            "TRANSACTION_CLOSURE_RETRIED_EVENT" -> updateClosureRetriedData(update, event)
            "TRANSACTION_CLOSURE_FAILED_EVENT" -> updateClosureFailedData(update, event)
            "TRANSACTION_REFUNDED_EVENT" -> updateRefundedData(update, event)
            "TRANSACTION_REFUND_RETRIED_EVENT" -> updateRefundRetriedData(update, event)
            "TRANSACTION_ADD_USER_RECEIPT_RETRY_EVENT" -> updateUserReceiptRetryData(update, event)
            else -> {
                logger.warn("Unknown event code: [{}] for event: [{}]", eventCode, event.toJson())
                // for unmapped events, just update timestamp and basic fields
                updateGenericEventData(update, event)
            }
        }

        return update
    }

    /**
     * Updates fields for TRANSACTION_ACTIVATED_EVENT. This creates the initial transaction view
     * document with all basic transaction information.
     */
    private fun updateActivationData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            // extract email from nested object structure
            val emailObj = eventData.get("email") as? Document
            emailObj?.let { update.set("email", it) }

            eventData.get("paymentNotices")?.let { update.set("paymentNotices", it) }

            eventData.getString("clientId")?.let { update.set("clientId", it) }
            eventData.getString("paymentToken")?.let { update.set("paymentToken", it) }

            event.getString("creationDate")?.let { update.set("creationDate", it) }
        }

        return update
    }

    /**
     * Updates fields for TRANSACTION_AUTHORIZATION_REQUESTED_EVENT. Adds payment gateway
     * information and authorization details.
     */
    private fun updateAuthRequestData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("paymentGateway")?.let { update.set("paymentGateway", it) }
            eventData.getString("paymentMethodId")?.let { update.set("paymentMethodId", it) }
            eventData.getString("pspId")?.let { update.set("pspId", it) }
            eventData.getString("paymentTypeCode")?.let { update.set("paymentTypeCode", it) }

            eventData.getInteger("fee")?.let { update.set("feeTotal", it) }

            event.getString("creationDate")?.let { update.set("authorizationRequestedAt", it) }

            eventData.get("paymentGatewayDetails")?.let { update.set("paymentGatewayDetails", it) }

            eventData.getString("paymentMethodName")?.let { update.set("paymentMethodName", it) }
            eventData.getString("pspBusinessName")?.let { update.set("pspBusinessName", it) }
            eventData.getString("authorizationRequestId")?.let {
                update.set("authorizationRequestId", it)
            }
        }

        return update
    }

    /**
     * Updates fields for TRANSACTION_AUTHORIZATION_COMPLETED_EVENT. Adds authorization results and
     * gateway response information.
     */
    private fun updateAuthCompletedData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("authorizationCode")?.let { update.set("authorizationCode", it) }
            eventData.getString("rrn")?.let { update.set("rrn", it) }

            val gatewayAuthData = eventData.get("transactionGatewayAuthorizationData") as? Document
            gatewayAuthData?.let { authData ->
                authData.getString("operationResult")?.let {
                    update.set("gatewayAuthorizationStatus", it)
                }
                authData.getString("errorCode")?.let { update.set("authorizationErrorCode", it) }
            }

            eventData.get("paymentGatewayResponse")?.let {
                update.set("paymentGatewayResponse", it)
            }
            eventData.getString("gatewayAuthorizationId")?.let {
                update.set("gatewayAuthorizationId", it)
            }

            event.getString("creationDate")?.let { update.set("authorizationCompletedAt", it) }
        }

        return update
    }

    /**
     * Updates fields for TRANSACTION_CLOSURE_REQUESTED_EVENT. Adds closure information and outcome
     * details.
     */
    private fun updateClosureRequestData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("closureReason")?.let { update.set("closureReason", it) }
            eventData.getString("closureOutcome")?.let { update.set("closureOutcome", it) }

            event.getString("creationDate")?.let { update.set("closureRequestedAt", it) }
        }

        return update
    }

    /**
     * Updates fields for TRANSACTION_USER_RECEIPT_REQUESTED_EVENT. Adds user receipt information.
     */
    private fun updateUserReceiptData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("receiptId")?.let { update.set("receiptId", it) }
            eventData.getString("receiptData")?.let { update.set("receiptData", it) }

            event.getString("creationDate")?.let { update.set("userReceiptRequestedAt", it) }
        }

        return update
    }

    /** Updates fields for TRANSACTION_EXPIRED_EVENT. Adds expiration information. */
    private fun updateExpiredData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("expiredReason")?.let { update.set("expiredReason", it) }

            event.getString("creationDate")?.let { update.set("expiredAt", it) }
        }

        return update
    }

    /** Updates fields for TRANSACTION_REFUND_REQUESTED_EVENT. Adds refund request information. */
    private fun updateRefundRequestData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("refundReason")?.let { update.set("refundReason", it) }
            eventData.getInteger("refundAmount")?.let { update.set("refundAmount", it) }

            event.getString("creationDate")?.let { update.set("refundRequestedAt", it) }
        }

        return update
    }

    /**
     * Updates fields for TRANSACTION_CLOSED_EVENT. Sets sendPaymentResultOutcome and closure
     * timestamp.
     */
    private fun updateClosedData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("responseOutcome")?.let {
                val outcome = if (it == "OK") "OK" else "KO"
                update.set("sendPaymentResultOutcome", outcome)
            }
        }

        event.getString("creationDate")?.let { update.set("closedAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_USER_CANCELED_EVENT. Adds cancellation timestamp. */
    private fun updateUserCanceledData(update: Update, event: Document): Update {
        event.getString("creationDate")?.let { update.set("userCanceledAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_REFUND_ERROR_EVENT. Adds refund error information. */
    private fun updateRefundErrorData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("statusBeforeRefunded")?.let {
                update.set("statusBeforeRefunded", it)
            }
        }

        event.getString("creationDate")?.let { update.set("refundErrorAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_CLOSURE_ERROR_EVENT. Adds closure error timestamp. */
    private fun updateClosureErrorData(update: Update, event: Document): Update {
        event.getString("creationDate")?.let { update.set("closureErrorAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_USER_RECEIPT_ADDED_EVENT. Sets notification outcome. */
    private fun updateUserReceiptAddedData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("responseOutcome")?.let {
                val outcome = if (it == "OK") "OK" else "KO"
                update.set("sendPaymentResultOutcome", outcome)
            }
        }

        event.getString("creationDate")?.let { update.set("userReceiptAddedAt", it) }

        return update
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT. Adds receipt error information.
     */
    private fun updateUserReceiptErrorData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("responseOutcome")?.let {
                update.set("sendPaymentResultOutcome", "KO")
            }
        }

        event.getString("creationDate")?.let { update.set("userReceiptErrorAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_CLOSURE_RETRIED_EVENT. Adds closure retry information. */
    private fun updateClosureRetriedData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getInteger("retryCount")?.let { update.set("closureRetryCount", it) }
            eventData.getString("retryReason")?.let { update.set("closureRetryReason", it) }
        }

        event.getString("creationDate")?.let { update.set("closureRetriedAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_CLOSURE_FAILED_EVENT. Adds closure failure information. */
    private fun updateClosureFailedData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("failureReason")?.let { update.set("closureFailureReason", it) }
            eventData.getString("errorCode")?.let { update.set("closureErrorCode", it) }
        }

        event.getString("creationDate")?.let { update.set("closureFailedAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_REFUNDED_EVENT. Adds refund completion information. */
    private fun updateRefundedData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getInteger("refundedAmount")?.let { update.set("refundedAmount", it) }
            eventData.getString("refundStatus")?.let { update.set("refundStatus", it) }
            eventData.getString("refundId")?.let { update.set("refundId", it) }
        }

        event.getString("creationDate")?.let { update.set("refundedAt", it) }

        return update
    }

    /** Updates fields for TRANSACTION_REFUND_RETRIED_EVENT. Adds refund retry information. */
    private fun updateRefundRetriedData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getInteger("retryCount")?.let { update.set("refundRetryCount", it) }
            eventData.getString("retryReason")?.let { update.set("refundRetryReason", it) }
        }

        event.getString("creationDate")?.let { update.set("refundRetriedAt", it) }

        return update
    }

    /**
     * Updates fields for TRANSACTION_ADD_USER_RECEIPT_RETRY_EVENT. Adds receipt retry information.
     */
    private fun updateUserReceiptRetryData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getInteger("retryCount")?.let { update.set("receiptRetryCount", it) }
            eventData.getString("retryReason")?.let { update.set("receiptRetryReason", it) }
        }

        event.getString("creationDate")?.let { update.set("receiptRetriedAt", it) }

        return update
    }

    /** Updates generic fields for unknown event types. Used as fallback for unrecognized events. */
    private fun updateGenericEventData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        // save the entire event data for unmapped events
        data?.let { update.set("lastEventData", it) }

        return update
    }

    /**
     * Creates a Transaction object from TRANSACTION_ACTIVATED_EVENT data. This ensures the mappping
     * is done correctly to commons Transaction type as per the original implementation by the
     * transactions-service.
     *
     * @param transactionId The transaction identifier
     * @param event The MongoDB change stream event document
     * @return Mono<Transaction> A Transaction object
     */
    private fun createTransactionFromActivationEvent(
        transactionId: String,
        event: Document,
    ): Mono<Transaction> {
        return Mono.fromCallable {
            val data =
                event.get("data") as? Document
                    ?: throw IllegalArgumentException("Missing data in activation event")

            val emailObj = data.get("email") as? Document
            val emailData =
                emailObj?.getString("data")
                    ?: throw IllegalArgumentException("Missing email data in activation event")
            val email = Confidential<Email>(emailData)

            val paymentNoticesData =
                data.get("paymentNotices") as? List<*>
                    ?: throw IllegalArgumentException("Missing paymentNotices in activation event")

            val paymentNotices =
                paymentNoticesData.map { noticeData ->
                    val notice = noticeData as Document
                    val transferList =
                        (notice.get("transferList") as? List<*>)?.map { transferData ->
                            val transfer = transferData as Document
                            PaymentTransferInformation().apply {
                                paFiscalCode = transfer.getString("paFiscalCode")
                                digitalStamp = transfer.getBoolean("digitalStamp", false)
                                transferAmount = transfer.getInteger("transferAmount")
                                transferCategory = transfer.getString("transferCategory")
                            }
                        } ?: emptyList()

                    PaymentNotice(
                        notice.getString("paymentToken"),
                        notice.getString("rptId"),
                        notice.getString("description"),
                        notice.getInteger("amount"),
                        null,
                        transferList,
                        notice.getBoolean("isAllCCP", false),
                        notice.getString("companyName"),
                        notice.getString("creditorReferenceId"),
                    )
                }

            val clientIdString =
                data.getString("clientId")
                    ?: throw IllegalArgumentException("Missing clientId in activation event")
            val clientId =
                Transaction.ClientId.fromString(clientIdString)
                    ?: throw IllegalArgumentException(
                        "Invalid clientId in activation event: $clientIdString"
                    )

            val creationDate =
                event.getString("creationDate")
                    ?: throw IllegalArgumentException("Missing creationDate in activation event")

            Transaction(
                transactionId,
                paymentNotices,
                null,
                email,
                TransactionStatusDto.ACTIVATED,
                clientId,
                creationDate,
                null,
                null,
                null,
                null,
                null,
                ZonedDateTime.parse(creationDate).toInstant().toEpochMilli(), // lastProcessedEventAt
            )
        }
    }
}
