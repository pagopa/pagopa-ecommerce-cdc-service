package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import java.time.ZonedDateTime
import org.bson.Document
import org.slf4j.LoggerFactory
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
class TransactionViewUpsertService(private val mongoTemplate: ReactiveMongoTemplate) {

    private val logger = LoggerFactory.getLogger(TransactionViewUpsertService::class.java)

    /**
     * Performs an upsert operation for transaction view data based on the event content.
     *
     * @param transactionId The transaction identifier
     * @param event The MongoDB change stream event document
     * @return Mono<BaseTransactionView> The upserted transaction view document
     */
    fun upsertEventData(transactionId: String, event: Document): Mono<BaseTransactionView> {
        return Mono.defer {
                val eventCode = event.getString("eventCode") ?: "UNKNOWN"
                //                val creationDate = event.getString("creationDate")

                logger.debug(
                    "Upserting transaction view data for transactionId: [{}], eventCode: [{}]",
                    transactionId,
                    eventCode,
                )

                val query = Query.query(Criteria.where("transactionId").`is`(transactionId))
                val update = buildUpdateFromEvent(event)

                mongoTemplate
                    .upsert(query, update, "cdc-transactions-view")
                    .then(
                        mongoTemplate.findOne(
                            query,
                            BaseTransactionView::class.java,
                            "cdc-transactions-view",
                        )
                    )
            }
            .doOnSuccess { result ->
                logger.info(
                    "Successfully upserted transaction view for transactionId: [{}]",
                    transactionId,
                )
            }
            .doOnError { error ->
                logger.error(
                    "Failed to upsert transaction view for transactionId: [{}]",
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

        // apply updates based on specific event types
        when (eventCode) {
            "TRANSACTION_ACTIVATED_EVENT" -> updateActivationData(update, event)
            "TRANSACTION_AUTHORIZATION_REQUESTED_EVENT" -> updateAuthRequestData(update, event)
            "TRANSACTION_AUTHORIZATION_COMPLETED_EVENT" -> updateAuthCompletedData(update, event)
            "TRANSACTION_CLOSURE_REQUESTED_EVENT" -> updateClosureRequestData(update, event)
            "TRANSACTION_USER_RECEIPT_REQUESTED_EVENT" -> updateUserReceiptData(update, event)
            "TRANSACTION_EXPIRED_EVENT" -> updateExpiredData(update, event)
            "TRANSACTION_REFUND_REQUESTED_EVENT" -> updateRefundRequestData(update, event)
            "TRANSACTION_CANCELED_EVENT" -> updateCanceledData(update, event)
            "TRANSACTION_USER_CANCELED_EVENT" -> updateUserCanceledData(update, event)
            "TRANSACTION_CLOSED_EVENT" -> updateClosedData(update, event)
            "TRANSACTION_REFUND_ERROR_EVENT" -> updateRefundErrorData(update, event)
            "TRANSACTION_CLOSURE_ERROR_EVENT" -> updateClosureErrorData(update, event)
            "TRANSACTION_USER_RECEIPT_ADDED_EVENT" -> updateUserReceiptAddedData(update, event)
            "TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT" -> updateUserReceiptErrorData(update, event)
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
            eventData.getInteger("paymentTokenValiditySeconds")?.let {
                update.set("paymentTokenValiditySeconds", it)
            }

            event.getString("creationDate")?.let { update.set("creationDate", it) }

            event.getString("transactionId")?.let { update.set("transactionId", it) }

            update.set("_class", "it.pagopa.ecommerce.commons.documents.v2.Transaction")

            val paymentNotices = eventData.get("paymentNotices") as? List<*>
            if (!paymentNotices.isNullOrEmpty()) {
                val firstNotice = paymentNotices[0] as? Document
                firstNotice?.getInteger("amount")?.let { update.set("amount", it) }
            }
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

    /** Updates fields for TRANSACTION_CANCELED_EVENT. Adds cancellation information. */
    private fun updateCanceledData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        data?.let { eventData ->
            eventData.getString("cancelReason")?.let { update.set("cancelReason", it) }

            event.getString("creationDate")?.let { update.set("canceledAt", it) }
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

    /** Updates generic fields for unknown event types. Used as fallback for unrecognized events. */
    private fun updateGenericEventData(update: Update, event: Document): Update {
        val data = event.get("data") as? Document

        // save the entire event data for unmapped events
        data?.let { update.set("lastEventData", it) }

        return update
    }
}
