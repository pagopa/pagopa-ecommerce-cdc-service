package it.pagopa.ecommerce.cdc.utils

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import org.bson.Document
import org.springframework.stereotype.Component

@Component
class ViewUpdateTracingUtils(val openTelemetryUtils: OpenTelemetryUtils) {
    companion object {
        const val CDC_EVENT_STORE_PROCESSED_EVENT_SPAN_NAME = "eventstoreCDCEvent"
        val CDC_EVENT_STORE_PROCESSED_EVENT_CODE_ATTRIBUTE_KEY: AttributeKey<String> =
            AttributeKey.stringKey("ecommerce.cdc.processedEvent.eventCode")
        val CDC_EVENT_STORE_PROCESSED_EVENT_TRANSACTION_ID_ATTRIBUTE_KEY: AttributeKey<String> =
            AttributeKey.stringKey("ecommerce.cdc.processedEvent.transactionId")
        val CDC_EVENT_STORE_PROCESSED_EVENT_CREATION_DATE_ATTRIBUTE_KEY: AttributeKey<String> =
            AttributeKey.stringKey("ecommerce.cdc.processedEvent.eventCreationDate")
        val CDC_EVENT_STORE_PROCESSED_EVENT_OUTCOME_ATTRIBUTE_KEY: AttributeKey<String> =
            AttributeKey.stringKey("ecommerce.cdc.processedEvent.outcome")
        val CDC_EVENT_STORE_PROCESSED_EVENT_VIEW_STATUS_UPDATE_ATTRIBUTE_KEY: AttributeKey<String> =
            AttributeKey.stringKey("ecommerce.cdc.processedEvent.viewStatusUpdate")
    }

    fun addSpanForProcessedEvent(
        event: TransactionEvent<*>,
        outcome: String,
        updateStatusDocument: Document,
    ) {
        val spanAttributes =
            updateStatusDocument.let { updateDocument ->
                var attributes =
                    Attributes.of(
                        CDC_EVENT_STORE_PROCESSED_EVENT_CODE_ATTRIBUTE_KEY,
                        event.eventCode,
                        CDC_EVENT_STORE_PROCESSED_EVENT_TRANSACTION_ID_ATTRIBUTE_KEY,
                        event.transactionId,
                        CDC_EVENT_STORE_PROCESSED_EVENT_CREATION_DATE_ATTRIBUTE_KEY,
                        event.creationDate,
                        CDC_EVENT_STORE_PROCESSED_EVENT_OUTCOME_ATTRIBUTE_KEY,
                        outcome,
                    )
                // add custom attributes taken from update query
                if (updateDocument[$$"$set"] is Document) {
                    val setDocument = updateDocument[$$"$set"] as Document
                    val viewStatus = setDocument["status"]?.toString()
                    if (viewStatus != null) {
                        attributes =
                            attributes
                                .toBuilder()
                                .put(
                                    CDC_EVENT_STORE_PROCESSED_EVENT_VIEW_STATUS_UPDATE_ATTRIBUTE_KEY,
                                    viewStatus,
                                )
                                .build()
                    }
                }
                attributes
            }

        openTelemetryUtils.addSpanWithAttributes(
            CDC_EVENT_STORE_PROCESSED_EVENT_SPAN_NAME,
            spanAttributes,
        )
    }
}
