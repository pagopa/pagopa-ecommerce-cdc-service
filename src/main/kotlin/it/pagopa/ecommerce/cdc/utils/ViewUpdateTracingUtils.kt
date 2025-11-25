package it.pagopa.ecommerce.cdc.utils

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import org.springframework.stereotype.Component

@Component
class ViewUpdateTracingUtils(
    val openTelemetryUtils: OpenTelemetryUtils
) {
    companion object {
        const val CDC_EVENT_STORE_PROCESSED_EVENT_SPAN_NAME = "CDC event store processed event"
        val CDC_EVENT_STORE_PROCESSED_EVENT_CODE_ATTRIBUTE_KEY =
            AttributeKey.stringKey("ecommerce.cdc.processedEvent.eventCode")
    }


    fun addSpanForProcessedEvent(event: TransactionEvent<*>) {
        openTelemetryUtils.addSpanWithAttributes(
            CDC_EVENT_STORE_PROCESSED_EVENT_SPAN_NAME,
            Attributes.of(CDC_EVENT_STORE_PROCESSED_EVENT_CODE_ATTRIBUTE_KEY, event.eventCode)
        )
    }
}