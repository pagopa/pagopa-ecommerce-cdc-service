import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.cdc.utils.ViewUpdateTracingUtils
import it.pagopa.ecommerce.commons.documents.v2.TransactionActivatedEvent
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class ViewUpdateTracingUtilsTest {

    private val openTelemetryUtils: OpenTelemetryUtils = mock()

    private val viewUpdateTracingUtils = ViewUpdateTracingUtils(openTelemetryUtils)

    @Test
    fun `should add span with correct attributes for processed event`() {
        // --- ARRANGE ---
        val expectedCode = "TRANSACTION_ACTIVATED"
        val expectedTxId = "tx-12345"
        val expectedDate = "2023-10-05T12:00:00Z"
        val expectedOutcome = "OK"

        val event: TransactionActivatedEvent = mock()
        whenever(event.eventCode).thenReturn(expectedCode)
        whenever(event.transactionId).thenReturn(expectedTxId)
        whenever(event.creationDate).thenReturn(expectedDate)

        // --- ACT ---
        viewUpdateTracingUtils.addSpanForProcessedEvent(event, expectedOutcome)

        // --- ASSERT ---
        argumentCaptor<Attributes> {
            verify(openTelemetryUtils)
                .addSpanWithAttributes(
                    eq(ViewUpdateTracingUtils.CDC_EVENT_STORE_PROCESSED_EVENT_SPAN_NAME),
                    capture(),
                )

            val capturedAttributes = firstValue

            assertEquals(
                expectedCode,
                capturedAttributes.get(
                    ViewUpdateTracingUtils.CDC_EVENT_STORE_PROCESSED_EVENT_CODE_ATTRIBUTE_KEY
                ),
                "Event code attribute mismatch",
            )
            assertEquals(
                expectedTxId,
                capturedAttributes.get(
                    ViewUpdateTracingUtils
                        .CDC_EVENT_STORE_PROCESSED_EVENT_TRANSACTION_ID_ATTRIBUTE_KEY
                ),
                "Transaction ID attribute mismatch",
            )
            assertEquals(
                expectedDate,
                capturedAttributes.get(
                    ViewUpdateTracingUtils
                        .CDC_EVENT_STORE_PROCESSED_EVENT_CREATION_DATE_ATTRIBUTE_KEY
                ),
                "Creation date attribute mismatch",
            )
            assertEquals(
                expectedOutcome,
                capturedAttributes.get(
                    ViewUpdateTracingUtils.CDC_EVENT_STORE_PROCESSED_EVENT_OUTCOME_ATTRIBUTE_KEY
                ),
                "Outcome attribute mismatch",
            )
        }
    }
}
