import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.cdc.utils.ViewUpdateTracingUtils
import it.pagopa.ecommerce.commons.documents.v2.TransactionActivatedEvent
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.NullSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify

class ViewUpdateTracingUtilsTest {

    private val openTelemetryUtils: OpenTelemetryUtils = mock()

    private val viewUpdateTracingUtils = ViewUpdateTracingUtils(openTelemetryUtils)

    @ParameterizedTest
    @ValueSource(strings = ["ACTIVATED"])
    @NullSource
    fun `should add span with correct attributes for processed event handling optional attributes`(
        exceptedViewStatus: String?
    ) {
        // --- ARRANGE ---

        val event: TransactionActivatedEvent = TransactionTestUtils.transactionActivateEvent()
        val expectedCode = event.eventCode
        val expectedTxId = event.transactionId
        val expectedDate = event.creationDate
        val expectedOutcome = "OK"

        // --- ACT ---
        viewUpdateTracingUtils.addSpanForProcessedEvent(event, expectedOutcome, exceptedViewStatus)

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
            assertEquals(
                exceptedViewStatus,
                capturedAttributes.get(
                    ViewUpdateTracingUtils
                        .CDC_EVENT_STORE_PROCESSED_EVENT_VIEW_STATUS_UPDATE_ATTRIBUTE_KEY
                ),
                "Outcome attribute mismatch",
            )
        }
    }
}
