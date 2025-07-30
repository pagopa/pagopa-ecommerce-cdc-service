package it.pagopa.ecommerce.cdc.utils

import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.OperationType
import java.time.ZonedDateTime
import org.bson.BsonDocument
import org.bson.Document
import org.mockito.Mockito.lenient
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.data.mongodb.core.ChangeStreamEvent

/** Utility class for creating test documents and change stream events for ecommerce CDC testing. */
object EcommerceChangeStreamDocumentUtil {

    private const val TEST_TRANSACTION_ID_1 = "93cce28d3b7c4cb9975e6d856ecee89f"
    private const val TEST_EVENT_ID_1 = "event_93cce28d3b7c4cb9975e6d856ecee89f"
    private const val TEST_PAYMENT_TOKEN_1 = "payment_token_93cce28d3b7c4cb9975e6d856ecee89f"

    /** Creates a sample eventstore document for testing purposes. */
    fun createSampleTransactionDocument(
        eventId: String = TEST_EVENT_ID_1,
        transactionId: String = TEST_TRANSACTION_ID_1,
        eventCode: String = "TRANSACTION_ACTIVATED_EVENT",
        eventClass: String = "it.pagopa.ecommerce.commons.documents.v2.TransactionActivatedEvent",
        creationDate: String? = ZonedDateTime.now().toString(),
    ): Document {
        return Document().apply {
            put("_id", eventId)
            put("transactionId", transactionId)
            put("eventCode", eventCode)
            put("_class", eventClass)
            put("creationDate", creationDate)
            put(
                "data",
                Document().apply {
                    put("email", Document().apply { put("data", "test@example.com") })
                    put(
                        "paymentNotices",
                        listOf(
                            Document().apply {
                                put("paymentToken", TEST_PAYMENT_TOKEN_1)
                                put("amount", 1000)
                                put("description", "Test payment")
                            }
                        ),
                    )
                    put("clientId", "CHECKOUT")
                    put("paymentTokenValiditySeconds", 900)
                },
            )
        }
    }

    /** Creates a mock ChangeStreamEvent for testing purposes using Mockito. */
    fun createMockChangeStreamEvent(
        operationType: String = "insert",
        fullDocument: Document? = null,
    ): ChangeStreamEvent<BsonDocument> {
        val document = fullDocument ?: createSampleTransactionDocument()

        val mockEvent = mock<ChangeStreamEvent<BsonDocument>>()
        val mockRaw = mock<ChangeStreamDocument<Document>>()

        lenient().whenever(mockRaw.fullDocument).thenReturn(document)

        lenient()
            .whenever(mockEvent.operationType)
            .thenReturn(OperationType.valueOf(operationType.uppercase()))
        lenient().whenever(mockEvent.raw).thenReturn(mockRaw)

        return mockEvent
    }

    /**
     * Creates a mock ChangeStreamEvent with explicitly null fullDocument for testing null handling.
     */
    fun createMockChangeStreamEventWithNullDocument(
        operationType: String = "insert"
    ): ChangeStreamEvent<BsonDocument> {
        val mockEvent = mock<ChangeStreamEvent<BsonDocument>>()
        val mockRaw = mock<ChangeStreamDocument<Document>>()

        lenient().whenever(mockRaw.fullDocument).thenReturn(null)

        lenient()
            .whenever(mockEvent.operationType)
            .thenReturn(OperationType.valueOf(operationType.uppercase()))
        lenient().whenever(mockEvent.raw).thenReturn(mockRaw)

        return mockEvent
    }
}
