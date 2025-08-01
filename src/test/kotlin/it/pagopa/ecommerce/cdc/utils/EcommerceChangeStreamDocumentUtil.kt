package it.pagopa.ecommerce.cdc.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.OperationType
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import java.time.ZonedDateTime
import org.bson.Document
import org.mockito.Mockito.lenient
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.data.mongodb.core.ChangeStreamEvent

/** Utility class for creating test documents and change stream events for ecommerce CDC testing. */
object EcommerceChangeStreamDocumentUtil {

    private val objectMapper = ObjectMapper()

    /** Creates a sample eventstore document for testing purposes. */
    fun createSampleEventStoreEvent(
        eventId: String = TransactionTestUtils.TRANSACTION_ID,
        creationDate: ZonedDateTime = ZonedDateTime.now(),
    ): TransactionEvent<*> {

        return TransactionTestUtils.transactionActivateEvent().apply {
            this.transactionId = eventId
            this.creationDate = creationDate.toString()
        }
    }

    fun toDocument(value: Any): Document = Document.parse(objectMapper.writeValueAsString(value))

    /** Creates a mock ChangeStreamEvent for testing purposes using Mockito. */
    fun createMockChangeStreamEvent(
        operationType: String = "insert",
        event: TransactionEvent<*>,
    ): ChangeStreamEvent<TransactionEvent<*>> {
        val document = toDocument(event)
        val mockEvent = mock<ChangeStreamEvent<TransactionEvent<Any>>>()
        val mockRaw = mock<ChangeStreamDocument<Document>>()
        lenient().whenever(mockRaw.fullDocument).thenReturn(document)
        lenient()
            .whenever(mockEvent.operationType)
            .thenReturn(OperationType.valueOf(operationType.uppercase()))
        lenient().whenever(mockEvent.raw).thenReturn(mockRaw)
        lenient().whenever(mockEvent.body).thenReturn(event as TransactionEvent<Any>)
        return mockEvent as ChangeStreamEvent<TransactionEvent<*>>
    }

    /**
     * Creates a mock ChangeStreamEvent with explicitly null fullDocument for testing null handling.
     */
    fun createMockChangeStreamEventWithNullDocument(
        operationType: String = "insert"
    ): ChangeStreamEvent<TransactionEvent<*>> {
        val mockEvent = mock<ChangeStreamEvent<TransactionEvent<*>>>()
        val mockRaw = mock<ChangeStreamDocument<Document>>()

        lenient().whenever(mockRaw.fullDocument).thenReturn(null)

        lenient()
            .whenever(mockEvent.operationType)
            .thenReturn(OperationType.valueOf(operationType.uppercase()))
        lenient().whenever(mockEvent.raw).thenReturn(mockRaw)

        return mockEvent
    }
}
