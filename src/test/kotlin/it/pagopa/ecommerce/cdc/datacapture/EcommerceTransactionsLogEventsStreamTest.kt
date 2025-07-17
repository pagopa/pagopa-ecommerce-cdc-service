package it.pagopa.ecommerce.cdc.datacapture

import com.mongodb.MongoException
import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import it.pagopa.ecommerce.cdc.services.EcommerceCDCEventDispatcherService
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil
import java.time.Duration
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.given
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.boot.ApplicationArguments
import org.springframework.data.mongodb.core.ChangeStreamOptions
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class EcommerceTransactionsLogEventsStreamTest {

    companion object {
        private const val TEST_TRANSACTION_ID_1 = "93cce28d3b7c4cb9975e6d856ecee89f"
        private const val TEST_TRANSACTION_ID_2 = "a1b2c3d4e5f6789012345678901234ab"
    }

    private val reactiveMongoTemplate = Mockito.mock<ReactiveMongoTemplate>()
    private val ecommerceCDCEventDispatcherService =
        Mockito.mock<EcommerceCDCEventDispatcherService>()
    private val changeStreamOptionsConfig =
        ChangeStreamOptionsConfig(
            collection = "eventstore",
            operationType = listOf("insert", "update", "replace"),
            project = "fullDocument",
        )
    private val retryStreamPolicyConfig =
        RetryStreamPolicyConfig(maxAttempts = 3, intervalInMs = 1000)

    private lateinit var ecommerceTransactionsLogEventsStream: EcommerceTransactionsLogEventsStream

    @BeforeEach
    fun setup() {
        ecommerceTransactionsLogEventsStream =
            EcommerceTransactionsLogEventsStream(
                reactiveMongoTemplate,
                changeStreamOptionsConfig,
                ecommerceCDCEventDispatcherService,
                retryStreamPolicyConfig,
            )
    }

    @Test
    fun `should start change stream on application run and process one event`() {
        val sampleDocument = EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument()
        val changeStreamEvent =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                operationType = "insert",
                fullDocument = sampleDocument,
            )

        val mockFlux = Flux.just(changeStreamEvent)

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .willReturn(mockFlux)

        given(ecommerceCDCEventDispatcherService.dispatchEvent(any()))
            .willReturn(Mono.just(sampleDocument))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()

        verify(reactiveMongoTemplate, times(1))
            .changeStream(
                eq("eventstore"),
                any<ChangeStreamOptions>(),
                eq(BsonDocument::class.java),
            )

        verify(ecommerceCDCEventDispatcherService, times(1)).dispatchEvent(eq(sampleDocument))
    }

    @Test
    fun `should successfully process change stream events`() {
        val sampleDocument = EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument()
        val changeStreamEvent =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                operationType = "insert",
                fullDocument = sampleDocument,
            )

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.just(changeStreamEvent))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(any()))
            .thenReturn(Mono.just(sampleDocument))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()

        verify(ecommerceCDCEventDispatcherService).dispatchEvent(sampleDocument)
    }

    @Test
    fun `should handle MongoDB connection errors with retry`() {
        val mongoException = MongoException("Connection failed")
        val sampleDocument = EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument()
        val changeStreamEvent =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                operationType = "insert",
                fullDocument = sampleDocument,
            )

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.error(mongoException))
            .thenReturn(Flux.just(changeStreamEvent))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(sampleDocument))
            .thenReturn(Mono.just(sampleDocument))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()

        verify(reactiveMongoTemplate, times(2))
            .changeStream(
                eq("eventstore"),
                any<ChangeStreamOptions>(),
                eq(BsonDocument::class.java),
            )
        verify(ecommerceCDCEventDispatcherService).dispatchEvent(sampleDocument)
    }

    @Test
    fun `should handle null documents gracefully`() {
        val changeStreamEvent =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEventWithNullDocument(
                operationType = "insert"
            )

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.just(changeStreamEvent))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(ecommerceCDCEventDispatcherService, never()).dispatchEvent(any())
    }

    @Test
    fun `should handle event dispatcher errors gracefully`() {
        val sampleDocument = EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument()
        val changeStreamEvent =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                operationType = "insert",
                fullDocument = sampleDocument,
            )

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.just(changeStreamEvent))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(sampleDocument))
            .thenReturn(Mono.error(RuntimeException("Event processing failed")))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(ecommerceCDCEventDispatcherService).dispatchEvent(sampleDocument)
    }

    @Test
    fun `should process multiple events in sequence`() {
        val document1 =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                transactionId = TEST_TRANSACTION_ID_1,
                eventCode = "TRANSACTION_ACTIVATED_EVENT",
            )
        val document2 =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                transactionId = TEST_TRANSACTION_ID_2,
                eventCode = "TRANSACTION_AUTHORIZATION_REQUESTED_EVENT",
            )

        val event1 =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                operationType = "insert",
                fullDocument = document1,
            )
        val event2 =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                operationType = "update",
                fullDocument = document2,
            )

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.just(event1, event2))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(document1))
            .thenReturn(Mono.just(document1))
        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(document2))
            .thenReturn(Mono.just(document2))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(document1).expectNext(document2).verifyComplete()

        verify(ecommerceCDCEventDispatcherService).dispatchEvent(document1)
        verify(ecommerceCDCEventDispatcherService).dispatchEvent(document2)
    }

    @Test
    fun `should exhaust retry attempts for persistent MongoDB errors`() {
        val mongoException = MongoException("Persistent connection error")

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.error(mongoException))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result)
            .expectErrorMatches { it.message?.contains("Retries exhausted") == true }
            .verify(Duration.ofSeconds(10))

        verify(reactiveMongoTemplate, times((retryStreamPolicyConfig.maxAttempts + 1).toInt()))
            .changeStream(
                eq("eventstore"),
                any<ChangeStreamOptions>(),
                eq(BsonDocument::class.java),
            )
    }

    @Test
    fun `should not retry for non-MongoDB exceptions`() {
        val runtimeException = RuntimeException("Non-MongoDB error")

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.error(runtimeException))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result)
            .expectError(RuntimeException::class.java)
            .verify(Duration.ofSeconds(5))

        verify(reactiveMongoTemplate, times(1))
            .changeStream(
                eq("eventstore"),
                any<ChangeStreamOptions>(),
                eq(BsonDocument::class.java),
            )
    }

    @Test
    fun `should execute doOnComplete callback when stream completes in run method`() {
        val spy = spy(ecommerceTransactionsLogEventsStream)
        val mockArguments = Mockito.mock<ApplicationArguments>()

        doReturn(Flux.empty<Document>()).whenever(spy).streamEcommerceTransactionsLogEvents()

        spy.run(mockArguments)

        verify(spy).streamEcommerceTransactionsLogEvents()
    }
}
