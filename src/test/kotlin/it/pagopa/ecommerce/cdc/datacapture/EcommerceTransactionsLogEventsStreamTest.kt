package it.pagopa.ecommerce.cdc.datacapture

import com.mongodb.MongoException
import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import it.pagopa.ecommerce.cdc.services.CdcLockService
import it.pagopa.ecommerce.cdc.services.EcommerceCDCEventDispatcherService
import it.pagopa.ecommerce.cdc.services.RedisResumePolicyService
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil
import java.time.Duration
import java.time.Instant
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.boot.context.event.ApplicationReadyEvent
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
    private val cdcLockService: CdcLockService = mock()
    private val redisResumePolicyService: RedisResumePolicyService = mock()
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
    private val saveInterval = 10

    private lateinit var ecommerceTransactionsLogEventsStream: EcommerceTransactionsLogEventsStream

    @BeforeEach
    fun setup() {
        whenever(redisResumePolicyService.getResumeTimestamp()).thenReturn(Instant.now())

        ecommerceTransactionsLogEventsStream =
            EcommerceTransactionsLogEventsStream(
                reactiveMongoTemplate,
                changeStreamOptionsConfig,
                ecommerceCDCEventDispatcherService,
                retryStreamPolicyConfig,
                cdcLockService,
                redisResumePolicyService,
                saveInterval,
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

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

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

        verify(cdcLockService, times(1)).acquireEventLock(any())
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

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(any()))
            .thenReturn(Mono.just(sampleDocument))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
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

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

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
        verify(cdcLockService).acquireEventLock(any())
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

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(sampleDocument))
            .thenReturn(Mono.error(RuntimeException("Event processing failed")))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
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

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(document1))
            .thenReturn(Mono.just(document1))
        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(document2))
            .thenReturn(Mono.just(document2))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(document1).expectNext(document2).verifyComplete()

        verify(cdcLockService, times(2)).acquireEventLock(any())
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
    fun `should not process event when lock acquisition fails`() {
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

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(false))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService, never()).dispatchEvent(any())
    }

    @Test
    fun `should handle lock acquisition errors gracefully`() {
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

        whenever(cdcLockService.acquireEventLock(any()))
            .thenReturn(Mono.error(RuntimeException("Lock service error")))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService, never()).dispatchEvent(any())
    }

    @Test
    fun `should execute doOnComplete callback when stream completes in onApplicationEvent method`() {
        val spy = spy(ecommerceTransactionsLogEventsStream)
        val mockEvent = Mockito.mock<ApplicationReadyEvent>()

        doReturn(Flux.empty<Document>()).whenever(spy).streamEcommerceTransactionsLogEvents()

        spy.onApplicationEvent(mockEvent)

        verify(spy).streamEcommerceTransactionsLogEvents()
    }

    @Test
    fun `should save resume token at save interval boundary`() {
        // Create test documents with timestamps
        val documents =
            (1..12).map { i ->
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    eventId = "event_$i",
                    creationDate = "2024-01-15T10:30:0${i % 10}.123Z",
                )
            }

        val changeStreamEvents =
            documents.map { document ->
                EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                    operationType = "insert",
                    fullDocument = document,
                )
            }

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.fromIterable(changeStreamEvents))

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(any())).thenAnswer {
            Mono.just(it.arguments[0] as Document)
        }

        doNothing().whenever(redisResumePolicyService).saveResumeTimestamp(any())

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNextCount(12).verifyComplete()

        // With saveInterval = 10, resume token should be saved only at positions 9 and 19
        // (0-based indexing: 9 = 10th element, 19 = 20th element)
        // Since we only have 12 elements, only position 9 should trigger save
        verify(redisResumePolicyService, times(1)).saveResumeTimestamp(any())
    }

    @Test
    fun `should not save resume token when not at interval boundary`() {
        // Create test documents - only 5 elements, so with saveInterval = 10, no saves should occur
        val documents =
            (1..5).map { i ->
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    eventId = "event_$i",
                    creationDate = "2024-01-15T10:30:0${i}.123Z",
                )
            }

        val changeStreamEvents =
            documents.map { document ->
                EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                    operationType = "insert",
                    fullDocument = document,
                )
            }

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.fromIterable(changeStreamEvents))

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(any())).thenAnswer {
            Mono.just(it.arguments[0] as Document)
        }

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNextCount(5).verifyComplete()

        // With saveInterval = 10 and only 5 elements, no saves should occur
        // (positions 0-4 don't trigger saves, need position 9 for first save)
        verify(redisResumePolicyService, never()).saveResumeTimestamp(any())
    }

    @Test
    fun `should handle null and blank timestamps in save token`() {
        // Create a custom stream with saveInterval = 3 for easier testing
        val customStream =
            EcommerceTransactionsLogEventsStream(
                reactiveMongoTemplate,
                changeStreamOptionsConfig,
                ecommerceCDCEventDispatcherService,
                retryStreamPolicyConfig,
                cdcLockService,
                redisResumePolicyService,
                3, // saveInterval = 3
            )

        // Create documents with null, empty, and blank creationDate timestamps
        val documentWithNull =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                eventId = "event_null",
                creationDate = null,
            )
        val documentWithEmpty =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                eventId = "event_empty",
                creationDate = "",
            )
        val documentWithBlank =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                eventId = "event_blank",
                creationDate = "   ",
            )

        val changeStreamEvents =
            listOf(
                EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                    "insert",
                    documentWithNull,
                ),
                EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                    "insert",
                    documentWithEmpty,
                ),
                EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                    "insert",
                    documentWithBlank,
                ),
            )

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.fromIterable(changeStreamEvents))

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(any())).thenAnswer {
            Mono.just(it.arguments[0] as Document)
        }

        doNothing().whenever(redisResumePolicyService).saveResumeTimestamp(any())

        val result = customStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNextCount(3).verifyComplete()

        // With saveInterval = 3, the 3rd element (index 2) should trigger save
        // Even with null/empty/blank timestamps, save should still occur using Instant.now()
        // fallback
        verify(redisResumePolicyService, times(1)).saveResumeTimestamp(any())
    }

    @Test
    fun `should parse valid timestamp from document in save token`() {
        // Create a custom stream with saveInterval = 1 for easier testing
        val customStream =
            EcommerceTransactionsLogEventsStream(
                reactiveMongoTemplate,
                changeStreamOptionsConfig,
                ecommerceCDCEventDispatcherService,
                retryStreamPolicyConfig,
                cdcLockService,
                redisResumePolicyService,
                1, // saveInterval = 1
            )

        val expectedTimestamp = "2024-01-15T10:30:45.123Z"
        val expectedInstant = Instant.parse(expectedTimestamp)

        val document =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                eventId = "event_with_timestamp",
                creationDate = expectedTimestamp,
            )

        val changeStreamEvent =
            EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                operationType = "insert",
                fullDocument = document,
            )

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.just(changeStreamEvent))

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(any()))
            .thenReturn(Mono.just(document))

        doNothing().whenever(redisResumePolicyService).saveResumeTimestamp(any())

        val result = customStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(document).verifyComplete()

        // Verify that saveResumeTimestamp was called exactly once with the expected timestamp
        verify(redisResumePolicyService, times(1)).saveResumeTimestamp(eq(expectedInstant))
    }

    @Test
    fun `should handle save resume token errors gracefully`() {
        // Create a custom stream with saveInterval = 1 for easier testing
        val customStream =
            EcommerceTransactionsLogEventsStream(
                reactiveMongoTemplate,
                changeStreamOptionsConfig,
                ecommerceCDCEventDispatcherService,
                retryStreamPolicyConfig,
                cdcLockService,
                redisResumePolicyService,
                1, // saveInterval = 1
            )

        val documents =
            (1..3).map { i ->
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    eventId = "event_$i",
                    creationDate = "2024-01-15T10:30:0${i}.123Z",
                )
            }

        val changeStreamEvents =
            documents.map { document ->
                EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent(
                    operationType = "insert",
                    fullDocument = document,
                )
            }

        whenever(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(BsonDocument::class.java),
                )
            )
            .thenReturn(Flux.fromIterable(changeStreamEvents))

        whenever(cdcLockService.acquireEventLock(any())).thenReturn(Mono.just(true))

        whenever(ecommerceCDCEventDispatcherService.dispatchEvent(any())).thenAnswer {
            Mono.just(it.arguments[0] as Document)
        }

        // Mock saveResumeTimestamp to throw an exception
        whenever(redisResumePolicyService.saveResumeTimestamp(any()))
            .thenThrow(RuntimeException("Redis connection error"))

        val result = customStream.streamEcommerceTransactionsLogEvents()

        // Stream should complete successfully despite the save errors - errors are caught
        StepVerifier.create(result).verifyComplete()

        // Verify that saveResumeTimestamp was called 3 times (all failed)
        verify(redisResumePolicyService, times(3)).saveResumeTimestamp(any())

        // Verify that event dispatching still occurred for all events
        verify(ecommerceCDCEventDispatcherService, times(3)).dispatchEvent(any())
    }
}
