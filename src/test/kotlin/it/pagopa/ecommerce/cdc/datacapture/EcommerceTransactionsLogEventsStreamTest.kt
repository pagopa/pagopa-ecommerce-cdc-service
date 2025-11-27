package it.pagopa.ecommerce.cdc.datacapture

import com.mongodb.MongoException
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.OperationType
import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import it.pagopa.ecommerce.cdc.services.CdcLockService
import it.pagopa.ecommerce.cdc.services.EcommerceCDCEventDispatcherService
import it.pagopa.ecommerce.cdc.services.RedisResumePolicyService
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil.createMockChangeStreamEvent
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil.createMockChangeStreamEventWithNullDocument
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil.createSampleEventStoreEvent
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil.toDocument
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import kotlin.test.assertEquals
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito
import org.mockito.Mockito.lenient
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.data.mongodb.core.ChangeStreamEvent
import org.springframework.data.mongodb.core.ChangeStreamOptions
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class EcommerceTransactionsLogEventsStreamTest {

    private val reactiveMongoTemplate = Mockito.mock<ReactiveMongoTemplate>()
    private val cdcLockService: CdcLockService = mock()

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
    private val redisResumePolicyService: RedisResumePolicyService = mock()

    private val ecommerceTransactionsLogEventsStream: EcommerceTransactionsLogEventsStream =
        EcommerceTransactionsLogEventsStream(
            reactiveMongoTemplate,
            changeStreamOptionsConfig,
            ecommerceCDCEventDispatcherService,
            retryStreamPolicyConfig,
            cdcLockService,
            redisResumePolicyService,
            saveInterval,
        )

    @Test
    fun `should successfully process change stream events`() {
        val event = createSampleEventStoreEvent()
        val changeStreamEvent = createMockChangeStreamEvent(operationType = "insert", event = event)
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))
        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.just(changeStreamEvent))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(any())).willReturn(Mono.just(event))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(event).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService).dispatchEvent(event)
    }

    @Test
    fun `should handle MongoDB connection errors with retry`() {
        val mongoException = MongoException("Connection failed")
        val event = createSampleEventStoreEvent()
        val changeStreamEvent = createMockChangeStreamEvent(operationType = "insert", event = event)
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.error(mongoException))
            .willReturn(Flux.just(changeStreamEvent))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(any())).willReturn(Mono.just(event))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(event).verifyComplete()

        verify(reactiveMongoTemplate, times(2))
            .changeStream(
                eq("eventstore"),
                any<ChangeStreamOptions>(),
                eq(TransactionEvent::class.java),
            )
        verify(cdcLockService).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService)
            .dispatchEvent(
                argThat { dispatchedEvent ->
                    assertEquals(dispatchedEvent, event)
                    true
                }
            )
    }

    @Test
    fun `should handle null documents gracefully`() {
        val changeStreamEvent =
            createMockChangeStreamEventWithNullDocument(operationType = "insert")
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.just(changeStreamEvent))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(ecommerceCDCEventDispatcherService, never()).dispatchEvent(any())
    }

    @Test
    fun `should handle event dispatcher errors gracefully`() {
        val event = createSampleEventStoreEvent()
        val changeStreamEvent = createMockChangeStreamEvent(operationType = "insert", event = event)
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.just(changeStreamEvent))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(event))
            .willReturn(Mono.error(RuntimeException("Event processing failed")))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService).dispatchEvent(event)
    }

    @Test
    fun `should process multiple events in sequence`() {
        val event1 = createSampleEventStoreEvent(eventId = "event1")
        val event2 = createSampleEventStoreEvent(eventId = "event2")

        val changeStreamEvent1 =
            createMockChangeStreamEvent(operationType = "insert", event = event1)
        val changeStreamEvent2 =
            createMockChangeStreamEvent(operationType = "insert", event = event2)
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.just(changeStreamEvent1, changeStreamEvent2))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(event1))
            .willReturn(Mono.just(event1))
        given(ecommerceCDCEventDispatcherService.dispatchEvent(event2))
            .willReturn(Mono.just(event2))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(event1).expectNext(event2).verifyComplete()

        verify(cdcLockService, times(2)).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService).dispatchEvent(event1)
        verify(ecommerceCDCEventDispatcherService).dispatchEvent(event2)
        verify(cdcLockService, times(1)).acquireEventLock(event1.id)
        verify(cdcLockService, times(1)).acquireEventLock(event2.id)
    }

    @Test
    fun `should exhaust retry attempts for persistent MongoDB errors`() {
        val mongoException = MongoException("Persistent connection error")
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.error(mongoException))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result)
            .expectErrorMatches { it.message?.contains("Retries exhausted") == true }
            .verify(Duration.ofSeconds(10))

        verify(reactiveMongoTemplate, times((retryStreamPolicyConfig.maxAttempts + 1).toInt()))
            .changeStream(
                eq("eventstore"),
                any<ChangeStreamOptions>(),
                eq(TransactionEvent::class.java),
            )
    }

    @Test
    fun `should not retry for non-MongoDB exceptions`() {
        val runtimeException = RuntimeException("Non-MongoDB error")
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.error(runtimeException))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result)
            .expectError(RuntimeException::class.java)
            .verify(Duration.ofSeconds(5))

        verify(reactiveMongoTemplate, times(1))
            .changeStream(
                eq("eventstore"),
                any<ChangeStreamOptions>(),
                eq(TransactionEvent::class.java),
            )
    }

    @Test
    fun `should not process event when lock acquisition fails`() {
        val changeStreamEvent =
            createMockChangeStreamEvent(
                operationType = "insert",
                event = createSampleEventStoreEvent(),
            )
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.just(changeStreamEvent))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(false))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService, never()).dispatchEvent(any())
    }

    @Test
    fun `should handle lock acquisition errors gracefully`() {
        val changeStreamEvent =
            createMockChangeStreamEvent(
                operationType = "insert",
                event = createSampleEventStoreEvent(),
            )
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))

        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.just(changeStreamEvent))

        given(cdcLockService.acquireEventLock(any()))
            .willReturn(Mono.error(RuntimeException("Lock service error")))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).verifyComplete()

        verify(cdcLockService).acquireEventLock(any())
        verify(ecommerceCDCEventDispatcherService, never()).dispatchEvent(any())
    }

    @Test
    fun `should save resume token at save interval boundary`() {
        // Create test documents with timestamps
        val startDate = ZonedDateTime.now()
        val events =
            (1..12).map { i ->
                createSampleEventStoreEvent(
                    eventId = "event_$i",
                    creationDate = startDate + Duration.ofSeconds(i.toLong()),
                )
            }

        val changeStreamEvents =
            events.map { event ->
                createMockChangeStreamEvent(operationType = "insert", event = event)
            }

        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))
        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.fromIterable(changeStreamEvents))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(any())).willAnswer {
            Mono.just(it.arguments[0])
        }

        given(redisResumePolicyService.saveResumeTimestamp(any())).willReturn(Mono.just(true))

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNextCount(12).verifyComplete()

        // With saveInterval = 10, resume token should be saved only at positions 9 and 19
        // (0-based indexing: 9 = 10th element, 19 = 20th element)
        // Since we only have 12 elements, only position 9 should trigger save
        verify(redisResumePolicyService, times(1))
            .saveResumeTimestamp(ZonedDateTime.parse(events[9].creationDate).toInstant())
    }

    @Test
    fun `should not save resume token when not at interval boundary`() {
        val logEventStream =
            EcommerceTransactionsLogEventsStream(
                reactiveMongoTemplate,
                changeStreamOptionsConfig,
                ecommerceCDCEventDispatcherService,
                retryStreamPolicyConfig,
                cdcLockService,
                redisResumePolicyService,
                10,
            )
        // Create test documents - only 5 elements, so with saveInterval = 10, no saves should occur
        val startDate = ZonedDateTime.now()
        val events =
            (1..5).map { i ->
                createSampleEventStoreEvent(
                    eventId = "event_$i",
                    creationDate = startDate + Duration.ofSeconds(1.toLong()),
                )
            }

        val changeStreamEvents =
            events.map { event ->
                createMockChangeStreamEvent(operationType = "insert", event = event)
            }

        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))
        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.fromIterable(changeStreamEvents))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(any())).willAnswer {
            Mono.just(it.arguments[0])
        }

        val result = logEventStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNextCount(5).verifyComplete()

        // With saveInterval = 10 and only 5 elements, no saves should occur
        // (positions 0-4 don't trigger saves, need position 9 for first save)
        verify(redisResumePolicyService, never()).saveResumeTimestamp(any())
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

        val expectedTimestamp = ZonedDateTime.parse("2025-01-01T00:00:00.000000000Z[GMT]")
        val expectedInstant = expectedTimestamp.toInstant()

        val event = createSampleEventStoreEvent(creationDate = expectedTimestamp)

        val changeStreamEvent = createMockChangeStreamEvent(operationType = "insert", event = event)

        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))
        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.just(changeStreamEvent))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(any())).willReturn(Mono.just(event))

        given(redisResumePolicyService.saveResumeTimestamp(any())).willReturn(Mono.just(true))

        val result = customStream.streamEcommerceTransactionsLogEvents()

        StepVerifier.create(result).expectNext(event).verifyComplete()

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
        val startDate = ZonedDateTime.now()
        val events =
            (1..3).map { i ->
                createSampleEventStoreEvent(
                    eventId = "event_$i",
                    creationDate = startDate + Duration.ofSeconds(1),
                )
            }

        val changeStreamEvents =
            events.map { event ->
                createMockChangeStreamEvent(operationType = "insert", event = event)
            }

        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))
        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(Flux.fromIterable(changeStreamEvents))

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(any())).willAnswer {
            Mono.just(it.arguments[0])
        }

        // Mock saveResumeTimestamp to throw an exception
        given(redisResumePolicyService.saveResumeTimestamp(any()))
            .willThrow(RuntimeException("Redis connection error"))

        val result = customStream.streamEcommerceTransactionsLogEvents()

        // Stream should complete successfully despite the save errors - errors are caught
        StepVerifier.create(result).verifyComplete()

        // Verify that saveResumeTimestamp was called 3 times (all failed)
        verify(redisResumePolicyService, times(3)).saveResumeTimestamp(any())

        // Verify that event dispatching still occurred for all events
        verify(ecommerceCDCEventDispatcherService, times(3)).dispatchEvent(any())
    }

    @Test
    fun `should handle document parsing errors`() {
        Hooks.onOperatorDebug()
        val event = createSampleEventStoreEvent()
        val document = toDocument(event)
        val mockEvent = mock<ChangeStreamEvent<TransactionEvent<Any>>>()
        val mockRaw = mock<ChangeStreamDocument<Document>>()
        lenient().whenever(mockRaw.fullDocument).thenReturn(document)
        lenient().whenever(mockEvent.operationType).thenReturn(OperationType.valueOf("INSERT"))
        lenient().whenever(mockEvent.raw).thenReturn(mockRaw)
        lenient().whenever(mockEvent.body).thenThrow(IllegalArgumentException("cannot parse event"))
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))
        val changeStreamEvent = createMockChangeStreamEvent(operationType = "insert", event = event)
        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(
                Flux.fromIterable(
                    listOf(mockEvent as ChangeStreamEvent<TransactionEvent<*>>, changeStreamEvent)
                )
            )

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(event)).willAnswer {
            Mono.just(it.arguments[0])
        }

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()
        // flux should complete with an empty result, skipping event
        StepVerifier.create(result).expectNext(event).verifyComplete()

        verify(cdcLockService, times(1)).acquireEventLock(event.id)
        verify(ecommerceCDCEventDispatcherService, times(1)).dispatchEvent(event)
    }

    @Test
    fun `should skip events with ttl field set (marked as to be expired by scheduler service)`() {
        Hooks.onOperatorDebug()
        val event = createSampleEventStoreEvent()
        val document = toDocument(event)
        document["ttl"] = 123456
        val mockEvent = mock<ChangeStreamEvent<TransactionEvent<Any>>>()
        val mockRaw = mock<ChangeStreamDocument<Document>>()
        lenient().whenever(mockRaw.fullDocument).thenReturn(document)
        lenient().whenever(mockEvent.operationType).thenReturn(OperationType.valueOf("INSERT"))
        lenient().whenever(mockEvent.raw).thenReturn(mockRaw)
        given(redisResumePolicyService.getResumeTimestamp()).willReturn(Mono.just(Instant.now()))
        given(
                reactiveMongoTemplate.changeStream(
                    any<String>(),
                    any<ChangeStreamOptions>(),
                    eq(TransactionEvent::class.java),
                )
            )
            .willReturn(
                Flux.fromIterable(listOf(mockEvent as ChangeStreamEvent<TransactionEvent<*>>))
            )

        given(cdcLockService.acquireEventLock(any())).willReturn(Mono.just(true))

        given(ecommerceCDCEventDispatcherService.dispatchEvent(event)).willAnswer {
            Mono.just(it.arguments[0])
        }

        val result = ecommerceTransactionsLogEventsStream.streamEcommerceTransactionsLogEvents()
        // flux should complete with an empty result, skipping event
        StepVerifier.create(result).verifyComplete()

        verify(cdcLockService, times(0)).acquireEventLock(event.id)
        verify(ecommerceCDCEventDispatcherService, times(0)).dispatchEvent(event)
        // filtered out before reaching document-to-pojo conversion
        verify(mockEvent, times(0)).body
    }
}
