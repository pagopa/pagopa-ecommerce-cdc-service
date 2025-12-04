package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisJobLockPolicyConfig
import it.pagopa.ecommerce.cdc.exceptions.CdcEventProcessingLockNotAcquiredException
import it.pagopa.ecommerce.commons.redis.reactivetemplatewrappers.ReactiveExclusiveLockDocumentWrapper
import java.time.Duration
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class CdcLockServiceTest {
    private val reactiveExclusiveLockDocumentWrapper: ReactiveExclusiveLockDocumentWrapper = mock()
    private val redisJobLockPolicyConfig: RedisJobLockPolicyConfig =
        RedisJobLockPolicyConfig("lockkeyspace", 20, 2)
    private val cdcLockService: CdcLockService =
        CdcLockService(reactiveExclusiveLockDocumentWrapper, redisJobLockPolicyConfig)

    /*+ Lock tests **/

    @Test
    fun `Should acquire lock`() {
        // pre-requisites
        val eventId = "eventId"
        whenever(reactiveExclusiveLockDocumentWrapper.saveIfAbsent(any(), any()))
            .thenReturn(Mono.just(true))

        // Test
        val result = cdcLockService.acquireEventLock(eventId)
        StepVerifier.create(result).expectNext(true).verifyComplete()

        // verifications
        verify(reactiveExclusiveLockDocumentWrapper, times(1))
            .saveIfAbsent(
                argThat {
                    assertEquals(this.id, "lockkeyspace:lock:$eventId")
                    assertEquals(this.holderName, "pagopa-ecommerce-cdc-service")
                    true
                },
                eq(Duration.ofMillis(20)),
            )
    }

    @Test
    fun `Should throw LockNotAcquiredException when tryLock throw exception`() {
        // pre-requisites
        val eventId = "eventId"
        given(reactiveExclusiveLockDocumentWrapper.saveIfAbsent(any(), any()))
            .willReturn(Mono.error(RuntimeException("Test exception")))

        // Test
        val result = cdcLockService.acquireEventLock(eventId)
        StepVerifier.create(result)
            .expectError(CdcEventProcessingLockNotAcquiredException::class.java)
            .verify()

        // verifications
        verify(reactiveExclusiveLockDocumentWrapper, times(1))
            .saveIfAbsent(
                argThat {
                    assertEquals(this.id, "lockkeyspace:lock:$eventId")
                    assertEquals(this.holderName, "pagopa-ecommerce-cdc-service")
                    true
                },
                eq(Duration.ofMillis(20)),
            )
    }
}
