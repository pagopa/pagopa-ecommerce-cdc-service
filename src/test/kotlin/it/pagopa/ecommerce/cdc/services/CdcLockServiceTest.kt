package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisJobLockPolicyConfig
import it.pagopa.ecommerce.cdc.exceptions.CdcEventProcessingLockNotAcquiredException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.redisson.api.RLockReactive
import org.redisson.api.RedissonReactiveClient
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.util.concurrent.TimeUnit

@ExtendWith(MockitoExtension::class)
class CdcLockServiceTest {
    private val rLockReactive: RLockReactive = mock()
    private val redissonClient: RedissonReactiveClient = mock()
    private val redisJobLockPolicyConfig: RedisJobLockPolicyConfig =
        RedisJobLockPolicyConfig("lockkeyspace", 20, 2)
    private val cdcLockService: CdcLockService =
        CdcLockService()

    /*+ Lock tests **/

    @Test
    fun `Should acquire lock`() {
        // pre-requisites
        val eventId = "eventId"
        whenever(redissonClient.getLock(any<String>())).thenReturn(rLockReactive)
        whenever(rLockReactive.tryLock(any(), any(), any())).thenReturn(Mono.just(true))

        // Test
        val result = cdcLockService.acquireEventLock(eventId)
        StepVerifier.create(result).expectNext(true).verifyComplete()

        // verifications
        verify(redissonClient, times(1)).getLock("lockkeyspace:lock:$eventId")
        verify(rLockReactive, times(1)).tryLock(2, 20, TimeUnit.MILLISECONDS)
    }

    @Test
    fun `Should throw LockNotAcquiredException when tryLock throw exception`() {
        // pre-requisites
        val eventId = "eventId"
        whenever(redissonClient.getLock(any<String>())).thenReturn(rLockReactive)
        whenever(rLockReactive.tryLock(any(), any(), any()))
            .thenThrow(RuntimeException("Test exception"))

        // Test
        val result = cdcLockService.acquireEventLock(eventId)
        StepVerifier.create(result)
            .expectError(CdcEventProcessingLockNotAcquiredException::class.java)
            .verify()

        // verifications
        verify(redissonClient, times(1)).getLock("lockkeyspace:lock:$eventId")
        verify(rLockReactive, times(1)).tryLock(2, 20, TimeUnit.MILLISECONDS)
    }

    @Test
    fun `Should throw LockNotAcquiredException when getLock throw exception`() {
        // pre-requisites
        val eventId = "eventId"
        whenever(redissonClient.getLock(any<String>()))
            .thenThrow(RuntimeException("Test exception"))

        // Test
        val result = cdcLockService.acquireEventLock(eventId)
        StepVerifier.create(result)
            .expectError(CdcEventProcessingLockNotAcquiredException::class.java)
            .verify()

        // verifications
        verify(redissonClient, times(1)).getLock("lockkeyspace:lock:$eventId")
    }
}
