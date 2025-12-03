package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisJobLockPolicyConfig
import it.pagopa.ecommerce.cdc.exceptions.CdcEventProcessingLockNotAcquiredException
import it.pagopa.ecommerce.commons.redis.reactivetemplatewrappers.ReactiveExclusiveLockDocumentWrapper
import it.pagopa.ecommerce.commons.repositories.ExclusiveLockDocument
import java.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Service responsible for managing distributed locking mechanisms for CDC event processing.
 *
 * Uses Redis-based distributed locks to prevent concurrent processing of the same event across
 * multiple CDC service instances, ensuring data consistency and avoiding duplicate processing of
 * transaction events.
 */
@Service
class CdcLockService(
    private val reactiveExclusiveLockDocumentWrapper: ReactiveExclusiveLockDocumentWrapper,
    private val redisJobLockPolicyConfig: RedisJobLockPolicyConfig,
) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    /**
     * Attempts to acquire a distributed lock for processing a specific transaction event.
     *
     * This method uses Redis-based distributed locking to ensure that each transaction event is
     * processed by only one CDC service instance at a time. The lock acquisition includes a
     * configurable wait time and TTL to handle various failure scenarios.
     *
     * @param eventId The unique identifier of the transaction event to lock
     * @return Mono<Boolean> that emits true if lock was acquired, false if not available, or error
     *   if acquisition fails
     * @throws CdcEventProcessingLockNotAcquiredException if lock acquisition fails due to Redis
     *   errors
     */
    fun acquireEventLock(eventId: String): Mono<Boolean> {
        logger.debug("Trying to acquire lock for event: {}", eventId)
        return reactiveExclusiveLockDocumentWrapper
            .saveIfAbsent(
                ExclusiveLockDocument(
                    redisJobLockPolicyConfig.getLockNameByEventId(eventId),
                    "pagopa-ecommerce-cdc-service",
                ),
                Duration.ofMillis(redisJobLockPolicyConfig.ttlInMs),
            )
            .onErrorMap { CdcEventProcessingLockNotAcquiredException(eventId, it) }
    }
}
