package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisJobLockPolicyConfig
import it.pagopa.ecommerce.cdc.exceptions.CdcEventProcessingLockNotAcquiredException
import java.util.concurrent.TimeUnit
import org.redisson.api.RedissonReactiveClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Service responsible for managing distributed locking mechanisms for CDC event processing.
 * 
 * Uses Redis-based distributed locks to prevent concurrent processing of the same event
 * across multiple CDC service instances, ensuring data consistency and avoiding duplicate
 * processing of transaction events.
 */
@Service
class CdcLockService(
    private val redissonClient: RedissonReactiveClient,
    private val redisJobLockPolicyConfig: RedisJobLockPolicyConfig,
) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    fun acquireEventLock(eventId: String): Mono<Boolean> {
        logger.debug("Trying to acquire lock for event: {}", eventId)
        return Mono.defer {
                redissonClient
                    .getLock(redisJobLockPolicyConfig.getLockNameByEventId(eventId))
                    .tryLock(
                        redisJobLockPolicyConfig.waitTimeInMs,
                        redisJobLockPolicyConfig.ttlInMs,
                        TimeUnit.MILLISECONDS,
                    )
            }
            .onErrorMap { CdcEventProcessingLockNotAcquiredException(eventId, it) }
    }
}
