package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisResumePolicyConfig
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

/**
 * Redis-based implementation of the resume policy service for CDC change stream operations.
 *
 * Manages timestamp persistence in Redis to support resumable MongoDB change stream processing.
 * This allows the CDC service to resume from the last processed event after restarts or failures,
 * ensuring no events are lost during service interruptions.
 */
@Service
class RedisResumePolicyService(
    private val redisTemplate: TimestampRedisTemplate,
    private val redisResumePolicyConfig: RedisResumePolicyConfig,
) : ResumePolicyService {
    private val logger = LoggerFactory.getLogger(RedisResumePolicyService::class.java)

    /**
     * Retrieves the last saved resume timestamp from Redis for MongoDB change stream resumption.
     *
     * This method supports fault-tolerant CDC processing by allowing the service to resume from the
     * last successfully processed event after restarts or failures. If no timestamp is found in
     * Redis, it falls back to a configurable time window before the current time.
     *
     * @return Instant representing the timestamp from which to resume change stream processing
     */
    override fun getResumeTimestamp(): Mono<Instant> {
        return redisTemplate
            .findByKeyspaceAndTarget(
                redisResumePolicyConfig.keyspace,
                redisResumePolicyConfig.target,
            )
            .switchIfEmpty {
                logger.warn(
                    "Resume timestamp not found on Redis, fallback on Instant.now()-{} minutes",
                    redisResumePolicyConfig.fallbackInMin,
                )
                Mono.just(
                    Instant.now().minus(redisResumePolicyConfig.fallbackInMin, ChronoUnit.MINUTES)
                )
            }
    }

    /**
     * Persists a resume timestamp to Redis for future MongoDB change stream resumption.
     *
     * The timestamp is saved with a configurable TTL to prevent indefinite storage growth while
     * ensuring the resume token remains available for reasonable restart scenarios. This method is
     * called periodically during change stream processing to maintain resumption capability.
     *
     * @param timestamp The Instant timestamp to save for resume operations
     */
    override fun saveResumeTimestamp(timestamp: Instant): Mono<Boolean> {
        logger.debug("Saving instant: {}", timestamp.toString())
        return redisTemplate.save(
            redisResumePolicyConfig.keyspace,
            redisResumePolicyConfig.target,
            timestamp,
            Duration.ofMinutes(redisResumePolicyConfig.ttlInMin),
        )
    }
}
