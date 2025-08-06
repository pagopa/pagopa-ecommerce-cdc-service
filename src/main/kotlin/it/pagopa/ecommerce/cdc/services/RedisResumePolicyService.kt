package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisResumePolicyConfig
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

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

    override fun getResumeTimestamp(): Instant {
        return redisTemplate
            .findByKeyspaceAndTarget(
                redisResumePolicyConfig.keyspace,
                redisResumePolicyConfig.target,
            )
            .orElseGet {
                logger.warn(
                    "Resume timestamp not found on Redis, fallback on Instant.now()-{} minutes",
                    redisResumePolicyConfig.fallbackInMin,
                )
                Instant.now().minus(redisResumePolicyConfig.fallbackInMin, ChronoUnit.MINUTES)
            }
    }

    override fun saveResumeTimestamp(timestamp: Instant) {
        logger.debug("Saving instant: {}", timestamp.toString())
        redisTemplate.save(
            redisResumePolicyConfig.keyspace,
            redisResumePolicyConfig.target,
            timestamp,
            Duration.ofMinutes(redisResumePolicyConfig.ttlInMin),
        )
    }
}
