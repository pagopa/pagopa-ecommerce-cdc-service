package it.pagopa.ecommerce.cdc.services

import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.text.format
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Component

@Component
class TimestampRedisTemplate(private val redisTemplate: RedisTemplate<String, Instant>) {
    fun save(keyspace: String, cdcTarget: String, instant: Instant, ttl: Duration) {
        redisTemplate.opsForValue().set(compoundKeyWithKeyspace(keyspace, cdcTarget), instant, ttl)
    }

    fun findByKeyspaceAndTarget(keyspace: String, cdcTarget: String): Optional<Instant> {
        return Optional.ofNullable(
            redisTemplate.opsForValue().get(compoundKeyWithKeyspace(keyspace, cdcTarget))
        )
    }

    private fun compoundKeyWithKeyspace(keyspace: String, cdcTarget: String): String {
        return "%s:%s:%s".format(keyspace, "time", cdcTarget)
    }
}
