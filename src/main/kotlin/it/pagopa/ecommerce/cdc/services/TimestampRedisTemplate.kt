package it.pagopa.ecommerce.cdc.services

import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.text.format
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.stereotype.Component

// extension function for indexed accessor syntax with TTL
// this is to avoid sonar warning on rule kotlin:S6518
// https://next.sonarqube.com/sonarqube/coding_rules?open=kotlin%3AS6518&rule_key=kotlin%3AS6518
operator fun ValueOperations<String, Instant>.set(key: String, ttl: Duration, value: Instant) {
    set(key, value, ttl)
}

@Component
class TimestampRedisTemplate(private val redisTemplate: RedisTemplate<String, Instant>) {
    fun save(keyspace: String, cdcTarget: String, instant: Instant, ttl: Duration) {
        redisTemplate.opsForValue()[compoundKeyWithKeyspace(keyspace, cdcTarget), ttl] = instant
    }

    fun findByKeyspaceAndTarget(keyspace: String, cdcTarget: String): Optional<Instant> {
        return Optional.ofNullable(
            redisTemplate.opsForValue()[compoundKeyWithKeyspace(keyspace, cdcTarget)]
        )
    }

    private fun compoundKeyWithKeyspace(keyspace: String, cdcTarget: String): String {
        return "%s:%s:%s".format(keyspace, "time", cdcTarget)
    }
}
