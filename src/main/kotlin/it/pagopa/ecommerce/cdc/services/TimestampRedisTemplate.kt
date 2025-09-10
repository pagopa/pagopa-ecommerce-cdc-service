package it.pagopa.ecommerce.cdc.services

import java.time.Duration
import java.time.Instant
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class TimestampRedisTemplate(private val redisTemplate: ReactiveRedisTemplate<String, Instant>) {

    fun save(keyspace: String, cdcTarget: String, instant: Instant, ttl: Duration): Mono<Boolean> {
        return redisTemplate
            .opsForValue()
            .set(compoundKeyWithKeyspace(keyspace, cdcTarget), instant, ttl)
    }

    fun findByKeyspaceAndTarget(keyspace: String, cdcTarget: String): Mono<Instant> {
        return redisTemplate.opsForValue()[compoundKeyWithKeyspace(keyspace, cdcTarget)]
    }

    private fun compoundKeyWithKeyspace(keyspace: String, cdcTarget: String): String {
        return "%s:%s:%s".format(keyspace, "time", cdcTarget)
    }
}
