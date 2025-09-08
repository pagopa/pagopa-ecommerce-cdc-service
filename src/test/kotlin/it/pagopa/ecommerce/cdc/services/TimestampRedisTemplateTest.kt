package it.pagopa.ecommerce.cdc.services

import java.time.Duration
import java.time.Instant
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.ReactiveValueOperations
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@TestPropertySource(locations = ["classpath:application-test.properties"])
class TimestampRedisTemplateTest {
    private val redisTemplate: ReactiveRedisTemplate<String, Instant> = mock()
    private val valueOps: ReactiveValueOperations<String, Instant> = mock()
    private lateinit var timestampRedisTemplate: TimestampRedisTemplate

    @BeforeEach
    fun initTimeStampRedisTemplate() {
        timestampRedisTemplate = TimestampRedisTemplate(redisTemplate)
    }

    @Test
    fun `time stamp redis template saves instant`() {
        given { redisTemplate.opsForValue() }.willReturn(valueOps)
        given(valueOps.set(anyOrNull(), anyOrNull(), any<Duration>())).willReturn(Mono.just(true))

        timestampRedisTemplate.save("keyspace", "target", Instant.now(), Duration.ofSeconds(0))
        verify(valueOps, times(1)).set(anyOrNull(), anyOrNull(), any<Duration>())
        verify(redisTemplate, times(1)).opsForValue()
    }

    @Test
    fun `time stamp redis template gets instant`() {
        val now = Instant.now()
        given { redisTemplate.opsForValue() }.willReturn(valueOps)
        given { valueOps.get(anyOrNull()) }.willReturn(Mono.just(now))

        StepVerifier.create(timestampRedisTemplate.findByKeyspaceAndTarget("keyspace", "target"))
            .assertNext { resumeTimestamp -> assertEquals(now, resumeTimestamp) }
            .verifyComplete()
        verify(valueOps, times(1)).get(anyOrNull())
        verify(redisTemplate, times(1)).opsForValue()
    }
}
