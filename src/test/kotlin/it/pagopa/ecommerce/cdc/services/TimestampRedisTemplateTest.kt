package it.pagopa.ecommerce.cdc.services

import java.time.Duration
import java.time.Instant
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.ValueOperations
import org.springframework.test.context.TestPropertySource

@ExtendWith(MockitoExtension::class)
@TestPropertySource(locations = ["classpath:application-test.properties"])
class TimestampRedisTemplateTest {
    private val redisTemplate: RedisTemplate<String, Instant> = mock()
    private val valueOps: ValueOperations<String, Instant> = mock()
    private lateinit var timestampRedisTemplate: TimestampRedisTemplate

    @BeforeEach
    fun initTimeStampRedisTemplate() {
        timestampRedisTemplate = TimestampRedisTemplate(redisTemplate)
    }

    @Test
    fun `time stamp redis template saves instant`() {
        given { redisTemplate.opsForValue() }.willReturn(valueOps)
        doNothing().`when`(valueOps).set(anyOrNull(), anyOrNull(), any<Duration>())

        timestampRedisTemplate.save("keyspace", "target", Instant.now(), Duration.ofSeconds(0))
        verify(valueOps, times(1)).set(anyOrNull(), anyOrNull(), any<Duration>())
        verify(redisTemplate, times(1)).opsForValue()
    }

    @Test
    fun `time stamp redis template gets instant`() {
        val now = Instant.now()
        given { redisTemplate.opsForValue() }.willReturn(valueOps)
        given { valueOps.get(anyOrNull()) }.willReturn(now)

        val resumeTimestamp = timestampRedisTemplate.findByKeyspaceAndTarget("keyspace", "target")

        Assertions.assertTrue(!resumeTimestamp.isEmpty)
        Assertions.assertEquals(now, resumeTimestamp.get())
        verify(valueOps, times(1)).get(anyOrNull())
        verify(redisTemplate, times(1)).opsForValue()
    }
}
