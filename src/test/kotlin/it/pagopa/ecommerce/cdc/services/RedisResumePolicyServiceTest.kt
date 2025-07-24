package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisResumePolicyConfig
import java.time.Instant
import java.util.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.test.context.TestPropertySource

@ExtendWith(MockitoExtension::class)
@TestPropertySource(locations = ["classpath:application-test.properties"])
class RedisResumePolicyServiceTest {
    private val redisTemplate: TimestampRedisTemplate = mock()
    private val redisResumePolicyConfig: RedisResumePolicyConfig = mock()
    private lateinit var redisResumePolicyService: ResumePolicyService

    @BeforeEach
    fun initEventStream() {
        redisResumePolicyService = RedisResumePolicyService(redisTemplate, redisResumePolicyConfig)
    }

    @Test
    fun `redis resume policy will get default resume timestamp in case of cache miss`() {
        val emptyOptional: Optional<Instant> = mock()
        val expected: Instant = Instant.now()
        given { redisTemplate.findByKeyspaceAndTarget(anyOrNull(), anyOrNull()) }
            .willReturn(emptyOptional)
        given { emptyOptional.orElseGet(anyOrNull()) }.willReturn(expected)

        val actual = redisResumePolicyService.getResumeTimestamp()
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `redis resume policy will get resume timestamp in case of cache hit`() {
        val expected: Instant = Instant.now()
        given { redisTemplate.findByKeyspaceAndTarget(anyOrNull(), anyOrNull()) }
            .willReturn(Optional.of(expected))

        val actual = redisResumePolicyService.getResumeTimestamp()
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `redis resume policy will save resume timestamp`() {
        val expected: Instant = Instant.now()
        doNothing().`when`(redisTemplate).save(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull())

        redisResumePolicyService.saveResumeTimestamp(expected)

        verify(redisTemplate, times(1)).save(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull())
    }
}
