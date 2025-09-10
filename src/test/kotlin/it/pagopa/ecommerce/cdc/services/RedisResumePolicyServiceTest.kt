package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RedisResumePolicyConfig
import java.time.Duration
import java.time.Instant
import kotlin.test.assertTrue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@TestPropertySource(locations = ["classpath:application-test.properties"])
class RedisResumePolicyServiceTest {
    private val redisTemplate: TimestampRedisTemplate = mock()
    private val redisResumePolicyConfig: RedisResumePolicyConfig =
        RedisResumePolicyConfig(
            keyspace = "keyspace",
            target = "target",
            fallbackInMin = 100L,
            ttlInMin = 200L,
        )
    private lateinit var redisResumePolicyService: ResumePolicyService

    @BeforeEach
    fun initEventStream() {
        redisResumePolicyService = RedisResumePolicyService(redisTemplate, redisResumePolicyConfig)
    }

    @Test
    fun `redis resume policy will get default resume timestamp in case of cache miss`() {
        given { redisTemplate.findByKeyspaceAndTarget(anyOrNull(), anyOrNull()) }
            .willReturn(Mono.empty())

        StepVerifier.create(redisResumePolicyService.getResumeTimestamp())
            .assertNext { resumeTimestamp ->
                val now = Instant.now()
                val diff = Duration.between(resumeTimestamp, Instant.now())
                println(
                    "Resume timestamp: [$resumeTimestamp], difference with current timestamp: [$diff]"
                )
                // cache miss, we expect the fallback mechanism to take place returning now
                // (mockedValue) minus fallback property value
                assertTrue(diff >= Duration.ofMinutes(redisResumePolicyConfig.fallbackInMin))
            }
            .verifyComplete()
    }

    @Test
    fun `redis resume policy will get resume timestamp in case of cache hit`() {
        val mockedInstantValue = Instant.now()
        given { redisTemplate.findByKeyspaceAndTarget(anyOrNull(), anyOrNull()) }
            .willReturn(Mono.just(mockedInstantValue))

        StepVerifier.create(redisResumePolicyService.getResumeTimestamp())
            .assertNext { resumeTimestamp ->
                //
                assertEquals(mockedInstantValue, resumeTimestamp)
            }
            .verifyComplete()
    }

    @Test
    fun `redis resume policy will save resume timestamp`() {
        val expected: Instant = Instant.now()
        given(redisTemplate.save(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull()))
            .willReturn(Mono.just(true))

        StepVerifier.create(redisResumePolicyService.saveResumeTimestamp(expected))
            .expectNext(true)
            .verifyComplete()

        verify(redisTemplate, times(1)).save(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull())
    }
}
