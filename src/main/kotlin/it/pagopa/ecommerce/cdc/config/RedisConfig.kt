package it.pagopa.ecommerce.cdc.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import java.time.Instant
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer

/**
 * Redis configuration for the CDC service.
 *
 * Configures Redis connectivity and serialization for timestamp-based resume policy operations.
 * This configuration supports the CDC service's ability to resume MongoDB change stream processing
 * from the last successfully processed event timestamp.
 */
@Configuration
class RedisConfig {

    /**
     * Creates a Redis template configured for storing and retrieving Instant timestamps.
     *
     * The template is specifically configured for the CDC service's resume policy functionality,
     * allowing the service to persist and retrieve the last processed event timestamp to/from
     * Redis.
     *
     * @param redisConnectionFactory The Redis connection factory provided by Spring Boot
     *   autoconfiguration
     * @return RedisTemplate<String, Instant> configured for timestamp operations
     */
    @Bean
    fun reactiveRedisTemplate(
        reactiveRedisConnectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, Instant> {
        val mapper = ObjectMapper()
        mapper.registerModule(JavaTimeModule())
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        val jacksonRedisSerializer: Jackson2JsonRedisSerializer<Instant> =
            Jackson2JsonRedisSerializer(mapper, Instant::class.java)
        val serializationContext =
            RedisSerializationContext.newSerializationContext<String, Instant>(
                    StringRedisSerializer()
                )
                .value(jacksonRedisSerializer)
                .build()

        return ReactiveRedisTemplate(reactiveRedisConnectionFactory, serializationContext)
    }
}
