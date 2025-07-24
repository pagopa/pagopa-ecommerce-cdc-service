package it.pagopa.ecommerce.cdc

import it.pagopa.ecommerce.cdc.datacapture.EcommerceTransactionsLogEventsStream
import it.pagopa.ecommerce.cdc.services.*
import java.time.Instant
import org.junit.jupiter.api.Test
import org.redisson.api.RedissonReactiveClient
import org.redisson.spring.starter.RedissonAutoConfigurationV2
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration
import org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoBean

@SpringBootTest
@TestPropertySource(locations = ["classpath:application-test.properties"])
@EnableAutoConfiguration(
    exclude =
        [
            RedissonAutoConfigurationV2::class,
            RedisAutoConfiguration::class,
            RedisRepositoriesAutoConfiguration::class,
            MongoAutoConfiguration::class,
            MongoDataAutoConfiguration::class,
            MongoRepositoriesAutoConfiguration::class,
        ]
)
class PagopaEcommerceCdcServiceApplicationTests {
    @MockitoBean private lateinit var redissonReactiveClient: RedissonReactiveClient
    @MockitoBean private lateinit var redisConnectionFactory: RedisConnectionFactory
    @MockitoBean private lateinit var redisTemplate: RedisTemplate<String, Instant>
    @MockitoBean private lateinit var timestampRedisTemplate: TimestampRedisTemplate
    @MockitoBean private lateinit var reactiveMongoTemplate: ReactiveMongoTemplate
    @MockitoBean private lateinit var cdcLockService: CdcLockService
    @MockitoBean private lateinit var redisResumePolicyService: RedisResumePolicyService
    @MockitoBean
    private lateinit var ecommerceCDCEventDispatcherService: EcommerceCDCEventDispatcherService
    @MockitoBean
    private lateinit var ecommerceTransactionsLogEventsStream: EcommerceTransactionsLogEventsStream

    @Test fun contextLoads() {}
}
