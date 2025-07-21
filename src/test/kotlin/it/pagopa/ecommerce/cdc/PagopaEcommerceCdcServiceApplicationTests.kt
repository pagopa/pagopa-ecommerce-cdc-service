package it.pagopa.ecommerce.cdc

import org.junit.jupiter.api.Test
import org.redisson.api.RedissonReactiveClient
import org.redisson.spring.starter.RedissonAutoConfigurationV2
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoBean

@SpringBootTest
@TestPropertySource(locations = ["classpath:application-test.properties"])
@EnableAutoConfiguration(exclude = [RedissonAutoConfigurationV2::class])
class PagopaEcommerceCdcServiceApplicationTests {
    @MockitoBean private lateinit var redissonReactiveClient: RedissonReactiveClient

    @Test fun contextLoads() {}
}
