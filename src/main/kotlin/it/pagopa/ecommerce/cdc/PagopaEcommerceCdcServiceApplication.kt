package it.pagopa.ecommerce.cdc

import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(
    ChangeStreamOptionsConfig::class,
    RetrySendPolicyConfig::class,
    RetryStreamPolicyConfig::class,
)
class PagopaEcommerceCdcServiceApplication

fun main(args: Array<String>) {
    runApplication<PagopaEcommerceCdcServiceApplication>(*args)
}
