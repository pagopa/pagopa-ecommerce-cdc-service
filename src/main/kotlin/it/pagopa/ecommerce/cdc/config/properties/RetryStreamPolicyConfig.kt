package it.pagopa.ecommerce.cdc.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties

/** Configuration properties for retry policy when connecting to MongoDB Change Streams. */
@ConfigurationProperties(prefix = "cdc.retry-stream")
data class RetryStreamPolicyConfig(
    /** max number of retry attempts for stream connection */
    val maxAttempts: Long,
    /** interval between retry attempts in milliseconds */
    val intervalInMs: Long,
)
