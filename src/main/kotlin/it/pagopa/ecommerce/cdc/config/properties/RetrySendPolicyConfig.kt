package it.pagopa.ecommerce.cdc.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties

/** Configuration properties for retry policy when sending events/messages. */
@ConfigurationProperties(prefix = "cdc.retry-send")
data class RetrySendPolicyConfig(
    /** max number of retry attempts for sending events */
    val maxAttempts: Long,
    /** interval between retry attempts in milliseconds */
    val intervalInMs: Long,
)
