package it.pagopa.ecommerce.cdc.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "cdc.redis-job-lock")
data class RedisJobLockPolicyConfig(
    val keyspace: String,
    val ttlInMs: Long,
    val waitTimeInMs: Long,
) {
    fun getLockNameByEventId(eventId: String): String = "%s:%s:%s".format(keyspace, "lock", eventId)
}
