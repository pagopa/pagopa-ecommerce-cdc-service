package it.pagopa.ecommerce.cdc.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties

/** Configuration properties for MongoDB Change Stream options. */
@ConfigurationProperties(prefix = "cdc.ecommerce-transactions-log-events")
data class ChangeStreamOptionsConfig(
    // mongodb collection name to monitor for changes
    val collection: String,
    // list of operation types to monitor (insert, update, delete, etc.)
    val operationType: List<String>,
    // projection fields for change stream events
    val project: String,
)
