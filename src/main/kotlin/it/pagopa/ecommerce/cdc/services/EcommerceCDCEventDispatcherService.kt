package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import java.time.Duration
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.util.retry.Retry

/**
 * Service responsible for dispatching and processing transaction status change events.
 *
 * Currently implements logging-only functionality as per initial requirements. Future iterations
 * will add queue/topic publishing capabilities
 */
@Component
class EcommerceCDCEventDispatcherService(private val retrySendPolicyConfig: RetrySendPolicyConfig) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    /**
     * Dispatches a collection change event for processing. Currently logs the event details in
     * structured format.
     *
     * @param event The MongoDB change stream document containing eventstore data
     * @return Mono<Document> The processed document
     */
    fun dispatchEvent(event: Document?): Mono<Document> =
        if (event != null) {
            Mono.defer {
                    // extract document fields
                    val eventId = event.getString("_id") ?: "unknown"
                    val transactionId = event.getString("transactionId") ?: "unknown"
                    val eventCode = event.getString("eventCode") ?: "unknown"
                    val eventClass = event.getString("_class") ?: "unknown"
                    val creationDate = event.getString("creationDate") ?: "unknown"

                    logger.info(
                        "Processing eventstore change event: eventId={}, transactionId={}, eventCode={}, eventType={}, creationDate={}, fullDocument={}",
                        eventId,
                        transactionId,
                        eventCode,
                        eventClass,
                        creationDate,
                        event.toJson(),
                    )

                    // TODO tracing + send event to queue/topic
                    processTransactionEvent(event)
                }
                .retryWhen(
                    Retry.fixedDelay(
                            retrySendPolicyConfig.maxAttempts,
                            Duration.ofMillis(retrySendPolicyConfig.intervalInMs),
                        )
                        .filter { t -> t is Exception }
                        .doBeforeRetry { signal ->
                            logger.warn(
                                "Retrying writing event on CDC queue due to: ${signal.failure().message}"
                            )
                        }
                )
                .doOnError { e ->
                    logger.error("Failed to send event after retries: {}", e.message)
                }
                .map { event }
        } else {
            logger.warn("Received null transaction event for processing")
            Mono.empty()
        }

    /**
     * Processes the transaction event based on its type and content. Currently implements
     * structured logging for all event types.
     *
     * @param event The transaction change document
     * @return Mono<Document> The processed document
     */
    private fun processTransactionEvent(event: Document): Mono<Document> {
        return Mono.fromCallable {
                val eventId = event.getString("_id")
                val transactionId = event.getString("transactionId")
                val eventCode = event.getString("eventCode")
                val eventClass = event.getString("_class")
                val creationDate = event.getString("creationDate")
                
                // extract data from nested 'data' field if present
                val data = event.get("data") as? Document
                val email = data?.get("email")?.let { 
                    when (it) {
                        is String -> it
                        is Document -> it.getString("data")
                        else -> null
                    }
                }
                val paymentNotices = data?.get("paymentNotices") as? List<*>
                val clientId = data?.getString("clientId")

                logger.info(
                    "Eventstore CDC Event Details: eventId={}, transactionId={}, eventCode={}, eventClass={}, creationDate={}, clientId={}, email={}, paymentNotices={}, processingTimestamp={}",
                    eventId,
                    transactionId,
                    eventCode,
                    eventClass,
                    creationDate,
                    clientId,
                    email?.let { maskEmail(it) }, // mask PII data
                    paymentNotices?.size ?: 0,
                    System.currentTimeMillis(),
                )

                // TODO future iterations will add:
                // - Event validation and sanitization
                // - Queue/topic publishing
                // - View updates with Redis caching
                // - Metrics collection

                event
            }
            .doOnSuccess {
                logger.debug("Successfully processed eventstore event: ${event.getString("_id")}")
            }
    }

    /**
     * Masks email addresses for privacy compliance in logs.
     *
     * @param email The email address to mask
     * @return Masked email address
     */
    private fun maskEmail(email: String): String {
        return if (email.contains("@")) {
            val parts = email.split("@")
            val localPart = parts[0]
            val domain = parts[1]
            val maskedLocal =
                if (localPart.length > 2) {
                    localPart.take(2) + "*".repeat(localPart.length - 2)
                } else {
                    "*".repeat(localPart.length)
                }
            "$maskedLocal@$domain"
        } else {
            "***"
        }
    }
}
