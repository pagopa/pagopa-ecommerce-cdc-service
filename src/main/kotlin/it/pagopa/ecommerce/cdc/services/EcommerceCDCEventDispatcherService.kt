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
    fun dispatchEvent(event: Document): Mono<Document> =
        Mono.defer {
                // extract document fields
                val transactionId = event.getString("transactionId") ?: "unknown"
                val eventClass = event.getString("_class") ?: "unknown"
                val creationDate = event.getString("creationDate") ?: "unknown"

                logger.info(
                    "Handling new change stream event: transactionId: [{}], eventType: [{}], creationDate: [{}]",
                    transactionId,
                    eventClass,
                    creationDate,
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
                            "Retrying writing event on CDC queue due to: [{}]",
                            signal.failure().message,
                        )
                    }
            )
            .doOnError { e -> logger.error("Failed to send event after retries", e) }
            .map { event }

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
                val creationDate = event.getString("creationDate")

                // extract data from nested 'data' field if present
                val data = event.get("data") as? Document
                val paymentNotices = data?.get("paymentNotices") as? List<*>
                val clientId = data?.getString("clientId")

                logger.info(
                    "CDC Event Details: transactionId: [{}], eventId: [{}], eventCode: [{}], creationDate: [{}], clientId: [{}], paymentNotices: [{}]",
                    transactionId,
                    eventId,
                    eventCode,
                    creationDate,
                    clientId,
                    paymentNotices?.size ?: 0,
                )

                // TODO future iterations will add:
                // - Queue/topic publishing
                // - View updates with Redis caching

                event
            }
            .doOnSuccess {
                logger.debug(
                    "Successfully processed eventstore event: [{}]",
                    event.getString("_id"),
                )
            }
    }
}
