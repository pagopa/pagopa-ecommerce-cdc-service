package it.pagopa.ecommerce.cdc

import com.mongodb.MongoException
import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import it.pagopa.ecommerce.cdc.services.EcommerceCDCEventDispatcherService
import java.time.Duration
import org.bson.BsonDocument
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.data.mongodb.core.ChangeStreamOptions
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry

/** Main CDC component that listens to MongoDB Change Streams for transaction events. */
@Component
class EcommerceTransactionsLogEventsStream(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
    private val changeStreamOptionsConfig: ChangeStreamOptionsConfig,
    private val ecommerceCDCEventDispatcherService: EcommerceCDCEventDispatcherService,
    private val retryStreamPolicyConfig: RetryStreamPolicyConfig,
) : ApplicationListener<ApplicationReadyEvent> {

    private val logger = LoggerFactory.getLogger(EcommerceTransactionsLogEventsStream::class.java)

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        logger.info(
            "Starting transaction change stream consumer for collection: ${changeStreamOptionsConfig.collection}"
        )
        this.streamEcommerceTransactionsLogEvents()
            .subscribe(
                {},
                { error -> logger.error("Error in transaction change stream: ", error) },
                { logger.info("Transaction change stream completed") },
            )
    }

    /**
     * Creates and starts the MongoDB Change Stream for transaction events. Implements retry logic
     * and error handling based on wallet CDC patterns.
     */
    fun streamEcommerceTransactionsLogEvents(): Flux<Document> {
        val flux: Flux<Document> =
            Flux.defer {
                    logger.info(
                        "Connecting to MongoDB Change Stream for collection: ${changeStreamOptionsConfig.collection}"
                    )
                    reactiveMongoTemplate
                        .changeStream(
                            changeStreamOptionsConfig.collection,
                            ChangeStreamOptions.builder()
                                .filter(
                                    Aggregation.newAggregation(
                                        Aggregation.match(
                                            Criteria.where("operationType")
                                                .`in`(changeStreamOptionsConfig.operationType)
                                        ),
                                        Aggregation.project(changeStreamOptionsConfig.project),
                                    )
                                )
                                // TODO add resume policy
                                .build(),
                            BsonDocument::class.java,
                        )
                        // Process the elements of the Flux
                        .flatMap { changeStreamEvent ->
                            processEvent(changeStreamEvent.raw?.fullDocument)
                        }
                        // TODO save resume token
                        .doOnError { logger.error("Error listening to change stream: ", it) }
                }
                .retryWhen(
                    Retry.fixedDelay(
                            retryStreamPolicyConfig.maxAttempts,
                            Duration.ofMillis(retryStreamPolicyConfig.intervalInMs),
                        )
                        .filter { t -> t is MongoException }
                        .doBeforeRetry { signal ->
                            logger.warn("Retrying connection to DB: ${signal.failure().message}")
                        }
                )
                .doOnError { e ->
                    logger.error("Failed to connect to DB after retries {}", e.message)
                }

        return flux
    }

    /**
     * Processes individual change stream events. Currently delegates to the CDC event dispatcher
     * service for logging.
     */
    private fun processEvent(event: Document?): Mono<Document> {
        return Mono.defer {
                // TODO acquireEventLock
                event?.let { ecommerceCDCEventDispatcherService.dispatchEvent(it) }
                    ?: run {
                        logger.warn("Received null document from change stream")
                        Mono.empty()
                    }
            }
            .onErrorResume {
                logger.error("Error during event handling: ", it)
                Mono.empty()
            }
    }

    // TODO resume policy
}
