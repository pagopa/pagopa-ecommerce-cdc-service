package it.pagopa.ecommerce.cdc

import com.mongodb.MongoException
import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import it.pagopa.ecommerce.cdc.services.EcommerceCDCEventDispatcherService
import java.time.Duration
import org.bson.BsonDocument
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
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
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(EcommerceTransactionsLogEventsStream::class.java)

    override fun run(args: ApplicationArguments) {
        logger.info("=== CDC Service Starting ===")
        logger.info("Application arguments: ${args.sourceArgs.joinToString(", ")}")
        logger.info("Change stream configuration: collection=${changeStreamOptionsConfig.collection}, operationType=${changeStreamOptionsConfig.operationType}")
        
        logger.info(
            "Starting transaction change stream consumer for collection: ${changeStreamOptionsConfig.collection}"
        )
        
        try {
            logger.info("Attempting to create change stream...")
            this.streamEcommerceTransactionsLogEvents()
                .doOnSubscribe {
                    logger.info(
                        "CDC service is now running and waiting for change stream events..."
                    )
                }
                .doOnError { error ->
                    logger.error("A critical error occurred in the change stream pipeline", error)
                }
                .doOnComplete {
                    logger.warn(
                        "Transaction change stream completed. The service might stop processing new events."
                    )
                }
                .blockLast()
        } catch (e: Exception) {
            logger.error("The change stream has been terminated by a fatal error.", e)
            logger.error("Exception details: ${e.javaClass.name}: ${e.message}")
            if (e.cause != null) {
                logger.error("Root cause: ${e.cause?.javaClass?.name}: ${e.cause?.message}")
            }
            e.printStackTrace()
        }
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
                    logger.debug("MongoDB template connection info: ${reactiveMongoTemplate.mongoDatabase.name}")
                    logger.debug("Change stream options: operationType=${changeStreamOptionsConfig.operationType}, project=${changeStreamOptionsConfig.project}")
                    
                    try {
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
                    } catch (e: Exception) {
                        logger.error("Failed to create change stream: ${e.javaClass.name}: ${e.message}")
                        if (e.cause != null) {
                            logger.error("Change stream creation cause: ${e.cause?.javaClass?.name}: ${e.cause?.message}")
                        }
                        throw e
                    }
                }
                .retryWhen(
                    Retry.fixedDelay(
                            retryStreamPolicyConfig.maxAttempts,
                            Duration.ofMillis(retryStreamPolicyConfig.intervalInMs),
                        )
                        .filter { t -> t is MongoException }
                        .doBeforeRetry { signal ->
                            logger.warn("Retrying connection to DB: ${signal.failure().message}")
                            logger.debug("Retry attempt details: ${signal.totalRetriesInARow() + 1}/${retryStreamPolicyConfig.maxAttempts}")
                        }
                )
                .doOnError { e ->
                    logger.error("Failed to connect to DB after retries: ${e.javaClass.name}: ${e.message}")
                    if (e.cause != null) {
                        logger.error("Final error cause: ${e.cause?.javaClass?.name}: ${e.cause?.message}")
                    }
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
