package it.pagopa.ecommerce.cdc.datacapture

import com.mongodb.MongoException
import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import it.pagopa.ecommerce.cdc.services.CdcLockService
import it.pagopa.ecommerce.cdc.services.EcommerceCDCEventDispatcherService
import it.pagopa.ecommerce.cdc.services.RedisResumePolicyService
import it.pagopa.ecommerce.cdc.utils.ViewUpdateTracingUtils
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.data.mongodb.core.ChangeStreamOptions
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime

/** Main CDC component that listens to MongoDB Change Streams for transaction events. */
@Component
class EcommerceTransactionsLogEventsStream(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
    private val changeStreamOptionsConfig: ChangeStreamOptionsConfig,
    private val ecommerceCDCEventDispatcherService: EcommerceCDCEventDispatcherService,
    private val retryStreamPolicyConfig: RetryStreamPolicyConfig,
    private val cdcLockService: CdcLockService,
    private val redisResumePolicyService: RedisResumePolicyService,
    @Value("\${cdc.resume.saveInterval}") private val saveInterval: Int,
    private val viewUpdateTracingUtils: ViewUpdateTracingUtils
) : ApplicationListener<ApplicationReadyEvent> {

    private val logger = LoggerFactory.getLogger(EcommerceTransactionsLogEventsStream::class.java)

    override fun onApplicationEvent(event: ApplicationReadyEvent) {

        logger.info(
            "Starting transaction change stream consumer for collection: ${changeStreamOptionsConfig.collection}"
        )
        streamEcommerceTransactionsLogEvents()
            .doOnSubscribe {
                logger.info("CDC service is now running and waiting for change stream events...")
            }
            .doOnError { error ->
                logger.error("A critical error occurred in the change stream pipeline", error)
            }
            .doOnComplete {
                logger.warn(
                    "Transaction change stream completed. The service might stop processing new events."
                )
            }
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe()
    }

    /**
     * Creates and starts the MongoDB Change Stream for transaction events. Implements retry logic
     * and error handling based on wallet CDC patterns.
     */
    fun streamEcommerceTransactionsLogEvents(): Flux<TransactionEvent<*>> {
        val flux: Flux<TransactionEvent<*>> =
            Flux.defer {
                logger.info(
                    "Connecting to MongoDB Change Stream for collection: ${changeStreamOptionsConfig.collection}"
                )
                redisResumePolicyService
                    .getResumeTimestamp()
                    .flatMapMany { resumeTimestamp ->
                        reactiveMongoTemplate
                            .changeStream(
                                changeStreamOptionsConfig.collection,
                                ChangeStreamOptions.builder()
                                    .filter(
                                        Aggregation.newAggregation(
                                            Aggregation.match(
                                                Criteria.where("operationType")
                                                    .`in`(
                                                        changeStreamOptionsConfig.operationType
                                                    )
                                            ),
                                            Aggregation.project(
                                                changeStreamOptionsConfig.project
                                            ),
                                        )
                                    )
                                    .resumeAt(resumeTimestamp)
                                    .build(),
                                TransactionEvent::class.java,
                            )
                            .flatMap {
                                mono { it.body }
                                    .onErrorResume { exception ->
                                        logger.warn(
                                            "Exception converting document to POJO, skipping document with id: [${
                                                it.raw?.fullDocument?.get(
                                                    "_id"
                                                )
                                            }]",
                                            exception,
                                        )
                                        Mono.empty()
                                    }
                            }
                    }
                    // Process the elements of the Flux
                    .flatMap { processEvent(it) }
                    .doOnNext { viewUpdateTracingUtils.addSpanForProcessedEvent(it) }
                    // Save resume token every n emitted elements
                    .index { changeEventFluxIndex, changeEventDocument ->
                        Pair(changeEventFluxIndex, changeEventDocument)
                    }
                    .flatMap { (changeEventFluxIndex, changeEventDocument) ->
                        saveCdcResumeToken(changeEventFluxIndex, changeEventDocument)
                    }
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
    private fun processEvent(event: TransactionEvent<*>?): Mono<TransactionEvent<*>> {
        return Mono.defer {
            event?.let { event ->
                cdcLockService
                    .acquireEventLock(event.id)
                    .filter { it == true }
                    .flatMap { ecommerceCDCEventDispatcherService.dispatchEvent(event) }
            } ?: Mono.empty()
        }
            .onErrorResume {
                logger.error("Error during event handling: ", it)
                Mono.empty()
            }
    }

    private fun saveCdcResumeToken(
        changeEventFluxIndex: Long,
        changeEventDocument: TransactionEvent<*>,
    ): Mono<TransactionEvent<*>> =
        Mono.defer {
            val resumeTimestamp =
                if (changeEventFluxIndex.plus(1).mod(saveInterval) == 0) {
                    val documentTimestamp = changeEventDocument.creationDate
                    if (!documentTimestamp.isNullOrBlank()) {
                        ZonedDateTime.parse(documentTimestamp).toInstant()
                    } else {
                        Instant.now()
                    }
                } else {
                    null
                }
            mono { resumeTimestamp }
                .flatMap { redisResumePolicyService.saveResumeTimestamp(it) }
                .thenReturn(changeEventDocument)
        }
            .subscribeOn(Schedulers.boundedElastic())
            .onErrorResume {
                logger.error("Error saving resume policy: ", it)
                Mono.empty()
            }
}
