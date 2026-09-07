package it.pagopa.ecommerce.cdc.datacapture

import com.mongodb.MongoException
import it.pagopa.ecommerce.cdc.config.properties.ChangeStreamOptionsConfig
import it.pagopa.ecommerce.cdc.config.properties.RetryStreamPolicyConfig
import it.pagopa.ecommerce.cdc.liveness.CustomLivenessIndicator
import it.pagopa.ecommerce.cdc.services.CdcLockService
import it.pagopa.ecommerce.cdc.services.EcommerceCDCEventDispatcherService
import it.pagopa.ecommerce.cdc.services.RedisResumePolicyService
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
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
) : ApplicationListener<ApplicationReadyEvent> {

    private val logger = LoggerFactory.getLogger(EcommerceTransactionsLogEventsStream::class.java)

    override fun onApplicationEvent(event: ApplicationReadyEvent) {

        streamEcommerceTransactionsLogEvents()
            .doOnSubscribe { CustomLivenessIndicator.cdcStreamUpAndRunning.set(true) }
            .doOnError { error ->
                LogTracingUtils.loggerTracingUtils()
                    .failure()
                    .logError(
                        logger,
                        error,
                        "A critical error occurred in the change stream pipeline",
                    )
                CustomLivenessIndicator.cdcStreamUpAndRunning.set(false)
            }
            .doOnComplete {
                LogTracingUtils.loggerTracingUtils()
                    .failure()
                    .logWarn(
                        logger,
                        "Transaction change stream completed. The service might stop processing new events.",
                    )
                CustomLivenessIndicator.cdcStreamUpAndRunning.set(false)
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
                                .doOnNext {
                                    CustomLivenessIndicator.lastDequeuedEventAt = Instant.now()
                                }
                                .filter {
                                    /*
                                       @formatter:off
                                       events are immutable, once written they are never update by eCommerce application
                                       except for the data migration process that set TTL at document level.
                                       this update is seen as an update operation and this is the reason for this filter as per CDC listening operations
                                       list this cannot be set to insert only as it produce an error at CDC configuration level
                                       see here https://learn.microsoft.com/en-us/answers/questions/356668/how-to-get-inserted-change-stream-data-use-cosmosd
                                       unfortunately the operationType returned by CosmosDB driver is null, so no filter can be done on operationType field.
                                       For this reason an applicative filter have been applied here to exclude all those document that have ttl field set:
                                       eCommerce services does not set ttl field explicitly saving event to event store so when a detected changed document
                                       contains TTL field valued it can be skipped
                                    */
                                    val fullDocument = it.raw?.fullDocument
                                    val skipDocument = fullDocument?.containsKey("ttl") ?: false
                                    if (skipDocument) {
                                        LogTracingUtils.loggerTracingUtils()
                                            .success()
                                            .details(
                                                mapOf(
                                                    "skipped_event_id" to
                                                        fullDocument.get("_id")?.toString()
                                                )
                                            )
                                            .logInfo(logger, "Skipped event")
                                    }
                                    return@filter !skipDocument
                                }
                                .flatMap {
                                    mono { it.body }
                                        .onErrorResume { _ ->
                                            LogTracingUtils.loggerTracingUtils()
                                                .success()
                                                .details(
                                                    mapOf(
                                                        "raw_full_document" to
                                                            it.raw?.fullDocument.toString()
                                                    )
                                                )
                                                .logInfo(
                                                    logger,
                                                    "Exception converting document to POJO",
                                                )
                                            Mono.empty()
                                        }
                                }
                        }
                        // Process the elements of the Flux
                        .flatMap { currentEvent ->
                            processEvent(currentEvent).contextWrite { context ->
                                LogTracingUtils.enrichContextForEvent(
                                    mapOf(
                                        LogTracingUtils.AttributeKeys.CTX_TRANSACTION_ID to
                                            currentEvent.transactionId,
                                        LogTracingUtils.AttributeKeys.CTX_EVENT_CODE to
                                            currentEvent.eventCode,
                                        LogTracingUtils.AttributeKeys.CTX_EVENT_ID to
                                            currentEvent.id,
                                        LogTracingUtils.AttributeKeys.EVENT_ACTION to
                                            "PROCESS_CDC_EVENT",
                                    ),
                                    context,
                                )
                            }
                        }
                        // Save resume token every n emitted elements
                        .index { changeEventFluxIndex, changeEventDocument ->
                            Pair(changeEventFluxIndex, changeEventDocument)
                        }
                        .flatMap { (changeEventFluxIndex, changeEventDocument) ->
                            saveCdcResumeToken(changeEventFluxIndex, changeEventDocument)
                        }
                        .doOnError { error ->
                            LogTracingUtils.loggerTracingUtils()
                                .failure()
                                .logError(logger, error, "Error listening to change stream")
                        }
                }
                .retryWhen(
                    Retry.fixedDelay(
                            retryStreamPolicyConfig.maxAttempts,
                            Duration.ofMillis(retryStreamPolicyConfig.intervalInMs),
                        )
                        .filter { t -> t is MongoException }
                        .doAfterRetry { signal ->
                            LogTracingUtils.loggerTracingUtils()
                                .failure()
                                .details(mapOf("retry_failure_message" to signal.failure().message))
                                .logWarn(logger, "Connection restored to DB")
                        }
                )
                .doOnError { error ->
                    LogTracingUtils.loggerTracingUtils()
                        .failure()
                        .logError(logger, error, "Failed to connect to DB after retries")
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
                        .doOnNext {
                            LogTracingUtils.loggerTracingUtils()
                                .success()
                                .dependency(LogTracingUtils.REDIS_DEPENDENCY)
                                .logInfo(logger, "Acquired lock")
                        }
                        .flatMap { ecommerceCDCEventDispatcherService.dispatchEvent(event) }
                } ?: Mono.empty()
            }
            .onErrorResume { error ->
                LogTracingUtils.loggerTracingUtils()
                    .failure()
                    .logError(logger, error, "Error during event handling")
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
            .doOnSuccess {
                LogTracingUtils.loggerTracingUtils()
                    .success()
                    .dependency(LogTracingUtils.REDIS_DEPENDENCY)
                    .logInfo(logger, "Saved resume policy")
            }
            .onErrorResume { error ->
                LogTracingUtils.loggerTracingUtils()
                    .failure()
                    .dependency(LogTracingUtils.REDIS_DEPENDENCY)
                    .logError(logger, error, "Error saving resume policy")
                Mono.empty()
            }
}
