package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.cdc.exceptions.CdcException
import it.pagopa.ecommerce.cdc.mdcutilities.CdcTracingUtils
import it.pagopa.ecommerce.cdc.utils.ViewUpdateTracingUtils
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import java.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.core.publisher.SignalType
import reactor.util.retry.Retry

/**
 * Service responsible for dispatching and processing transaction status change events.
 *
 * Processes CDC events by updating transaction views with event data using upsert operations.
 * Handles event payload updates while maintaining retry logic and error handling.
 */
@Component
class EcommerceCDCEventDispatcherService(
    private val retrySendPolicyConfig: RetrySendPolicyConfig,
    private val transactionViewUpsertService: TransactionViewUpsertService,
    private val viewUpdateTracingUtils: ViewUpdateTracingUtils,
) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    /**
     * Dispatches a transaction event for CDC processing. Logs event details and performs upsert
     * operations on the transaction view with retry logic and error handling.
     *
     * @param event The transaction event containing transaction data and metadata
     * @return Mono<TransactionEvent<*>> The processed transaction event
     */
    fun dispatchEvent(event: TransactionEvent<*>): Mono<TransactionEvent<*>> =
        Mono.defer { processTransactionEvent(event) }
            .retryWhen(
                Retry.fixedDelay(
                        retrySendPolicyConfig.maxAttempts,
                        Duration.ofMillis(retrySendPolicyConfig.intervalInMs),
                    )
                    .filter { t ->
                        if (t is CdcException) {
                            t.retriableError
                        } else {
                            true
                        }
                    }
                    .doAfterRetry { signal ->
                        val retryAttempt = signal.totalRetries()
                        val signalFailure = signal.failure()
                        CdcTracingUtils.withContextDetailsMdc(
                            mapOf(
                                "retryAttempt" to retryAttempt,
                                "maxRetryAttempts" to retrySendPolicyConfig.maxAttempts,
                                "signalFailure" to signalFailure,
                            )
                        ) {
                            logger.warn("Retried event processing after an error during process")
                        }
                    }
            )
            .doOnError { error ->
                CdcTracingUtils.withErrorMdc(error) { logger.error("Error processing event") }
            }
            .map { event }

    /**
     * Processes the transaction event by performing upsert operations on the transaction view.
     * Updates event payload data using conditional logic based on timestamps and logs detailed
     * event information for monitoring and debugging purposes.
     *
     * @param event The transaction event to process
     * @return Mono<TransactionEvent<*>> The processed transaction event
     */
    private fun processTransactionEvent(event: TransactionEvent<*>): Mono<TransactionEvent<*>> {

        return transactionViewUpsertService
            .upsertEventData(event)
            .doOnSuccess { logger.info("Successfully upserted transaction view") }
            .doOnError { error ->
                CdcTracingUtils.withErrorMdc(error) {
                    logger.error("Failed to upsert transaction view")
                }
            }
            .doFinally { signalType ->
                val outcome = if (signalType == SignalType.ON_ERROR) "ERROR" else "OK"
                viewUpdateTracingUtils.addSpanForProcessedEvent(event, outcome)
            }
            .then(Mono.just(event))
    }
}
