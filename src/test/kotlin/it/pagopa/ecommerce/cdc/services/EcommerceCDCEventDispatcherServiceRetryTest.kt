package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil
import java.time.Duration
import org.bson.Document
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import reactor.util.retry.Retry

/**
 * Test class focused on retry scenarios and error handling. This class tests retry logic and error
 * handling by creating a service that simulates the same retry behavior as
 * EcommerceCDCEventDispatcherService.
 */
@ExtendWith(MockitoExtension::class)
class EcommerceCDCEventDispatcherServiceRetryTest {

    companion object {
        private const val TEST_TRANSACTION_ID_RETRY = "93cce28d3b7c4cb9975e6d856ecee89f"
        private const val TEST_TRANSACTION_ID_FAIL = "a1b2c3d4e5f6789012345678901234ab"
        private const val TEST_TRANSACTION_ID_ERROR = "f1e2d3c4b5a6978012345678901234cd"
    }

    private val retrySendPolicyConfig = RetrySendPolicyConfig(maxAttempts = 3, intervalInMs = 100)
    private lateinit var testService: TestServiceWithRetry

    @BeforeEach
    fun setup() {
        testService = TestServiceWithRetry(retrySendPolicyConfig)
    }

    @Test
    fun `should retry on exceptions and eventually succeed`() {
        // service that fails twice then succeeds (to test retry warning logs)
        val sampleDocument =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                transactionId = TEST_TRANSACTION_ID_RETRY,
                eventCode = "TRANSACTION_ACTIVATED_EVENT",
            )

        testService.setFailureCount(2) // fail twice, then succeed

        val result = testService.processWithRetry(sampleDocument)

        // should eventually succeed after retries
        StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()
    }

    @Test
    fun `should exhaust retries and fail`() {
        // service that always fails
        val sampleDocument =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                transactionId = TEST_TRANSACTION_ID_FAIL,
                eventCode = "TRANSACTION_ACTIVATED_EVENT",
            )

        testService.setFailureCount(10) // always fail (more than maxAttempts)

        val result = testService.processWithRetry(sampleDocument)

        // should fail after exhausting retries
        StepVerifier.create(result)
            .expectError(RuntimeException::class.java)
            .verify(Duration.ofSeconds(5))
    }

    @Test
    fun `should not retry on non-Exception errors`() {
        // service that throws non-Exception error
        val sampleDocument =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                transactionId = TEST_TRANSACTION_ID_ERROR,
                eventCode = "TRANSACTION_ACTIVATED_EVENT",
            )

        testService.setThrowError(true) // throw Error instead of Exception

        val result = testService.processWithRetry(sampleDocument)

        // should fail immediately without retries (tests filter logic)
        StepVerifier.create(result)
            .expectError(AssertionError::class.java)
            .verify(Duration.ofSeconds(2))
    }

    /**
     * Test service that simulates the retry behavior of EcommerceCDCEventDispatcherService to test
     * retry scenarios and error handling.
     */
    private class TestServiceWithRetry(private val retrySendPolicyConfig: RetrySendPolicyConfig) {

        private var failureCount = 0
        private var currentAttempt = 0
        private var throwError = false

        fun setFailureCount(count: Int) {
            this.failureCount = count
            this.currentAttempt = 0
        }

        fun setThrowError(throwError: Boolean) {
            this.throwError = throwError
        }

        fun processWithRetry(event: Document): Mono<Document> {
            return Mono.defer {
                    if (throwError) {
                        // throw Error (not Exception) to test filter logic
                        throw AssertionError("Test error that should not be retried")
                    }

                    currentAttempt++
                    if (currentAttempt <= failureCount) {
                        // fail with RuntimeException to trigger retry
                        throw RuntimeException("Test failure attempt $currentAttempt")
                    }

                    // success case
                    Mono.just(event)
                }
                .retryWhen(
                    Retry.fixedDelay(
                            retrySendPolicyConfig.maxAttempts,
                            Duration.ofMillis(retrySendPolicyConfig.intervalInMs),
                        )
                        .filter { t -> t is Exception }
                        .doBeforeRetry { signal ->
                            println(
                                "Retrying writing event on CDC queue due to: ${signal.failure().message}"
                            )
                        }
                )
                .doOnError { e -> println("Failed to send event after retries: ${e.message}") }
        }
    }
}
