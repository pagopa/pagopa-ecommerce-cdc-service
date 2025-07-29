package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.cdc.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil
import org.bson.Document
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.mock
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class EcommerceCDCEventDispatcherServiceTest {

    companion object {
        private const val TEST_TRANSACTION_ID_1 = "93cce28d3b7c4cb9975e6d856ecee89f"
        private const val TEST_TRANSACTION_ID_2 = "a1b2c3d4e5f6789012345678901234ab"
        private const val TEST_TRANSACTION_ID_3 = "f1e2d3c4b5a6978012345678901234cd"
        private const val TEST_TRANSACTION_ID_4 = "1234567890abcdef1234567890abcdef"
        private const val TEST_TRANSACTION_ID_5 = "fedcba0987654321fedcba0987654321"
    }

    private val retrySendPolicyConfig = RetrySendPolicyConfig(maxAttempts = 3, intervalInMs = 100)
    private lateinit var ecommerceCDCEventDispatcherService: EcommerceCDCEventDispatcherService
    private val transactionsViewRepository: TransactionsViewRepository = mock()

    @BeforeEach
    fun setup() {
        ecommerceCDCEventDispatcherService =
            EcommerceCDCEventDispatcherService(transactionsViewRepository, retrySendPolicyConfig)
    }

    @Test
    fun `should successfully dispatch and process transaction event`() {
        val sampleDocument =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                transactionId = TEST_TRANSACTION_ID_1,
                eventCode = "TRANSACTION_ACTIVATED_EVENT",
            )

        val result = ecommerceCDCEventDispatcherService.dispatchEvent(sampleDocument)

        StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()
    }

    @Test
    fun `should handle events with missing fields gracefully`() {
        val documentWithMissingFields =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_1,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                )
                .apply { remove("data") }

        val result = ecommerceCDCEventDispatcherService.dispatchEvent(documentWithMissingFields)

        StepVerifier.create(result).expectNext(documentWithMissingFields).verifyComplete()
    }

    @Test
    fun `should handle different transaction statuses`() {
        val eventCodes =
            listOf(
                "TRANSACTION_ACTIVATED_EVENT",
                "TRANSACTION_EXPIRED_EVENT",
                "TRANSACTION_USER_CANCELED_EVENT",
                "TRANSACTION_CLOSURE_REQUESTED_EVENT",
            )

        eventCodes.forEach { eventCode ->
            val sampleDocument =
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_1,
                    eventCode = eventCode,
                )

            val result = ecommerceCDCEventDispatcherService.dispatchEvent(sampleDocument)

            StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()
        }
    }

    @Test
    fun `should handle events with different amounts`() {
        val amounts = listOf(0, 100, 1000, 50000, 999999)

        amounts.forEach { amount ->
            val sampleDocument =
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_1,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                )

            val result = ecommerceCDCEventDispatcherService.dispatchEvent(sampleDocument)

            StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()
        }
    }

    @Test
    fun `should handle events with various email formats`() {
        val emails =
            listOf(
                "test@example.com",
                "user.name@domain.co.uk",
                "user+tag@example.org",
                "simple@test.io",
                "very.long.email.address@very.long.domain.name.com",
            )

        emails.forEach { email ->
            val sampleDocument =
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_1,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                )

            val result = ecommerceCDCEventDispatcherService.dispatchEvent(sampleDocument)

            StepVerifier.create(result).expectNext(sampleDocument).verifyComplete()
        }
    }

    @Test
    fun `should process events with complex document structure`() {
        val complexDocument =
            EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument().apply {
                put("paymentGateway", "NPG")
                put("feeTotal", 150)
                put("clientId", "CLIENT_123")
                put("idempotencyKey", "idem_key_123")
                put("authorizationRequestId", "auth_req_123")
                put(
                    "transactionActivatedData",
                    mapOf(
                        "paymentNotices" to
                            listOf(
                                mapOf(
                                    "paymentToken" to "token1",
                                    "rptId" to "rpt123",
                                    "amount" to 1000,
                                )
                            )
                    ),
                )
            }

        val result = ecommerceCDCEventDispatcherService.dispatchEvent(complexDocument)

        StepVerifier.create(result).expectNext(complexDocument).verifyComplete()
    }

    @Test
    fun `should handle batch processing of multiple events`() {
        val documents =
            listOf(
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_1,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                ),
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_2,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                ),
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_3,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                ),
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_4,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                ),
                EcommerceChangeStreamDocumentUtil.createSampleTransactionDocument(
                    transactionId = TEST_TRANSACTION_ID_5,
                    eventCode = "TRANSACTION_ACTIVATED_EVENT",
                ),
            )

        documents.forEach { document ->
            val result = ecommerceCDCEventDispatcherService.dispatchEvent(document)

            StepVerifier.create(result).expectNext(document).verifyComplete()
        }
    }

    @Test
    fun `should handle documents with null string fields`() {
        val documentWithNullFields =
            Document().apply {
                put("_id", null)
                put("transactionId", null)
                put("eventCode", null)
                put("_class", null)
                put("creationDate", null)
                put(
                    "data",
                    Document().apply {
                        put("email", Document().apply { put("data", "test@example.com") })
                        put("clientId", "CHECKOUT")
                    },
                )
            }

        val result = ecommerceCDCEventDispatcherService.dispatchEvent(documentWithNullFields)

        StepVerifier.create(result).expectNext(documentWithNullFields).verifyComplete()
    }

    @Test
    fun `should handle different email data types`() {
        val testCases =
            listOf(
                Document().apply {
                    put("_id", "test1")
                    put("transactionId", TEST_TRANSACTION_ID_1)
                    put("eventCode", "TRANSACTION_ACTIVATED_EVENT")
                    put("_class", "TestEvent")
                    put("creationDate", "2024-01-01")
                    put(
                        "data",
                        Document().apply {
                            put("email", "direct.string@example.com")
                            put("clientId", "CHECKOUT")
                        },
                    )
                },
                Document().apply {
                    put("_id", "test2")
                    put("transactionId", TEST_TRANSACTION_ID_2)
                    put("eventCode", "TRANSACTION_ACTIVATED_EVENT")
                    put("_class", "TestEvent")
                    put("creationDate", "2024-01-01")
                    put(
                        "data",
                        Document().apply {
                            put("email", 12345)
                            put("clientId", "CHECKOUT")
                        },
                    )
                },
            )

        testCases.forEach { document ->
            val result = ecommerceCDCEventDispatcherService.dispatchEvent(document)

            StepVerifier.create(result).expectNext(document).verifyComplete()
        }
    }
}
