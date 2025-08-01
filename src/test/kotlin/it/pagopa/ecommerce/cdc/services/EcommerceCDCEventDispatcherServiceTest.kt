package it.pagopa.ecommerce.cdc.services

import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.mock
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class EcommerceCDCEventDispatcherServiceTest {

    private val retrySendPolicyConfig = RetrySendPolicyConfig(maxAttempts = 3, intervalInMs = 100)
    private lateinit var ecommerceCDCEventDispatcherService: EcommerceCDCEventDispatcherService
    private val transactionViewUpsertService: TransactionViewUpsertService = mock()

    @BeforeEach
    fun setup() {
        ecommerceCDCEventDispatcherService =
            EcommerceCDCEventDispatcherService(retrySendPolicyConfig, transactionViewUpsertService)
    }

    @Test
    fun `should successfully dispatch and process transaction activated event`() {
        val event = TransactionTestUtils.transactionActivateEvent()
        StepVerifier.create(ecommerceCDCEventDispatcherService.dispatchEvent(event))
            .expectNext(event)
            .verifyComplete()
    }

    @Test
    fun `should handle different transaction statuses`() {
        val events =
            listOf(
                TransactionTestUtils.transactionActivateEvent(),
                TransactionTestUtils.transactionExpiredEvent(TransactionStatusDto.ACTIVATED),
                TransactionTestUtils.transactionUserCanceledEvent(),
                TransactionTestUtils.transactionClosureRequestedEvent(),
            )

        events.forEach { event ->
            StepVerifier.create(ecommerceCDCEventDispatcherService.dispatchEvent(event))
                .expectNext(event)
                .verifyComplete()
        }
    }
}
