package it.pagopa.ecommerce.cdc.services

import com.mongodb.client.result.UpdateResult
import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class EcommerceCDCEventDispatcherServiceTest {

    private val retrySendPolicyConfig = RetrySendPolicyConfig(maxAttempts = 3, intervalInMs = 100)
    private val transactionViewUpsertService: TransactionViewUpsertService = mock()
    private val ecommerceCDCEventDispatcherService: EcommerceCDCEventDispatcherService =
        EcommerceCDCEventDispatcherService(retrySendPolicyConfig, transactionViewUpsertService)

    @Test
    fun `should successfully dispatch and process transaction event`() {
        val event = EcommerceChangeStreamDocumentUtil.createSampleEventStoreEvent()

        given(transactionViewUpsertService.upsertEventData(any())).willReturn {
            mono { UpdateResult.acknowledged(0L, 0L, null) }
        }
        val result = ecommerceCDCEventDispatcherService.dispatchEvent(event)

        StepVerifier.create(result).expectNext(event).verifyComplete()
        verify(transactionViewUpsertService, times(1)).upsertEventData(event)
    }
}
