package it.pagopa.ecommerce.cdc.services

import com.mongodb.client.result.UpdateResult
import it.pagopa.ecommerce.cdc.config.properties.RetrySendPolicyConfig
import it.pagopa.ecommerce.cdc.exceptions.CdcEventProcessingLockNotAcquiredException
import it.pagopa.ecommerce.cdc.exceptions.CdcEventTypeException
import it.pagopa.ecommerce.cdc.exceptions.CdcException
import it.pagopa.ecommerce.cdc.exceptions.CdcQueryMatchException
import it.pagopa.ecommerce.cdc.utils.EcommerceChangeStreamDocumentUtil
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
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

    companion object {
        @JvmStatic
        fun `Retriable CdcException errors method source`(): Stream<CdcException> =
            Stream.of(CdcQueryMatchException(message = "retriable error", retriableError = true))

        @JvmStatic
        fun `Not retriable CdcException errors method source`(): Stream<CdcException> =
            Stream.of(
                CdcQueryMatchException(message = "retriable error", retriableError = false),
                CdcEventTypeException(message = "not retriable error"),
                CdcEventProcessingLockNotAcquiredException(lockName = "test"),
            )
    }

    @ParameterizedTest
    @MethodSource("Retriable CdcException errors method source")
    fun `should perform retry processing event for CdcException retriable errors`(
        exception: Exception
    ) {
        val event = EcommerceChangeStreamDocumentUtil.createSampleEventStoreEvent()

        given(transactionViewUpsertService.upsertEventData(any())).willReturn {
            Mono.error(exception)
        }
        val result = ecommerceCDCEventDispatcherService.dispatchEvent(event)

        StepVerifier.create(result).expectError().verify()
        verify(transactionViewUpsertService, times(retrySendPolicyConfig.maxAttempts.toInt() + 1))
            .upsertEventData(event)
    }

    @ParameterizedTest
    @MethodSource("Retriable CdcException errors method source")
    fun `should interrupt retry for OK operation after retry`(exception: Exception) {
        val event = EcommerceChangeStreamDocumentUtil.createSampleEventStoreEvent()

        given(transactionViewUpsertService.upsertEventData(any()))
            .willReturnConsecutively(
                listOf(Mono.error(exception), mono { UpdateResult.acknowledged(0L, 0L, null) })
            )
        val result = ecommerceCDCEventDispatcherService.dispatchEvent(event)

        StepVerifier.create(result).expectNext(event).verifyComplete()
        verify(transactionViewUpsertService, times(2)).upsertEventData(event)
    }

    @ParameterizedTest
    @MethodSource("Not retriable CdcException errors method source")
    fun `should not perform retry processing event for CdcException retriable errors`(
        exception: Exception
    ) {
        val event = EcommerceChangeStreamDocumentUtil.createSampleEventStoreEvent()

        given(transactionViewUpsertService.upsertEventData(any())).willReturn {
            Mono.error(exception)
        }
        val result = ecommerceCDCEventDispatcherService.dispatchEvent(event)

        StepVerifier.create(result).expectError().verify()
        verify(transactionViewUpsertService, times(1)).upsertEventData(event)
    }
}
