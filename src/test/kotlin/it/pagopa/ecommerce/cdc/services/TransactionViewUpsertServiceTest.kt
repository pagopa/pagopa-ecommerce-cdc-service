package it.pagopa.ecommerce.cdc.services

import com.mongodb.client.result.UpdateResult
import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import org.bson.BsonString
import org.bson.BsonValue
import java.time.ZonedDateTime
import java.util.stream.Stream
import kotlin.test.assertEquals
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.*
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.updateFirst
import org.springframework.http.HttpStatus
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class TransactionViewUpsertServiceTest {

    private val mongoTemplate: ReactiveMongoTemplate = mock()
    private val collectionName = "collectionName"

    private val transactionViewUpsertService =
        TransactionViewUpsertService(
            mongoTemplate = mongoTemplate,
            transactionViewName = collectionName,
        )

    companion object {
        @JvmStatic
        fun `authorization completed test data method source`(): Stream<Arguments> =
            Stream.of(
                Arguments.of(
                    TransactionTestUtils.transactionAuthorizationCompletedEvent(
                        TransactionTestUtils.npgTransactionGatewayAuthorizationData(
                            OperationResultDto.FAILED,
                            "npg failed",
                        )
                    ),
                    OperationResultDto.FAILED.toString(),
                    "npg failed",
                ),
                Arguments.of(
                    TransactionTestUtils.transactionAuthorizationCompletedEvent(
                        TransactionTestUtils.redirectTransactionGatewayAuthorizationData(
                            RedirectTransactionGatewayAuthorizationData.Outcome.KO,
                            "redirect failed",
                        )
                    ),
                    RedirectTransactionGatewayAuthorizationData.Outcome.KO.toString(),
                    "redirect failed",
                ),
            )

        @JvmStatic
        fun `no upsert done for events that does not update view method source`():
            Stream<TransactionEvent<*>> {
            val baseTransaction =
                TransactionTestUtils.transactionActivated(ZonedDateTime.now().toString())
            return Stream.of(
                TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.KO),
                TransactionTestUtils.transactionExpiredEvent(TransactionStatusDto.ACTIVATED),
                TransactionTestUtils.transactionRefundRequestedEvent(baseTransaction),
                TransactionTestUtils.transactionUserCanceledEvent(),
                TransactionTestUtils.transactionClosureRequestedEvent(),
                TransactionTestUtils.transactionRefundErrorEvent(),
                TransactionTestUtils.transactionUserReceiptAddedEvent(
                    TransactionTestUtils.transactionUserReceiptData(
                        TransactionUserReceiptData.Outcome.OK
                    )
                ),
                TransactionTestUtils.transactionUserReceiptAddErrorEvent(
                    TransactionTestUtils.transactionUserReceiptData(
                        TransactionUserReceiptData.Outcome.OK
                    )
                ),
                TransactionTestUtils.transactionClosureFailedEvent(
                    TransactionClosureData.Outcome.OK
                ),
                TransactionTestUtils.transactionRefundedEvent(baseTransaction),
                TransactionTestUtils.transactionRefundRetriedEvent(1),
                TransactionTestUtils.transactionUserReceiptAddRetriedEvent(1),
            )
        }
    }

    @Test
    fun `should perform upsert operation gathering data from transaction activated event`() {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionActivateEvent(
                TransactionTestUtils.npgTransactionGatewayActivationData()
            )
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(
                                ZonedDateTime.parse(event.creationDate)
                                    .toInstant()
                                    .toEpochMilli()
                            ),
                    )
            )

        given(mongoTemplate.updateFirst(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(0L, 0L, null)))

        given(mongoTemplate.upsert(eq(queryByTransactionAndLastProcessedEventAtCondition), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(0L, 0L, null)))

        given(mongoTemplate.upsert(eq(queryByTransactionId), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId))))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(2))
            .upsert(
                argThat { query ->
                    assertEquals(
                        event.transactionId,
                        query.queryObject.filter { it.key == "transactionId" }.map { it.value }[0],
                    )
                    true
                },
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(event.data.email.opaqueData, setDocument["email"])
                    assertEquals(event.data.paymentNotices, setDocument["paymentNotices"])
                    assertEquals(event.data.clientId, setDocument["clientId"])
                    assertEquals(event.creationDate, setDocument["creationDate"])
                    assertEquals(Transaction::class.java.canonicalName, setDocument["_class"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should perform upsert operation gathering data from transaction authorization requested event`() {
        // pre-conditions
        val event = TransactionTestUtils.transactionAuthorizationRequestedEvent()
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .upsert(
                argThat { query ->
                    assertEquals(
                        event.transactionId,
                        query.queryObject.filter { it.key == "transactionId" }.map { it.value }[0],
                    )
                    true
                },
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(event.data.paymentGateway, setDocument["paymentGateway"])
                    assertEquals(event.data.paymentTypeCode, setDocument["paymentTypeCode"])
                    assertEquals(event.data.pspId, setDocument["pspId"])
                    assertEquals(event.data.fee, setDocument["feeTotal"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @ParameterizedTest
    @MethodSource("authorization completed test data method source")
    fun `should perform upsert operation gathering data from transaction authorization completed event`(
        event: TransactionAuthorizationCompletedEvent,
        expectedAuthorizationStatus: String,
        expectedAuthorizationErrorCode: String,
    ) {
        // pre-conditions)
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .upsert(
                argThat { query ->
                    assertEquals(
                        event.transactionId,
                        query.queryObject.filter { it.key == "transactionId" }.map { it.value }[0],
                    )
                    true
                },
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(event.data.rrn, setDocument["rrn"])
                    assertEquals(event.data.authorizationCode, setDocument["authorizationCode"])
                    assertEquals(
                        expectedAuthorizationStatus,
                        setDocument["gatewayAuthorizationStatus"],
                    )
                    assertEquals(
                        expectedAuthorizationErrorCode,
                        setDocument["authorizationErrorCode"],
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closed event with OK Node outcome`() {
        // pre-conditions
        val event = TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .upsert(
                argThat { query ->
                    assertEquals(
                        event.transactionId,
                        query.queryObject.filter { it.key == "transactionId" }.map { it.value }[0],
                    )
                    true
                },
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"],
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closure error event`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureErrorEvent(closureErrorData)
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .upsert(
                argThat { query ->
                    assertEquals(
                        event.transactionId,
                        query.queryObject.filter { it.key == "transactionId" }.map { it.value }[0],
                    )
                    true
                },
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"],
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closure retried data`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureRetriedEvent(1, closureErrorData)
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .upsert(
                argThat { query ->
                    assertEquals(
                        event.transactionId,
                        query.queryObject.filter { it.key == "transactionId" }.map { it.value }[0],
                    )
                    true
                },
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"],
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @ParameterizedTest
    @ValueSource(strings = ["OK", "KO"])
    fun `should perform upsert operation gathering data from transaction add user receipt requested event data`(
        sendPaymentResultOutcome: String
    ) {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionUserReceiptRequestedEvent(
                TransactionTestUtils.transactionUserReceiptData(
                    TransactionUserReceiptData.Outcome.valueOf(sendPaymentResultOutcome)
                )
            )
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .upsert(
                argThat { query ->
                    assertEquals(
                        event.transactionId,
                        query.queryObject.filter { it.key == "transactionId" }.map { it.value }[0],
                    )
                    true
                },
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(sendPaymentResultOutcome, setDocument["sendPaymentResultOutcome"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @ParameterizedTest
    @MethodSource("no upsert done for events that does not update view method source")
    fun `should not perform upsert operation for events that does not update view data section`(
        event: TransactionEvent<*>
    ) {
        // pre-conditions
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(Unit)
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }
}
