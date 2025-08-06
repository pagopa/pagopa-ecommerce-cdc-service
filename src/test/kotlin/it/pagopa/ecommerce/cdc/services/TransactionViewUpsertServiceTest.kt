package it.pagopa.ecommerce.cdc.services

import com.mongodb.client.result.UpdateResult
import it.pagopa.ecommerce.cdc.exceptions.CdcQueryMatchException
import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import java.time.ZonedDateTime
import java.util.stream.Stream
import kotlin.test.assertEquals
import kotlinx.coroutines.reactor.mono
import org.bson.BsonString
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.anyString
import org.mockito.kotlin.*
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
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
            Stream<Pair<TransactionEvent<*>, TransactionStatusDto?>> {
            val baseTransaction =
                TransactionTestUtils.transactionActivated(ZonedDateTime.now().toString())
            return Stream.of(
                Pair(
                    TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.KO),
                    TransactionStatusDto.UNAUTHORIZED,
                ),
                Pair(
                    TransactionTestUtils.transactionExpiredEvent(TransactionStatusDto.ACTIVATED),
                    TransactionStatusDto.EXPIRED,
                ),
                Pair(
                    TransactionTestUtils.transactionRefundRequestedEvent(baseTransaction),
                    TransactionStatusDto.REFUND_REQUESTED,
                ),
                Pair(
                    TransactionTestUtils.transactionUserCanceledEvent(),
                    TransactionStatusDto.CANCELLATION_REQUESTED,
                ),
                Pair(
                    TransactionTestUtils.transactionClosureRequestedEvent(),
                    TransactionStatusDto.CLOSURE_REQUESTED,
                ),
                Pair(
                    TransactionTestUtils.transactionRefundErrorEvent(),
                    TransactionStatusDto.REFUND_ERROR,
                ),
                Pair(
                    TransactionTestUtils.transactionUserReceiptAddedEvent(
                        TransactionTestUtils.transactionUserReceiptData(
                            TransactionUserReceiptData.Outcome.OK
                        )
                    ),
                    TransactionStatusDto.NOTIFIED_OK,
                ),
                Pair(
                    TransactionTestUtils.transactionUserReceiptAddErrorEvent(
                        TransactionTestUtils.transactionUserReceiptData(
                            TransactionUserReceiptData.Outcome.OK
                        )
                    ),
                    TransactionStatusDto.NOTIFICATION_ERROR,
                ),
                Pair(
                    TransactionTestUtils.transactionClosureFailedEvent(
                        TransactionClosureData.Outcome.OK
                    ),
                    TransactionStatusDto.UNAUTHORIZED,
                ),
                Pair(
                    TransactionTestUtils.transactionRefundedEvent(baseTransaction),
                    TransactionStatusDto.REFUNDED,
                ),
                Pair(TransactionTestUtils.transactionRefundRetriedEvent(1), null),
                Pair(TransactionTestUtils.transactionUserReceiptAddRetriedEvent(1), null),
            )
        }
    }

    // Activated
    @Test
    fun `should perform upsert operation gathering data from transaction activated event when no transaction is present for that transactionId`() {
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any()))
            .willReturn(Mono.just(false))

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    argThat { update ->
                        val setDocument = update.updateObject[$$"$set"] as Document
                        assertEquals(TransactionStatusDto.ACTIVATED, setDocument["status"])
                        assertEquals(
                            ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                            setDocument["lastProcessedEventAt"],
                        )
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
            )
            .willReturn(Mono.just(UpdateResult.acknowledged(0L, 0L, null)))

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionId),
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
            )
            .willReturn(Mono.just(UpdateResult.acknowledged(0L, 0L, null)))

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willReturn(
                Mono.just(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            )

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1)).exists(any(), any(), anyString())

        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                any(),
                any(),
                any(),
            )
        verify(mongoTemplate, times(1)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.ACTIVATED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
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
    fun `should perform upsert operation gathering data from transaction activated event when transaction is already present for that transactionId`() {
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willReturn(Mono.just(UpdateResult.acknowledged(0L, 0L, null)))

        given(mongoTemplate.updateFirst(eq(queryByTransactionId), any(), any(), any()))
            .willReturn(Mono.just(UpdateResult.acknowledged(1L, 1L, null)))

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(0)).exists(any(), any(), anyString())

        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                any(),
                any(),
                any(),
            )
        verify(mongoTemplate, times(1)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    // Authorization Requested
    @Test
    fun `should perform upsert operation gathering data from transaction authorization requested event when transaction view is already present with lastProcessedEventAt timestamp before event creationDate `() {
        // pre-conditions
        val event = TransactionTestUtils.transactionAuthorizationRequestedEvent()
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(1L, 1L, null) } }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        TransactionStatusDto.AUTHORIZATION_REQUESTED,
                        setDocument["status"],
                    )
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(event.data.paymentGateway, setDocument["paymentGateway"])
                    assertEquals(event.data.paymentTypeCode, setDocument["paymentTypeCode"])
                    assertEquals(event.data.pspId, setDocument["pspId"])
                    assertEquals(event.data.fee, setDocument["feeTotal"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @Test
    fun `should perform upsert operation gathering data from transaction authorization requested event when is the first event processed for that transaction`() {
        // pre-conditions
        val event = TransactionTestUtils.transactionAuthorizationRequestedEvent()
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.updateFirst(eq(queryByTransactionId), any(), any(), any())).willAnswer {
            mono { UpdateResult.acknowledged(0L, 0L, null) }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any()))
            .willReturn(Mono.just(false))

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        TransactionStatusDto.AUTHORIZATION_REQUESTED,
                        setDocument["status"],
                    )
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(event.data.paymentGateway, setDocument["paymentGateway"])
                    assertEquals(event.data.paymentTypeCode, setDocument["paymentTypeCode"])
                    assertEquals(event.data.pspId, setDocument["pspId"])
                    assertEquals(event.data.fee, setDocument["feeTotal"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(1)).upsert(any(), any(), any(), any())
    }

    @Test
    fun `should perform upsert operation gathering data from transaction authorization requested event when transaction view is already present with lastProcessedEventAt timestamp after event creationDate `() {
        // pre-conditions
        val event = TransactionTestUtils.transactionAuthorizationRequestedEvent()
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.updateFirst(eq(queryByTransactionId), any(), any(), any())).willAnswer {
            mono { UpdateResult.acknowledged(1L, 1L, null) }
        }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionId),
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

        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        TransactionStatusDto.AUTHORIZATION_REQUESTED,
                        setDocument["status"],
                    )
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(event.data.paymentGateway, setDocument["paymentGateway"])
                    assertEquals(event.data.paymentTypeCode, setDocument["paymentTypeCode"])
                    assertEquals(event.data.pspId, setDocument["pspId"])
                    assertEquals(event.data.fee, setDocument["feeTotal"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    // Authorization Completed
    @ParameterizedTest
    @MethodSource("authorization completed test data method source")
    fun `should perform upsert operation gathering data from transaction authorization completed event when transaction view is already present with lastProcessedEventAt timestamp before event creationDate `(
        event: TransactionAuthorizationCompletedEvent,
        expectedAuthorizationStatus: String,
        expectedAuthorizationErrorCode: String,
    ) {
        // pre-conditions)
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(1L, 1L, null) } }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        TransactionStatusDto.AUTHORIZATION_COMPLETED,
                        setDocument["status"],
                    )
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(event.data.rrn, setDocument["rrn"])
                    assertEquals(event.data.authorizationCode, setDocument["authorizationCode"])
                    assertEquals(
                        expectedAuthorizationStatus,
                        setDocument["gatewayAuthorizationStatus"].toString(),
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

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @ParameterizedTest
    @MethodSource("authorization completed test data method source")
    fun `should perform upsert operation gathering data from transaction authorization completed event when transaction view is already present with lastProcessedEventAt timestamp after event creationDate `(
        event: TransactionAuthorizationCompletedEvent,
        expectedAuthorizationStatus: String,
        expectedAuthorizationErrorCode: String,
    ) {
        // pre-conditions)
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.updateFirst(eq(queryByTransactionId), any(), any(), any())).willAnswer {
            mono { UpdateResult.acknowledged(1L, 1L, null) }
        }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        TransactionStatusDto.AUTHORIZATION_COMPLETED,
                        setDocument["status"],
                    )
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(event.data.rrn, setDocument["rrn"])
                    assertEquals(event.data.authorizationCode, setDocument["authorizationCode"])
                    assertEquals(
                        expectedAuthorizationStatus,
                        setDocument["gatewayAuthorizationStatus"].toString(),
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

        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionId),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(event.data.rrn, setDocument["rrn"])
                    assertEquals(event.data.authorizationCode, setDocument["authorizationCode"])
                    assertEquals(
                        expectedAuthorizationStatus,
                        setDocument["gatewayAuthorizationStatus"].toString(),
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

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @ParameterizedTest
    @MethodSource("authorization completed test data method source")
    fun `should perform upsert operation gathering data from transaction authorization completed event when transaction view is not present`(
        event: TransactionAuthorizationCompletedEvent,
        expectedAuthorizationStatus: String,
        expectedAuthorizationErrorCode: String,
    ) {
        // pre-conditions)
        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.updateFirst(eq(queryByTransactionId), any(), any(), any())).willAnswer {
            mono { UpdateResult.acknowledged(0L, 0L, null) }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any()))
            .willReturn(Mono.just(false))

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        TransactionStatusDto.AUTHORIZATION_COMPLETED,
                        setDocument["status"],
                    )
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(event.data.rrn, setDocument["rrn"])
                    assertEquals(event.data.authorizationCode, setDocument["authorizationCode"])
                    assertEquals(
                        expectedAuthorizationStatus,
                        setDocument["gatewayAuthorizationStatus"].toString(),
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

        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionId),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(event.data.rrn, setDocument["rrn"])
                    assertEquals(event.data.authorizationCode, setDocument["authorizationCode"])
                    assertEquals(
                        expectedAuthorizationStatus,
                        setDocument["gatewayAuthorizationStatus"].toString(),
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

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(event.data.rrn, setDocument["rrn"])
                    assertEquals(event.data.authorizationCode, setDocument["authorizationCode"])
                    assertEquals(
                        expectedAuthorizationStatus,
                        setDocument["gatewayAuthorizationStatus"].toString(),
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

    // CLOSED
    @Test
    fun `should perform upsert operation gathering data from transaction closed event with OK Node outcome when transaction view is already present with lastProcessedEventAt timestamp before event creationDate`() {
        // pre-conditions
        val event = TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(1L, 1L, null) } }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closed canceled event with OK Node outcome when transaction view is already present with lastProcessedEventAt timestamp before event creationDate`() {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionClosedEventCanceledByUser(
                TransactionClosureData.Outcome.OK
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(1L, 1L, null) } }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CANCELED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closed event with OK Node outcome when transaction view doesn't exist`() {
        // pre-conditions
        val event = TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { false }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should perform upsert operation gathering data from transaction close canceled event with OK Node outcome when transaction view doesn't exist`() {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionClosedEventCanceledByUser(
                TransactionClosureData.Outcome.OK
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { false }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CANCELED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CANCELED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should throw CDC query exception operation processing transaction close event with OK Node outcome when transaction view exist with lastProcessedEventAt timestamp after event creationDate`() {
        // pre-conditions
        val event = TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { true }
        }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectError(CdcQueryMatchException::class.java)
            .verify()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0))
            .upsert(eq(queryByTransactionAndLastProcessedEventAtCondition), any(), any(), any())
    }

    @Test
    fun `should throw CDC query exception operation processing transaction closed canceled event with OK Node outcome when transaction view exists with lastProcessedEventAt timestamp after event creationDate`() {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionClosedEventCanceledByUser(
                TransactionClosureData.Outcome.OK
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { true }
        }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectError(CdcQueryMatchException::class.java)
            .verify()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CANCELED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    assertEquals(null, setDocument["closureErrorData"])
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0))
            .upsert(eq(queryByTransactionAndLastProcessedEventAtCondition), any(), any(), any())
    }

    // Closure Error
    @Test
    fun `should perform upsert operation gathering data from transaction closure error event when transaction view exists with lastProcessedEventAt timestamp before event creationDate`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureErrorEvent(closureErrorData)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(1L, 1L, null) } }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSURE_ERROR, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closure error event when transaction view doesn't exists`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureErrorEvent(closureErrorData)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { false }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSURE_ERROR, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSURE_ERROR, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closure error event when transaction view exists and lastProcessedEventAt after event creationDate`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureErrorEvent(closureErrorData)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { true }
        }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectError(CdcQueryMatchException::class.java)
            .verify()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.CLOSURE_ERROR, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    // Closure retried
    @Test
    fun `should perform upsert operation gathering data from transaction closure retried data when transaction view exists with lastProcessedEventAt timestamp before event creationDate`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureRetriedEvent(1, closureErrorData)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(1L, 1L, null) } }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closure retried data when transaction view doesn't exists`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureRetriedEvent(1, closureErrorData)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { false }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @Test
    fun `should perform upsert operation gathering data from transaction closure retried data when transaction view exists and lastProcessedEventAt after event creationDate`() {
        // pre-conditions
        val closureErrorData =
            ClosureErrorData(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Generic error",
                ClosureErrorData.ErrorType.COMMUNICATION_ERROR,
            )
        val event = TransactionTestUtils.transactionClosureRetriedEvent(1, closureErrorData)

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { true }
        }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectError(CdcQueryMatchException::class.java)
            .verify()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(closureErrorData, setDocument["closureErrorData"])
                    assertEquals(
                        TransactionUserReceiptData.Outcome.NOT_RECEIVED.toString(),
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @ParameterizedTest
    @ValueSource(strings = ["OK", "KO"])
    fun `should perform upsert operation gathering data from transaction add user receipt requested event when transaction view is already present with lastProcessedEventAt timestamp before event creationDate `(
        sendPaymentResultOutcome: String
    ) {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionUserReceiptRequestedEvent(
                TransactionTestUtils.transactionUserReceiptData(
                    TransactionUserReceiptData.Outcome.valueOf(sendPaymentResultOutcome)
                )
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(1L, 1L, null) } }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.NOTIFICATION_REQUESTED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        sendPaymentResultOutcome,
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())

        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @ParameterizedTest
    @ValueSource(strings = ["OK", "KO"])
    fun `should perform upsert operation gathering data from transaction add user receipt requested event when is the first event processed for that transaction`(
        sendPaymentResultOutcome: String
    ) {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionUserReceiptRequestedEvent(
                TransactionTestUtils.transactionUserReceiptData(
                    TransactionUserReceiptData.Outcome.valueOf(sendPaymentResultOutcome)
                )
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.updateFirst(eq(queryByTransactionId), any(), any(), any())).willAnswer {
            mono { UpdateResult.acknowledged(0L, 0L, null) }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any()))
            .willReturn(Mono.just(false))

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.NOTIFICATION_REQUESTED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        sendPaymentResultOutcome,
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionId),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        sendPaymentResultOutcome,
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.NOTIFICATION_REQUESTED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        sendPaymentResultOutcome,
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }

    @ParameterizedTest
    @ValueSource(strings = ["OK", "KO"])
    fun `should perform upsert operation gathering data from transaction add user receipt requested event when transaction view is already present with lastProcessedEventAt timestamp after event creationDate `(
        sendPaymentResultOutcome: String
    ) {
        // pre-conditions
        val event =
            TransactionTestUtils.transactionUserReceiptRequestedEvent(
                TransactionTestUtils.transactionUserReceiptData(
                    TransactionUserReceiptData.Outcome.valueOf(sendPaymentResultOutcome)
                )
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
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.updateFirst(eq(queryByTransactionId), any(), any(), any())).willAnswer {
            mono { UpdateResult.acknowledged(1L, 1L, null) }
        }

        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(1L, 1L, null))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionId),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(
                        sendPaymentResultOutcome,
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )

        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(TransactionStatusDto.NOTIFICATION_REQUESTED, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    assertEquals(
                        sendPaymentResultOutcome,
                        setDocument["sendPaymentResultOutcome"].toString(),
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
        verify(mongoTemplate, times(0)).exists(eq(queryByTransactionId), any(), any())

        verify(mongoTemplate, times(0)).upsert(any(), any(), any(), any())
    }

    @ParameterizedTest
    @MethodSource("no upsert done for events that does not update view method source")
    fun `should create view when it doesn't exists yet`(
        pair: Pair<TransactionEvent<*>, TransactionStatusDto?>
    ) {
        val event = pair.first
        val status = pair.second

        val queryByTransactionId =
            Query.query(Criteria.where("transactionId").`is`(event.transactionId))

        val queryByTransactionAndLastProcessedEventAtCondition =
            Query.query(
                Criteria.where("transactionId")
                    .`is`(event.transactionId)
                    .orOperator(
                        Criteria.where("lastProcessedEventAt").exists(false),
                        Criteria.where("lastProcessedEventAt")
                            .lt(ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()),
                    )
            )

        given(
                mongoTemplate.updateFirst(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer { mono { UpdateResult.acknowledged(0L, 0L, null) } }

        given(mongoTemplate.exists(eq(queryByTransactionId), any(), any())).willAnswer {
            mono { false }
        }

        given(
                mongoTemplate.upsert(
                    eq(queryByTransactionAndLastProcessedEventAtCondition),
                    any(),
                    any(),
                    any(),
                )
            )
            .willAnswer {
                mono { UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)) }
            }

        // pre-conditions
        given(mongoTemplate.upsert(any(), any(), any(), any()))
            .willReturn(
                Mono.just(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            )
        // test
        StepVerifier.create(transactionViewUpsertService.upsertEventData(event))
            .expectNext(UpdateResult.acknowledged(0L, 0L, BsonString(event.transactionId)))
            .verifyComplete()

        // verifications
        verify(mongoTemplate, times(1))
            .updateFirst(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    assertEquals(status, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    true
                },
                any(),
                any(),
            )
        verify(mongoTemplate, times(0)).updateFirst(eq(queryByTransactionId), any(), any(), any())
        verify(mongoTemplate, times(1))
            .exists(
                eq(queryByTransactionId),
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
        verify(mongoTemplate, times(1))
            .upsert(
                eq(queryByTransactionAndLastProcessedEventAtCondition),
                argThat { update ->
                    val setDocument = update.updateObject[$$"$set"] as Document
                    if (status != null) assertEquals(status, setDocument["status"])
                    assertEquals(
                        ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli(),
                        setDocument["lastProcessedEventAt"],
                    )
                    true
                },
                eq(BaseTransactionView::class.java),
                eq(collectionName),
            )
    }
}
