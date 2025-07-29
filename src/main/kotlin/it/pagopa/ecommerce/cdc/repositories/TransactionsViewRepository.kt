package it.pagopa.ecommerce.cdc.repositories

import it.pagopa.ecommerce.commons.documents.BaseTransactionView
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono

@Repository
interface TransactionsViewRepository : ReactiveCrudRepository<BaseTransactionView, String> {
    fun findByTransactionId(transactionId: String): Mono<BaseTransactionView>
}
