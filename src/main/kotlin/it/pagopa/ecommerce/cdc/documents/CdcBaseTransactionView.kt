package it.pagopa.ecommerce.cdc.documents

import lombok.Data
import lombok.NoArgsConstructor
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

/**
 * Base transaction view version agnostic class. TODO to be removed when the cloned
 * transactions-view is dismissed (the BaseTransactionView from commons library should be used)
 */
@Data
@NoArgsConstructor
@Document(collection = "cdc-transactions-view")
abstract class BaseTransactionView
/**
 * All-args constructor
 *
 * @param transactionId the transaction id
 */
protected constructor(@field:Id private var transactionId: String?)
