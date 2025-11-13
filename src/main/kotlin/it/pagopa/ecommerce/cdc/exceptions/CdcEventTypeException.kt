package it.pagopa.ecommerce.cdc.exceptions

/**
 * Exception thrown when an unsupported or unrecognized transaction event type is encountered.
 *
 * This exception occurs during CDC event processing when the service receives a transaction event
 * that doesn't match any of the known event types handled by the TransactionViewUpsertService.
 *
 * @param message Details about the unhandled event type
 * @param throwable The underlying cause, if any
 */
class CdcEventTypeException(message: String, throwable: Throwable? = null) :
    CdcException(message = message, throwable = throwable, retriableError = false)
