package it.pagopa.ecommerce.cdc.exceptions

/**
 * Exception thrown when a CDC database query operation fails to match expected conditions.
 *
 * @param message Details about the query matching failure
 * @param throwable The underlying cause, if any
 */
class CdcQueryMatchException(message: String, throwable: Throwable? = null) :
    RuntimeException(message, throwable)
