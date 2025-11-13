package it.pagopa.ecommerce.cdc.exceptions

/**
 * Exception thrown when a CDC database query operation fails to match expected conditions.
 *
 * @param message Details about the query matching failure
 * @param throwable The underlying cause, if any
 */
class CdcQueryMatchException(
    message: String,
    throwable: Throwable? = null,
    override val retriableError: Boolean,
) : CdcException(message = message, throwable = throwable, retriableError = retriableError)
