package it.pagopa.ecommerce.cdc.exceptions

abstract class CdcException(
    open val retriableError: Boolean,
    message: String,
    throwable: Throwable? = null,
) : Exception("$message - retriable error: [$retriableError]", throwable) {}
