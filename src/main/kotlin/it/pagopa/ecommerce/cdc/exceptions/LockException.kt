package it.pagopa.ecommerce.cdc.exceptions

sealed class LockException(message: String, throwable: Throwable? = null) :
    RuntimeException(message, throwable)
