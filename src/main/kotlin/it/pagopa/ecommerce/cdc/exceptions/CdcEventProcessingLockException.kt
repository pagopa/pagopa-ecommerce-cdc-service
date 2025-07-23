package it.pagopa.ecommerce.cdc.exceptions

sealed class CdcEventProcessingLockException(message: String, throwable: Throwable? = null) :
    RuntimeException(message, throwable)
