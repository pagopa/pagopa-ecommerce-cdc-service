package it.pagopa.ecommerce.cdc.exceptions

/**
 * Base sealed class for CDC event processing lock-related exceptions.
 *
 * @param message The exception message describing the lock failure
 * @param throwable The underlying cause of the lock failure, if any
 */
sealed class CdcEventProcessingLockException(message: String, throwable: Throwable? = null) :
    RuntimeException(message, throwable)
