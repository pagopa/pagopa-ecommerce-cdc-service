package it.pagopa.ecommerce.cdc.exceptions

/**
 * Exception thrown when a CDC event processing lock cannot be acquired.
 *
 * @param lockName The name/key of the lock that could not be acquired
 * @param throwable The underlying cause of the lock acquisition failure
 */
class CdcEventProcessingLockNotAcquiredException(lockName: String, throwable: Throwable? = null) :
    CdcEventProcessingLockException("Could not acquire the lock [${lockName}]", throwable)
