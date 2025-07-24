package it.pagopa.ecommerce.cdc.exceptions

class CdcEventProcessingLockNotAcquiredException(lockName: String, throwable: Throwable? = null) :
    CdcEventProcessingLockException("Could not acquire the lock [${lockName}]", throwable)
