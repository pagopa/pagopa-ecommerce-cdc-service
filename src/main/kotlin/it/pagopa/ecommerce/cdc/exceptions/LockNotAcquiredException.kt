package it.pagopa.ecommerce.cdc.exceptions

class LockNotAcquiredException(lockName: String, throwable: Throwable? = null) :
    LockException("Could not acquire the lock [${lockName}]", throwable)
