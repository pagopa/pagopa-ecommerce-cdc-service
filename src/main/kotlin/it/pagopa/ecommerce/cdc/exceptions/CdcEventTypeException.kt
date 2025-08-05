package it.pagopa.ecommerce.cdc.exceptions

class CdcEventTypeException(message: String, throwable: Throwable? = null) :
    RuntimeException(message, throwable)
