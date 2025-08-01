package it.pagopa.ecommerce.cdc.exceptions

class CdcQueryMatchException(message: String, throwable: Throwable? = null) :
    RuntimeException(message, throwable)
