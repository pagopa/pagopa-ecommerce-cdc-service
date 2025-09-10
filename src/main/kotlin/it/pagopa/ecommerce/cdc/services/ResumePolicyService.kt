package it.pagopa.ecommerce.cdc.services

import java.time.Instant
import reactor.core.publisher.Mono

interface ResumePolicyService {
    fun getResumeTimestamp(): Mono<Instant>

    fun saveResumeTimestamp(timestamp: Instant): Mono<Boolean>
}
