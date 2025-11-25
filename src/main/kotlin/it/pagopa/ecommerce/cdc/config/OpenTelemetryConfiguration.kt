package it.pagopa.ecommerce.cdc.config

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenTelemetryConfiguration {

    @Bean
    fun openTelemetry(): OpenTelemetry = GlobalOpenTelemetry.get()

    @Bean
    fun openTelemetryTracer(openTelemetry: OpenTelemetry): Tracer =
        openTelemetry.getTracer("pagopa-ecommerce-cdc-service")

    @Bean
    fun openTelemetryUtils(openTelemetry: OpenTelemetry, tracer: Tracer): OpenTelemetryUtils =
        OpenTelemetryUtils(tracer)
}
