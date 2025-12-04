package it.pagopa.ecommerce.cdc.liveness

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.availability.LivenessStateHealthIndicator
import org.springframework.boot.availability.ApplicationAvailability
import org.springframework.boot.availability.AvailabilityState
import org.springframework.boot.availability.LivenessState
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

@Component
class CustomLivenessIndicator(
    availability: ApplicationAvailability,
    @Value($$"${customLivenessIndicator.cdc.inactivityTimeoutMillis}") val inactivityTimeoutMillis: Long
) : LivenessStateHealthIndicator(availability) {

    companion object {
        val cdcStreamUpAndRunning = AtomicBoolean(false)

        /*
            no need for an atomic reference here, concurrency updating this field can be
            ignored.
            The target here is to check that CDC is processing events, no need for synchronized access to this variable
         */
        var lastDequeuedEventAt: Instant = Instant.MIN
    }

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun getState(applicationAvailability: ApplicationAvailability): AvailabilityState {
        val state = super.getState(applicationAvailability) as LivenessState
        if (state != LivenessState.CORRECT) {
            return state
        }

        //app state is correct, checking CDC parameters
        if (!cdcStreamUpAndRunning.get()) {
            logger.error("App is not alive, detected CDC stream to not be alive")
            return LivenessState.BROKEN
        }
        val inactivityTimeout = Duration.ofMillis(inactivityTimeoutMillis)
        if (Duration.between(lastDequeuedEventAt, Instant.now()).abs() > Duration.ofMillis(inactivityTimeoutMillis)) {
            logger.error(
                "CDC inactivity detected. Last dequeued event at: [{}], inactivity timeout: [{}]",
                lastDequeuedEventAt,
                inactivityTimeout
            )
            return LivenessState.BROKEN
        }
        return LivenessState.CORRECT
    }
}