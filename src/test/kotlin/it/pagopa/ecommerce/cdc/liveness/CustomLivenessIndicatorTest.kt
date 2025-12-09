package it.pagopa.ecommerce.cdc.liveness

import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import org.springframework.boot.availability.ApplicationAvailability
import org.springframework.boot.availability.LivenessState

class CustomLivenessIndicatorTest {

    private val applicationAvailability: ApplicationAvailability = mock()

    private val inactivityTimeoutSeconds = 10L

    private val customLivenessIndicator =
        CustomLivenessIndicator(applicationAvailability, inactivityTimeoutSeconds)

    @Test
    fun `Should propagate error status from superclass`() {
        // pre-condition
        given(applicationAvailability.livenessState).willReturn(LivenessState.BROKEN)
        // test
        val state = customLivenessIndicator.getState(applicationAvailability)
        // assertions
        assertEquals(LivenessState.BROKEN, state)
    }

    @Test
    fun `Should return broken status for CDC not up and running detected`() {
        // pre-condition
        given(applicationAvailability.livenessState).willReturn(LivenessState.CORRECT)
        CustomLivenessIndicator.cdcStreamUpAndRunning.set(false)
        // test
        val state = customLivenessIndicator.getState(applicationAvailability)
        // assertions
        assertEquals(LivenessState.BROKEN, state)
    }

    @Test
    fun `Should return broken status for inactive CDC detected (no event processed inside threshold)`() {
        // pre-condition
        given(applicationAvailability.livenessState).willReturn(LivenessState.CORRECT)
        CustomLivenessIndicator.cdcStreamUpAndRunning.set(true)
        CustomLivenessIndicator.lastDequeuedEventAt =
            Instant.now() - Duration.ofSeconds(inactivityTimeoutSeconds + 1)
        // test
        val state = customLivenessIndicator.getState(applicationAvailability)
        // assertions
        assertEquals(LivenessState.BROKEN, state)
    }

    @Test
    fun `Should skip inactive CDC detected status for negative inactivity timeout parameter (no event processed inside threshold)`() {
        // pre-condition
        val customLivenessIndicator = CustomLivenessIndicator(applicationAvailability, -1L)
        given(applicationAvailability.livenessState).willReturn(LivenessState.CORRECT)
        CustomLivenessIndicator.cdcStreamUpAndRunning.set(true)
        CustomLivenessIndicator.lastDequeuedEventAt =
            Instant.now() - Duration.ofSeconds(inactivityTimeoutSeconds + 1)
        // test
        val state = customLivenessIndicator.getState(applicationAvailability)
        // assertions
        assertEquals(LivenessState.CORRECT, state)
    }

    @Test
    fun `Should return correct status for for healthy app`() {
        // pre-condition
        given(applicationAvailability.livenessState).willReturn(LivenessState.CORRECT)
        CustomLivenessIndicator.cdcStreamUpAndRunning.set(true)
        CustomLivenessIndicator.lastDequeuedEventAt =
            Instant.now() - Duration.ofSeconds(inactivityTimeoutSeconds - 1)
        // test
        val state = customLivenessIndicator.getState(applicationAvailability)
        // assertions
        assertEquals(LivenessState.CORRECT, state)
    }
}
