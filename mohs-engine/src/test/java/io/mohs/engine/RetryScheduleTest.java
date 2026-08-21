package io.mohs.engine;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryScheduleTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    @Test
    void exhaustsTheBudgetWhenTheFailedAttemptExceedsRetries() {
        assertThat(RetrySchedule.nextRetryAt(1, 0, NOW)).isEmpty();
        assertThat(RetrySchedule.nextRetryAt(3, 2, NOW)).isEmpty();
    }

    /** Full jitter: o delay é uniforme em [0, 1s × 2^(tentativa−1)] — só os limites são determinísticos, e são eles o contrato. */
    @Test
    void staysWithinTheExponentialBoundForEachAttempt() {
        for (int sample = 0; sample < 100; sample++) {
            assertThat(RetrySchedule.nextRetryAt(1, 5, NOW)).hasValueSatisfying(retryAt ->
                    assertThat(retryAt).isBetween(NOW, NOW.plusSeconds(1)));
            assertThat(RetrySchedule.nextRetryAt(2, 5, NOW)).hasValueSatisfying(retryAt ->
                    assertThat(retryAt).isBetween(NOW, NOW.plusSeconds(2)));
            assertThat(RetrySchedule.nextRetryAt(3, 5, NOW)).hasValueSatisfying(retryAt ->
                    assertThat(retryAt).isBetween(NOW, NOW.plusSeconds(4)));
        }
    }

    /** Teto de 10min — inclusive para números de tentativa que estourariam o shift sem o clamp de expoente. */
    @Test
    void capsTheBoundAtTenMinutesEvenForHugeAttemptNumbers() {
        for (int sample = 0; sample < 100; sample++) {
            Optional<Instant> retryAt = RetrySchedule.nextRetryAt(40, 100, NOW);
            assertThat(retryAt).hasValueSatisfying(at ->
                    assertThat(at).isBetween(NOW, NOW.plusSeconds(600)));
        }
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> RetrySchedule.nextRetryAt(0, 3, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failedAttempt");
        assertThatThrownBy(() -> RetrySchedule.nextRetryAt(1, -1, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retries");
    }
}
