/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    /** Full jitter: the delay is uniform in [0, 1s x 2^(attempt-1)] — only the bounds are deterministic, and they are the contract. */
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

    /** A 10-minute ceiling — including for attempt numbers that would overflow the shift without the exponent clamp. */
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
