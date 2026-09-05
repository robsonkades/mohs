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
package io.mohs.core.resource;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RateLimitTest {

    @Test
    void holdsNameMaxAndWindow() {
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        assertThat(rateLimit.name()).isEqualTo("smtp");
        assertThat(rateLimit.max()).isEqualTo(100);
        assertThat(rateLimit.window()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void rejectsBlankName() {
        assertThatThrownBy(() -> new RateLimit(" ", 100, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveMax() {
        assertThatThrownBy(() -> new RateLimit("smtp", 0, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveWindow() {
        assertThatThrownBy(() -> new RateLimit("smtp", 100, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The bucket yields one token every {@code window / max}. If that division truncates to zero,
     * the refill would divide by {@code Duration.ZERO} and bring down the ENTIRE claim round —
     * including jobs with no limit at all. The spec refuses to be created in that state, so neither
     * a property, nor a {@code @Bean}, nor a PATCH can produce it.
     */
    @Test
    void rejectsAWindowTooShortToIssueOneTokenPerInterval() {
        assertThatThrownBy(() -> new RateLimit("smtp", 2_000_000, Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short for max");
    }

    /** The exact limit — one token per nanosecond — remains valid: the rejection is of what is not representable, not of what is aggressive. */
    @Test
    void acceptsAWindowThatIssuesExactlyOneTokenPerNanosecond() {
        assertThat(new RateLimit("smtp", 1_000_000, Duration.ofMillis(1)).max()).isEqualTo(1_000_000);
    }
}
