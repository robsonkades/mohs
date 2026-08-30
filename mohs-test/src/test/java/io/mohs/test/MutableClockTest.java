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
package io.mohs.test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MutableClockTest {

    @Test
    void startsAtTheGivenInstant() {
        Instant start = Instant.parse("2026-08-13T00:00:00Z");

        MutableClock clock = MutableClock.startingAt(start);

        assertThat(clock.instant()).isEqualTo(start);
    }

    @Test
    void setToMovesTheClock() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-13T00:00:00Z"));
        Instant target = Instant.parse("2030-01-01T00:00:00Z");

        clock.setTo(target);

        assertThat(clock.instant()).isEqualTo(target);
    }

    @Test
    void advanceMovesTheClockForward() {
        Instant start = Instant.parse("2026-08-13T00:00:00Z");
        MutableClock clock = MutableClock.startingAt(start);

        clock.advance(Duration.ofMinutes(30));

        assertThat(clock.instant()).isEqualTo(start.plus(Duration.ofMinutes(30)));
    }

    @Test
    void withZoneKeepsTheSameInstantUnderADifferentZone() {
        Instant start = Instant.parse("2026-08-13T00:00:00Z");
        MutableClock clock = new MutableClock(start, ZoneId.of("UTC"));

        var saoPaulo = clock.withZone(ZoneId.of("America/Sao_Paulo"));

        assertThat(saoPaulo.getZone()).isEqualTo(ZoneId.of("America/Sao_Paulo"));
        assertThat(saoPaulo.instant()).isEqualTo(start);
    }

    /**
     * Regression guard: {@code withZone} returns a VIEW, not a snapshot.
     *
     * <p>An earlier version copied the instant, so the derived clock was born frozen. Because this
     * kit is published, anyone using it to test their own handler would get a test that passes —
     * or fails — for the wrong reason, with no signal at all. The test above misses it precisely
     * because it never advances the clock after calling {@code withZone}.
     */
    @Test
    void withZoneFollowsLaterAdvancesOnTheSourceClock() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-13T00:00:00Z"));
        var saoPaulo = clock.withZone(ZoneId.of("America/Sao_Paulo"));

        clock.advance(Duration.ofHours(2));

        assertThat(saoPaulo.instant()).isEqualTo(clock.instant());
    }
}
