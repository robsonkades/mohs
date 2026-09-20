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

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.mohs.core.definition.JobDefinition;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The armed-trigger horizon and the sleep cap it feeds — the two halves deliberately separated: one
 * decides WHICH trigger counts, the other does the arithmetic over it.
 *
 * <p>Pure functions: the defect they fix (the backoff sleeping past a known {@code next_fire_at}),
 * the risk they introduce (redefining the tick's cadence) and the silent failure mode (waking for a
 * trigger nobody fires) are all decidable without a wall clock.
 */
class EngineSleepTest {

    private static final Instant NOW = Instant.parse("2026-08-29T06:00:00Z");

    record Handler() {
    }

    /**
     * The cap's arithmetic: {@code min(backoff, time-to-trigger)}, never below the
     * {@code poll-interval} floor.
     */
    @Nested
    class CappedByNextFire {

        private static final Duration BACKOFF = Duration.ofSeconds(2);
        private static final Duration FLOOR = Duration.ofMillis(25);

        @Test
        void noArmedTriggerLeavesTheBackoffUntouched() {
            assertThat(Engine.cappedByNextFire(BACKOFF, null, NOW, FLOOR)).isEqualTo(BACKOFF);
        }

        @Test
        void triggerBeyondTheBackoffLeavesItUntouched() {
            assertThat(Engine.cappedByNextFire(BACKOFF, NOW.plusSeconds(30), NOW, FLOOR)).isEqualTo(BACKOFF);
        }

        /** The original defect: an occurrence due in 1s waiting out the 2s backoff. */
        @Test
        void triggerBeforeTheBackoffShortensTheSleepToIt() {
            assertThat(Engine.cappedByNextFire(BACKOFF, NOW.plusSeconds(1), NOW, FLOOR))
                    .isEqualTo(Duration.ofSeconds(1));
        }

        /**
         * The floor is {@code poll-interval}, not a symbolic minimum: what determines the tick's
         * cadence remains configuration, not the schedule's density. A trigger that is due, overdue or
         * microseconds away sleeps the poll — never {@code await(0)} in a loop against the database.
         */
        @Test
        void triggerInsideThePollIntervalSleepsThePollInterval() {
            assertThat(Engine.cappedByNextFire(BACKOFF, NOW, NOW, FLOOR)).isEqualTo(FLOOR);
            assertThat(Engine.cappedByNextFire(BACKOFF, NOW.minusSeconds(5), NOW, FLOOR)).isEqualTo(FLOOR);
            assertThat(Engine.cappedByNextFire(BACKOFF, NOW.plusNanos(1), NOW, FLOOR)).isEqualTo(FLOOR);
            assertThat(Engine.cappedByNextFire(BACKOFF, NOW.plusMillis(10), NOW, FLOOR)).isEqualTo(FLOOR);
        }

        /** Floor equal to ceiling (backoff off, the classic shape) is not shortenable by any trigger. */
        @Test
        void aFixedCadenceIsNotShortenedByATrigger() {
            Duration fixed = Duration.ofSeconds(3);
            assertThat(Engine.cappedByNextFire(fixed, NOW.plusSeconds(1), NOW, fixed)).isEqualTo(fixed);
        }
    }

}
