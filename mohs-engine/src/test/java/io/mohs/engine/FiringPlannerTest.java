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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FiringPlannerTest {

    private static final Duration THRESHOLD = Duration.ofSeconds(60);
    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    private final FiringPlanner planner = new FiringPlanner(new NextFireCalculator(), THRESHOLD);

    @Nested
    class FixedRate {

        private final IntervalSpec everyTenSeconds = new IntervalSpec(Duration.ofSeconds(10), false);

        @Test
        void dueWithinThresholdFiresAllOccurrencesLateWithoutMisfire() {
            // Due 25s ago: 3 occurrences (25s, 15s and 5s back), all within the threshold
            Instant nextFire = NOW.minusSeconds(25);

            FiringPlanner.Plan plan = planner.plan(everyTenSeconds, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isFalse();
            assertThat(plan.occurrences()).containsExactly(
                    NOW.minusSeconds(25), NOW.minusSeconds(15), NOW.minusSeconds(5));
            assertThat(plan.nextFireAt()).isEqualTo(NOW.plusSeconds(5));
        }

        @Test
        void ignoreDropsMissedButKeepsTheSeriesAnchor() {
            // Due 10 minutes ago on a 1-minute schedule: the missed ones are discarded, but the recent
            // ones and the next firing stay on the original series (:00 of the minute)
            IntervalSpec everyMinute = new IntervalSpec(Duration.ofMinutes(1), false);
            Instant nextFire = NOW.minus(Duration.ofMinutes(10));

            FiringPlanner.Plan plan = planner.plan(everyMinute, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isTrue();
            assertThat(plan.occurrences()).containsExactly(NOW.minusSeconds(60), NOW);
            assertThat(plan.nextFireAt()).isEqualTo(NOW.plusSeconds(60));
        }

        @Test
        void fireNowCompensatesTheMissedBatchWithASingleImmediateFire() {
            IntervalSpec everyMinute = new IntervalSpec(Duration.ofMinutes(1), false);
            Instant nextFire = NOW.minus(Duration.ofMinutes(10)).minusSeconds(30); // the series sits at :30

            FiringPlanner.Plan plan = planner.plan(everyMinute, Misfire.FIRE_NOW, nextFire, NOW);

            assertThat(plan.misfired()).isTrue();
            // The recent one (30s back) fires normally; [now] compensates for the 10 missed
            assertThat(plan.occurrences()).containsExactly(NOW.minusSeconds(30), NOW);
            assertThat(plan.nextFireAt()).isEqualTo(NOW.plusSeconds(30));
        }

        @Test
        void fireAllMissedReplaysEveryOccurrence() {
            IntervalSpec everyMinute = new IntervalSpec(Duration.ofMinutes(1), false);
            Instant nextFire = NOW.minus(Duration.ofMinutes(5));

            FiringPlanner.Plan plan = planner.plan(everyMinute, Misfire.FIRE_ALL_MISSED, nextFire, NOW);

            assertThat(plan.misfired()).isTrue();
            assertThat(plan.occurrences()).containsExactly(
                    NOW.minusSeconds(300), NOW.minusSeconds(240), NOW.minusSeconds(180),
                    NOW.minusSeconds(120), NOW.minusSeconds(60), NOW);
            assertThat(plan.nextFireAt()).isEqualTo(NOW.plusSeconds(60));
        }

        @Test
        void fireAllMissedCapsAtTheCycleCeilingAndStaysDue() {
            // 2 hours of missed occurrences on a 1-second schedule: only 1,440 per cycle; next_fire_at
            // advances to the first unmaterialised one and stays due (it drains later)
            IntervalSpec everySecond = new IntervalSpec(Duration.ofSeconds(1), false);
            Instant nextFire = NOW.minus(Duration.ofHours(2));

            FiringPlanner.Plan plan = planner.plan(everySecond, Misfire.FIRE_ALL_MISSED, nextFire, NOW);

            assertThat(plan.occurrences()).hasSize(FiringPlanner.MAX_OCCURRENCES_PER_CYCLE);
            assertThat(plan.occurrences().getFirst()).isEqualTo(nextFire);
            assertThat(plan.nextFireAt()).isEqualTo(nextFire.plusSeconds(FiringPlanner.MAX_OCCURRENCES_PER_CYCLE));
            assertThat(plan.nextFireAt()).isBefore(NOW);
        }

        /** "The cap applies to every materialisation": FIRE_NOW's compensation reserves its own slot — never cap+1, and never discarded. */
        @Test
        void fireNowCompensationStaysWithinTheCycleCap() {
            IntervalSpec everyTenMillis = new IntervalSpec(Duration.ofMillis(10), false);
            Instant nextFire = NOW.minus(Duration.ofMinutes(2)); // missed beyond the threshold plus ~6000 recent ones

            FiringPlanner.Plan plan = planner.plan(everyTenMillis, Misfire.FIRE_NOW, nextFire, NOW);

            assertThat(plan.occurrences()).hasSize(FiringPlanner.MAX_OCCURRENCES_PER_CYCLE);
            assertThat(plan.occurrences().getLast()).isEqualTo(NOW);
            assertThat(plan.nextFireAt()).isBefore(NOW); // Capped — it stays due and drains on the next tick
        }

        @Test
        void occurrenceExactlyAtTheThresholdIsNotMissed() {
            Instant nextFire = NOW.minus(THRESHOLD);

            FiringPlanner.Plan plan = planner.plan(everyTenSeconds, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isFalse();
            assertThat(plan.occurrences()).startsWith(nextFire);
        }
    }

    @Nested
    class Cron {

        private final CronSpec dailyAtTwo = new CronSpec("0 0 2 * * *", ZoneId.of("UTC"));

        @Test
        void ignoreResumesAtTheNextRegularOccurrence() {
            // The 02:00 firing missed 10 hours ago: IGNORE materialises nothing and resumes at tomorrow's
            // 02:00
            Instant nextFire = Instant.parse("2026-08-15T02:00:00Z");

            FiringPlanner.Plan plan = planner.plan(dailyAtTwo, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isTrue();
            assertThat(plan.occurrences()).isEmpty();
            assertThat(plan.nextFireAt()).isEqualTo(Instant.parse("2026-08-16T02:00:00Z"));
        }

        @Test
        void fireNowCompensatesAMissedCronFire() {
            Instant nextFire = Instant.parse("2026-08-15T02:00:00Z");

            FiringPlanner.Plan plan = planner.plan(dailyAtTwo, Misfire.FIRE_NOW, nextFire, NOW);

            assertThat(plan.occurrences()).containsExactly(NOW);
            assertThat(plan.nextFireAt()).isEqualTo(Instant.parse("2026-08-16T02:00:00Z"));
        }

        @Test
        void dueWithinThresholdFiresAtTheScheduledInstant() {
            CronSpec everyMinute = new CronSpec("0 * * * * *", ZoneId.of("UTC"));
            Instant nextFire = NOW.minusSeconds(30); // 11:59:30 is not a real occurrence, but it is the stored value

            FiringPlanner.Plan plan = planner.plan(everyMinute, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isFalse();
            assertThat(plan.occurrences()).containsExactly(nextFire, NOW);
            assertThat(plan.nextFireAt()).isEqualTo(NOW.plusSeconds(60));
        }

        @Test
        void fireAllMissedReplaysEachMissedCronOccurrence() {
            CronSpec everyMinute = new CronSpec("0 * * * * *", ZoneId.of("UTC"));
            Instant nextFire = NOW.minus(Duration.ofMinutes(3));

            FiringPlanner.Plan plan = planner.plan(everyMinute, Misfire.FIRE_ALL_MISSED, nextFire, NOW);

            assertThat(plan.occurrences()).containsExactly(
                    NOW.minusSeconds(180), NOW.minusSeconds(120), NOW.minusSeconds(60), NOW);
        }
    }

    @Nested
    class FixedDelay {

        private final IntervalSpec afterFinish = new IntervalSpec(Duration.ofMinutes(5), true);

        @Test
        void dueWithinThresholdFiresAndAwaitsTheFinish() {
            Instant nextFire = NOW.minusSeconds(10);

            FiringPlanner.Plan plan = planner.plan(afterFinish, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isFalse();
            assertThat(plan.occurrences()).containsExactly(nextFire);
            assertThat(plan.nextFireAt()).isNull(); // the completion rearms it
        }

        @Test
        void ignoreSkipsTheMissedFireAndReanchorsFromNow() {
            Instant nextFire = NOW.minus(Duration.ofMinutes(30));

            FiringPlanner.Plan plan = planner.plan(afterFinish, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isTrue();
            assertThat(plan.occurrences()).isEmpty();
            assertThat(plan.nextFireAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
        }

        @Test
        void fireNowFiresImmediatelyAndAwaitsTheFinish() {
            Instant nextFire = NOW.minus(Duration.ofMinutes(30));

            FiringPlanner.Plan plan = planner.plan(afterFinish, Misfire.FIRE_NOW, nextFire, NOW);

            assertThat(plan.misfired()).isTrue();
            assertThat(plan.occurrences()).containsExactly(NOW);
            assertThat(plan.nextFireAt()).isNull();
        }

        @Test
        void fireAllMissedBehavesLikeFireNowOnASingleOccurrenceChain() {
            Instant nextFire = NOW.minus(Duration.ofMinutes(30));

            FiringPlanner.Plan plan = planner.plan(afterFinish, Misfire.FIRE_ALL_MISSED, nextFire, NOW);

            assertThat(plan.occurrences()).containsExactly(NOW);
            assertThat(plan.nextFireAt()).isNull();
        }
    }

    @Test
    void onDemandIsNeverDue() {
        assertThatThrownBy(() -> planner.plan(new OnDemandSpec(), Misfire.IGNORE, NOW, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never due");
    }

    @Test
    void rejectsNonPositiveThreshold() {
        assertThatThrownBy(() -> new FiringPlanner(new NextFireCalculator(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("misfireThreshold");
    }
    /**
     * The "not due yet" guard's 1ms slack is the COLUMN's resolution, not clock tolerance: DATETIME2
     * (100ns) and DATETIME(6) (microseconds) round the next_fire_at the calculation produced with
     * nanoseconds, and the value read back may land marginally ahead of the raw now.
     *
     * <p>Without the slack, a non-event becomes a log.error per tick — and the guard throws from inside
     * fireTrigger, so the job would stop firing with the node healthy.
     */
    @Test
    void aTriggerRoundedForwardByTheColumnIsStillDue() {
        assertThatNoException().isThrownBy(() -> planner.plan(new IntervalSpec(Duration.ofMinutes(5), false),
                Misfire.FIRE_NOW, NOW.plusMillis(1), NOW));
    }

    /** The other side of the boundary: a genuinely future trigger is a violation of the caller's contract. */
    @Test
    void aTriggerThatIsGenuinelyInTheFutureIsRejected() {
        assertThatThrownBy(() -> planner.plan(new IntervalSpec(Duration.ofMinutes(5), false),
                Misfire.FIRE_NOW, NOW.plusMillis(2), NOW))   // 1ms passes, 2ms does not — THIS is the boundary
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not due yet");
    }
}
