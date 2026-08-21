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
            // devidas há 25s: 3 ocorrências (25s, 15s, 5s atrás), todas dentro do threshold
            Instant nextFire = NOW.minusSeconds(25);

            FiringPlanner.Plan plan = planner.plan(everyTenSeconds, Misfire.IGNORE, nextFire, NOW);

            assertThat(plan.misfired()).isFalse();
            assertThat(plan.occurrences()).containsExactly(
                    NOW.minusSeconds(25), NOW.minusSeconds(15), NOW.minusSeconds(5));
            assertThat(plan.nextFireAt()).isEqualTo(NOW.plusSeconds(5));
        }

        @Test
        void ignoreDropsMissedButKeepsTheSeriesAnchor() {
            // devidas há 10min numa agenda de 1min: perdidas descartadas, mas as
            // recentes e o próximo disparo continuam na série original (:00 do minuto)
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
            Instant nextFire = NOW.minus(Duration.ofMinutes(10)).minusSeconds(30); // série em :30

            FiringPlanner.Plan plan = planner.plan(everyMinute, Misfire.FIRE_NOW, nextFire, NOW);

            assertThat(plan.misfired()).isTrue();
            // a recente (30s atrás) dispara normalmente; [now] compensa as 10 perdidas
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
            // 2h de perdidas numa agenda de 1s: só 1.440 por ciclo; o next_fire_at
            // avança até a primeira não materializada e continua devido (drena depois)
            IntervalSpec everySecond = new IntervalSpec(Duration.ofSeconds(1), false);
            Instant nextFire = NOW.minus(Duration.ofHours(2));

            FiringPlanner.Plan plan = planner.plan(everySecond, Misfire.FIRE_ALL_MISSED, nextFire, NOW);

            assertThat(plan.occurrences()).hasSize(FiringPlanner.MAX_OCCURRENCES_PER_CYCLE);
            assertThat(plan.occurrences().getFirst()).isEqualTo(nextFire);
            assertThat(plan.nextFireAt()).isEqualTo(nextFire.plusSeconds(FiringPlanner.MAX_OCCURRENCES_PER_CYCLE));
            assertThat(plan.nextFireAt()).isBefore(NOW);
        }

        /** "O cap vale para toda materialização" (ADR-0035): a compensação do FIRE_NOW reserva a própria vaga — nunca cap+1, e nunca é descartada. */
        @Test
        void fireNowCompensationStaysWithinTheCycleCap() {
            IntervalSpec everyTenMillis = new IntervalSpec(Duration.ofMillis(10), false);
            Instant nextFire = NOW.minus(Duration.ofMinutes(2)); // perdidas além do threshold + ~6000 recentes

            FiringPlanner.Plan plan = planner.plan(everyTenMillis, Misfire.FIRE_NOW, nextFire, NOW);

            assertThat(plan.occurrences()).hasSize(FiringPlanner.MAX_OCCURRENCES_PER_CYCLE);
            assertThat(plan.occurrences().getLast()).isEqualTo(NOW);
            assertThat(plan.nextFireAt()).isBefore(NOW); // capado — continua devido, drena no próximo tick
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
            // disparo das 02:00 perdido há 10h: IGNORE não materializa nada e
            // retoma nas 02:00 de amanhã
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
            Instant nextFire = NOW.minusSeconds(30); // 11:59:30 não é ocorrência real, mas é o valor armazenado

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
            assertThat(plan.nextFireAt()).isNull(); // a conclusão rearma (ADR-0035)
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
}
