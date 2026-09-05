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
package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.engine.EngineSettings;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * S3 of the release suite: a burst of 0 to 100k enqueues in 10 seconds, and the question of whether
 * the cluster catches back up.
 *
 * <p>The criterion is an AGE, not a count, and that is the whole point: a backlog of 10k entries one
 * second old and a backlog of 10k entries a minute old are the same number and a different
 * incident. What is asserted is that the oldest entry a claim could take is back under
 * {@link #RECOVERY_TARGET} within {@link #RECOVERY_BUDGET} of the burst ending — the shape of "the
 * spike passed" rather than "the queue emptied", which a slower handler would fail for reasons that
 * have nothing to do with the engine.
 *
 * <p>The handler is deliberately trivial. This scenario measures the claim and dispatch path's
 * ability to absorb arrival rate; putting work inside the handler would measure the executor's
 * concurrency ceiling instead, which {@code ShutdownLatencyScenario} already covers.
 *
 * <p>Enqueues arrive in {@link #WAVES} waves rather than as one seed, because a single insert of
 * 100k rows is not a burst — it is a bulk load, and the engine would see it as one already-complete
 * backlog. Waves reproduce what the queue actually experiences: claim competing with insert.
 *
 * <p>Run by name (the surefire default pattern does not pick up {@code *Scenario}), with the burst
 * size and the backend both selectable:
 *
 * <pre>{@code
 * ./mvnw -pl mohs-benchmark test -Dtest=BurstAbsorptionScenario -Dmohs.scenario.burst=40000
 * }</pre>
 *
 * <p>Selects a backend with {@code -Dmohs.scenario.backend=sqlserver|mysql}.
 */
class BurstAbsorptionScenario {

    /**
     * The release criterion's size. It presupposes a throughput the harness's own environment may not
     * have: clearing this inside {@link #RECOVERY_BUDGET} after a {@link #WAVES}-second arrival needs
     * roughly 1,700 executions/s sustained, on top of absorbing the arrivals themselves.
     *
     * <p>Measured 2026-08-30 on three in-JVM engines against a Testcontainers PostgreSQL: about 1,150
     * executions/s end to end, so the criterion's size does not recover inside the budget HERE —
     * linearly, with no loss and no stall, simply slower than the budget. That is a statement about
     * this rig, not about the engine: it is not the one the performance baseline was taken on (the
     * demo application against a tuned local PostgreSQL, whose operating point is several times
     * higher). Run the criterion's size there; use {@link #BURST_PROPERTY} to get a meaningful
     * absorption curve anywhere else.
     */
    private static final int DEFAULT_BURST = 100_000;
    static final String BURST_PROPERTY = "mohs.scenario.burst";
    private static final int WAVES = 10;
    private static final Duration WAVE_INTERVAL = Duration.ofSeconds(1);
    private static final int NODES = 3;

    /** The release criterion: the age of the oldest queued entry back under 5s… */
    private static final Duration RECOVERY_TARGET = Duration.ofSeconds(5);
    /** …within 60s of the last wave. */
    private static final Duration RECOVERY_BUDGET = Duration.ofSeconds(60);

    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);

    @Test
    void theBurstIsAbsorbedWithinTheRecoveryBudget() {
        // Rounded down to a whole number of waves: the waves are what actually gets enqueued, and a
        // report that states the number ASKED FOR while the queue saw fewer is a report that lies
        int burst = Integer.getInteger(BURST_PROPERTY, DEFAULT_BURST) / WAVES * WAVES;
        ScenarioBackend backend = ScenarioBackend.current();
        DataSource dataSource = backend.freshSchema();
        Clock clock = Clock.systemUTC();
        AtomicInteger invocations = new AtomicInteger();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock, backend.delegate())) {
            // retries(0): a retry would re-enter the same queue and inflate the very backlog being
            // measured, turning a dispatch hiccup into an arrival rate the scenario never applied
            cluster.defineJob("burst", spec -> spec.retries(0));
            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("burst", (_, _) -> invocations.incrementAndGet());
            cluster.startAll();

            List<Sample> duringBurst = enqueueWaves(cluster, burst);
            long burstEndedNanos = System.nanoTime();
            Recovery recovery = awaitRecovery(cluster, burstEndedNanos);

            // ONE read, used by both the report and the assertion: the cluster is still running here,
            // so two reads can disagree — and a report that prints "FAILED: 0" beside a red assertion
            // about FAILED is exactly the half-hour of confusion this harness exists to prevent
            int failed = cluster.countTerminal("FAILED");
            report(burst, duringBurst, recovery, invocations.get(), cluster.countTerminal("SUCCEEDED"), failed);

            assertThat(failed)
                    .as("a burst is arrival rate, not failure — nothing here may end FAILED")
                    .isZero();
            assertThat(recovery.recovered())
                    .as("the oldest queued entry must be younger than %s within %s of the burst ending; "
                            + "last sample was %s behind with %d still queued",
                            RECOVERY_TARGET, RECOVERY_BUDGET, recovery.last().oldestAge(), recovery.last().queued())
                    .isTrue();
        }
    }

    /**
     * Samples the queue once a second until its head is back under {@link #RECOVERY_TARGET}, or the
     * budget runs out. The samples are kept rather than only the verdict: a run that misses the
     * target is answered by the SHAPE of the catch-up — still falling behind, or merely slow.
     */
    private static Recovery awaitRecovery(ScenarioCluster cluster, long burstEndedNanos) {
        List<Sample> samples = new ArrayList<>();
        boolean recovered = ScenarioCluster.awaitUntil(RECOVERY_BUDGET, () -> {
            Sample sample = sampleOf(cluster, burstEndedNanos);
            samples.add(sample);
            sleep(SAMPLE_INTERVAL);
            return sample.oldestAge().compareTo(RECOVERY_TARGET) < 0;
        });
        return new Recovery(recovered, List.copyOf(samples));
    }

    /** What the catch-up phase produced: the verdict and the trace behind it. */
    private record Recovery(boolean recovered, List<Sample> samples) {

        /**
         * The state the phase ended in. Always present: {@code awaitUntil} evaluates its condition
         * at least once, and this phase's condition is what appends a sample.
         */
        Sample last() {
            return samples.getLast();
        }
    }

    /**
     * The waves. Each one is a {@code seedReady} of {@code burst / WAVES} entries, one second apart,
     * and the samples taken between them are what shows how far behind the queue fell WHILE the
     * arrivals were still coming — the part a post-hoc measurement cannot reconstruct.
     */
    private static List<Sample> enqueueWaves(ScenarioCluster cluster, int burst) {
        List<Sample> samples = new ArrayList<>(WAVES);
        long start = System.nanoTime();
        for (int wave = 0; wave < WAVES; wave++) {
            cluster.seedReady("burst", burst / WAVES, 20);
            samples.add(sampleOf(cluster, start));
            sleep(WAVE_INTERVAL);
        }
        return samples;
    }

    private static Sample sampleOf(ScenarioCluster cluster, long sinceNanos) {
        return new Sample(Duration.ofNanos(System.nanoTime() - sinceNanos), cluster.countReady(),
                cluster.oldestQueuedAge());
    }

    /** One observation: when it was taken, how deep the queue was, and how far behind its head had fallen. */
    private record Sample(Duration at, int queued, Duration oldestAge) {
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while pacing the scenario", e);
        }
    }

    /**
     * {@code claimRounds} at 3 and a {@code max-poll-interval} of 200ms: a burst is precisely the
     * regime the chained rounds exist for, and leaving the backoff ceiling at its 2s default would
     * measure the sleep rather than the claim.
     */
    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(int burst, List<Sample> duringBurst, Recovery recovery, int invocations, int succeeded,
            int failed) {
        StringBuilder trace = new StringBuilder();
        appendPhase(trace, "burst", duringBurst);
        appendPhase(trace, "recovery", recovery.samples());
        Sample last = recovery.last();
        System.out.printf("""

                === Burst absorption (S3) — %d enqueues over %d waves, %d nodes ===
                %s
                oldest at the end    : %s (target < %s)
                still queued         : %d
                handler invocations  : %d
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                """, burst, WAVES, NODES, trace, last.oldestAge(), RECOVERY_TARGET, last.queued(),
                invocations, succeeded, failed);
    }

    private static void appendPhase(StringBuilder trace, String phase, List<Sample> samples) {
        for (Sample sample : samples) {
            trace.append("  %-8s %6s  queued=%7d  oldest=%s%n"
                    .formatted(phase, sample.at(), sample.queued(), sample.oldestAge()));
        }
    }
}
