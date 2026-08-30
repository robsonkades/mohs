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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.engine.EngineSettings;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * S4 of the release suite: a dense schedule, and the lag between a trigger becoming due and its
 * execution existing in the queue.
 *
 * <p>Materialisation is the step nothing else in the bench isolates. Every other scenario seeds the
 * queue directly ({@code seedReady}) and therefore starts after this step has already happened;
 * here the definitions are what exist, and the assertion is on how quickly {@code mohs_ready} rows
 * appear once {@code next_fire_at} passes. The firing is capped per tick
 * ({@code Engine.FIRE_LIMIT} = 500) and shared across the cluster's shards, so the lag is a
 * function of how many triggers come due at once against how many ticks it takes to drain them.
 *
 * <p><strong>The scale is not the release criterion's, and that is deliberate.</strong> The criterion
 * asks for 10M scheduled triggers with 1% due per minute; the definitions here are seeded through the real
 * {@code JobStore}, one upsert each, because the alternative is a bulk INSERT in the harness that
 * restates {@code mohs_job_definitions}' columns per dialect — the drift this project keeps out of
 * benches. Measured 2026-08-30 against a Testcontainers PostgreSQL: about 18 ms per upsert — a round
 * trip each, on the pool-less DataSource the harness deliberately uses — which is six minutes for the
 * 20k default and roughly FIFTY HOURS for 10M. A bulk seeder is therefore not a convenience for the
 * criterion's scale, it is a prerequisite. The default is {@link #DEFAULT_DEFINITIONS} and the real
 * number is opt-in:
 *
 * <pre>{@code
 * ./mvnw -pl mohs-benchmark test -Dtest=ScheduleDensityScenario -Dmohs.scenario.definitions=10000000
 * }</pre>
 *
 * <p>Until that run happens the criterion is UNMET, not met — what this class provides is the
 * bench, not the verdict. A bulk seeder is the missing piece, and it belongs to the store, not here.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=ScheduleDensityScenario}. Selects a
 * backend with {@code -Dmohs.scenario.backend=sqlserver|mysql}.
 */
class ScheduleDensityScenario {

    /**
     * Six minutes of seeding, which keeps the scenario runnable; the criterion's 10M is opt-in below.
     *
     * <p>Measured here 2026-08-30, three nodes against a Testcontainers PostgreSQL: 0.259 s of
     * materialisation lag at this density against a 2 s target, and 0.125 s at 2,000 definitions.
     * Density does cost — ten times the schedule for twice the lag, which is the per-tick read of
     * every definition — but at these scales it is nowhere near the budget.
     */
    private static final int DEFAULT_DEFINITIONS = 20_000;
    static final String DEFINITIONS_PROPERTY = "mohs.scenario.definitions";

    /** The release criterion: 1% of the schedule comes due each minute… */
    private static final int DUE_PERCENT = 1;
    /** …and materialises within 2s of coming due. */
    private static final Duration LAG_TARGET = Duration.ofSeconds(2);
    private static final Duration OBSERVATION = Duration.ofMinutes(2);

    /** Long enough to outlast the arming loop at the default scale — the run refuses to report a number when it is not. */
    private static final Duration DUE_INTERVAL = Duration.ofSeconds(30);

    private static final int NODES = 3;
    private static final Duration INTERVAL = Duration.ofHours(1);

    @Test
    void triggersComingDueAreMaterialisedWithinTheLagTarget() {
        int definitions = Integer.getInteger(DEFINITIONS_PROPERTY, DEFAULT_DEFINITIONS);
        int due = Math.max(1, definitions * DUE_PERCENT / 100);
        ScenarioBackend backend = ScenarioBackend.current();
        DataSource dataSource = backend.freshSchema();
        Clock clock = Clock.systemUTC();
        AtomicInteger invocations = new AtomicInteger();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock, backend.dialect())) {
            Duration seeding = seedDenseSchedule(cluster, definitions - due);

            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            registerDueHandlers(cluster, due, invocations);
            cluster.startAll();

            long lastCameDueAt = armAndAwaitLastDue(cluster, due);
            boolean materialised = ScenarioCluster.awaitUntil(OBSERVATION,
                    () -> cluster.countMaterialisedJobs("due-") >= due);
            Duration lag = Duration.ofNanos(System.nanoTime() - lastCameDueAt);

            report(definitions, due, seeding, lag, cluster.oldestQueuedAge(), invocations.get(),
                    cluster.countTerminal("SUCCEEDED"), cluster.countTerminal("FAILED"));

            assertThat(materialised)
                    .as("every one of the %d due triggers must reach the queue inside %s", due, OBSERVATION)
                    .isTrue();
            assertThat(lag)
                    .as("materialisation lag for %d triggers due at once, over a schedule of %d", due, definitions)
                    .isLessThan(LAG_TARGET);
        }
    }

    /**
     * The dense part of the schedule: an hourly interval means everything seeded here is armed for
     * an hour from now, so nothing fires by accident and the population is real work for every read
     * of the definitions the tick makes. Returns how long the seeding itself took.
     */
    private static Duration seedDenseSchedule(ScenarioCluster cluster, int count) {
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            cluster.defineRecurring("dense-" + i, INTERVAL);
        }
        return Duration.ofNanos(System.nanoTime() - start);
    }

    private static void registerDueHandlers(ScenarioCluster cluster, int due, AtomicInteger invocations) {
        for (int i = 0; i < due; i++) {
            cluster.registerEverywhere("due-" + i, (_, _) -> invocations.incrementAndGet());
        }
    }

    /**
     * Arms the 1% and returns the {@code nanoTime} instant at which the LAST of them came due — the
     * only honest zero for the lag measured after it.
     *
     * <p>Arming is {@code due} sequential upserts on a pool-less DataSource, and each trigger comes
     * due {@link #DUE_INTERVAL} after ITS OWN upsert, so the batch comes due staggered across the
     * arming window. Starting the clock before the end of that window would charge the engine for
     * the harness's own loop; starting it once the earliest triggers were already overdue would
     * credit the engine with their head start. Hence the wait — and hence the refusal: the run says
     * nothing unless arming finished before the FIRST trigger came due.
     */
    private static long armAndAwaitLastDue(ScenarioCluster cluster, int due) {
        long armStart = System.nanoTime();
        armDueTriggers(cluster, due);
        long lastArmedAt = System.nanoTime();
        Duration arming = Duration.ofNanos(lastArmedAt - armStart);
        if (arming.compareTo(DUE_INTERVAL) >= 0) {
            throw new IllegalStateException(("arming %d triggers took %s, which is not shorter than the %s interval"
                    + " — the first triggers came due while the rest were still being written, so any lag measured"
                    + " here is meaningless. Raise DUE_INTERVAL, or lower -D%s")
                    .formatted(due, arming, DUE_INTERVAL, DEFINITIONS_PROPERTY));
        }
        sleepUntil(lastArmedAt + DUE_INTERVAL.toNanos());
        return System.nanoTime();
    }

    /**
     * The 1%, armed only once the engines are already running: defining them before the start would
     * fold the cluster's own boot into the lag being measured.
     */
    private static void armDueTriggers(ScenarioCluster cluster, int due) {
        for (int i = 0; i < due; i++) {
            cluster.defineRecurring("due-" + i, DUE_INTERVAL);
        }
    }

    private static void sleepUntil(long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            return;
        }
        try {
            Thread.sleep(Duration.ofNanos(remaining));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the triggers to come due", e);
        }
    }

    /**
     * The defaults, deliberately: this scenario is about the tick's firing capacity, and raising
     * {@code claimRounds} or the batch size would move the number without moving the behaviour
     * being measured.
     */
    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 1,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(int definitions, int due, Duration seeding, Duration lag, Duration oldest,
            int invocations, int succeeded, int failed) {
        System.out.printf("""

                === Schedule density (S4) — %d definitions, %d due at once, %d nodes ===
                seeding took         : %s
                materialisation lag  : %s (target < %s)
                oldest queued at end : %s
                handler invocations  : %d
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                %s
                """, definitions, due, NODES, seeding, lag, LAG_TARGET, oldest, invocations, succeeded, failed,
                scaleNote(definitions));
    }

    /** A number below the criterion's scale is not a verdict, and the output has to say so. */
    private static String scaleNote(int definitions) {
        return definitions >= 10_000_000 ? ""
                : "NOTE: below the criterion's 10M — run with -D" + DEFINITIONS_PROPERTY
                        + "=10000000 for its real scale";
    }
}
