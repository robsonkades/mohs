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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.job.JobKey;
import io.mohs.engine.EngineSettings;

/**
 * The product's everyday case: one recurring job, a three-node cluster, and the question "what if
 * two nodes fire the same trigger?".
 *
 * <p>Materialisation is a CAS on the definition (leader-free): the occurrence is materialised by
 * whoever wins the CAS, and the other two move on. Duplicating here is not delivering the same
 * execution twice — the claim covers that — it is creating TWO executions for the SAME occurrence,
 * which no idempotent consumer detects, because they carry different ids.
 *
 * <p>It also measures something nobody measured: PUNCTUALITY. The product promises a dispatch
 * latency floor, and the honest ruler is {@code started_at - scheduled_at} over real firings, not
 * over a synthetic enqueue.
 *
 * <p>And it closes with everyday pause/resume: while paused, nothing new is materialised; once
 * resumed, {@code FiringPlanner} materialises ALL missed occurrences at once. That is not misfire:
 * an occurrence due within the {@code misfireThreshold} (60s here) fires late under ANY policy,
 * {@code IGNORE} included — a job's {@code Misfire} only decides what to do with what is OLDER than
 * the threshold. The operational consequence is a burst on resume proportional to the pause, and
 * the scenario asserts that instead of pretending resume merely picks the cadence back up.
 *
 * <p>Scope of this uniqueness proof: {@code duplicateOccurrences} groups by {@code scheduled_at},
 * which only sees double materialisation on the path WITHOUT misfire — under {@code FIRE_NOW}
 * compensation the planner stamps {@code now} and two nodes would have different instants. This
 * scenario never crosses the threshold, so the proof holds for the punctual path.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=RecurringTriggerScenario}.
 */
class RecurringTriggerScenario {

    private static final String JOB = "ticker";
    private static final Duration EVERY = Duration.ofSeconds(1);
    private static final Duration RUN = Duration.ofSeconds(20);
    private static final Duration PAUSED = Duration.ofSeconds(6);
    private static final int NODES = 3;
    /** How many occurrences are expected after the resume, on top of the reproduced paused window. */
    private static final int RESUME_OBSERVATION_TICKS = 4;

    @Test
    void oneOccurrenceFiresOnceAcrossTheClusterAndPauseHolds() throws Exception {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            cluster.defineRecurring(JOB, EVERY);
            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere(JOB, (_, _) -> {
            });
            cluster.startAll();

            Thread.sleep(RUN.toMillis());

            int firedWhileRunning = cluster.countExecutionsOf(JOB);
            List<Map<String, Object>> duplicates = duplicateOccurrences(cluster);
            Map<String, Object> lateness = lateness(cluster);

            cluster.jobs().pause(JobKey.of(JOB));
            // One occurrence of slack: the pause applies to MATERIALISATION, and whatever was
            // materialised before it still fires
            Thread.sleep(EVERY.toMillis() * 2);
            int atPause = cluster.countExecutionsOf(JOB);
            Thread.sleep(PAUSED.toMillis());
            int afterPause = cluster.countExecutionsOf(JOB);

            cluster.jobs().resume(JobKey.of(JOB));
            Thread.sleep(EVERY.toMillis() * RESUME_OBSERVATION_TICKS);
            int afterResume = cluster.countExecutionsOf(JOB);

            long expectedWhileRunning = RUN.dividedBy(EVERY);
            report(firedWhileRunning, expectedWhileRunning, duplicates, lateness, atPause, afterPause, afterResume);

            assertThat(duplicates)
                    .as("two nodes materialised the same occurrence — the trigger's advance CAS did not hold")
                    .isEmpty();
            assertThat(firedWhileRunning)
                    .as("a %s job over %s should fire about %d times", EVERY, RUN, expectedWhileRunning)
                    .isBetween((int) expectedWhileRunning - 2, (int) expectedWhileRunning + 2);
            assertThat(afterPause - atPause).as("a paused job must not materialise new occurrences").isZero();
            // The resume REPRODUCES the paused window (it is within the misfireThreshold): the
            // expected burst is the whole pause plus the occurrences of the observation interval,
            // and asserting only "> 0" would let a resume that silently lost the window pass
            long expectedBurst = PAUSED.plus(EVERY.multipliedBy(RESUME_OBSERVATION_TICKS)).dividedBy(EVERY);
            assertThat(afterResume - afterPause)
                    .as("resume must bring the job back AND replay the paused window (%d occurrences within the "
                            + "misfire threshold)", expectedBurst)
                    .isGreaterThanOrEqualTo((int) expectedBurst - 2);
        }
    }

    /** Two executions for the SAME {@code scheduled_at} is the exact signature of double materialisation. */
    private static List<Map<String, Object>> duplicateOccurrences(ScenarioCluster cluster) {
        return cluster.jdbc().queryForList("""
                SELECT scheduled_at, count(*) AS total
                  FROM mohs_execution
                 WHERE job_key = ?
                 GROUP BY scheduled_at
                HAVING count(*) > 1
                """, JOB);
    }

    /** {@code started_at − scheduled_at} in milliseconds: the recurring trigger's real delay. */
    private static Map<String, Object> lateness(ScenarioCluster cluster) {
        return cluster.jdbc().queryForMap("""
                SELECT round(avg(delay_ms))                                            AS avg_ms,
                       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY delay_ms))    AS p50_ms,
                       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY delay_ms))   AS p95_ms,
                       round(max(delay_ms))                                            AS max_ms
                  FROM (SELECT EXTRACT(EPOCH FROM (a.started_at - e.scheduled_at)) * 1000 AS delay_ms
                          FROM mohs_execution e
                          JOIN mohs_attempt a ON a.execution_id = e.execution_id AND a.number = 1
                         WHERE e.job_key = ?) delays
                """, JOB);
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 32, 16, 1,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(int fired, long expected, List<Map<String, Object>> duplicates,
            Map<String, Object> lateness, int atPause, int afterPause, int afterResume) {
        System.out.printf("""

                === Recurring trigger — %s job, %d nodes ===
                fired in %s          : %d (expected ~%d)
                duplicate occurrences: %d %s
                dispatch lateness    : avg %s ms · p50 %s ms · p95 %s ms · max %s ms
                paused at            : %d executions
                after %s paused      : %d (delta %d — must be 0)
                after resume         : %d (delta %d — must be > 0)
                """, EVERY, NODES, RUN, fired, expected, duplicates.size(), duplicates.isEmpty() ? "" : duplicates,
                lateness.get("avg_ms"), lateness.get("p50_ms"), lateness.get("p95_ms"), lateness.get("max_ms"),
                atPause, PAUSED, afterPause, afterPause - atPause, afterResume, afterResume - afterPause);
    }
}
