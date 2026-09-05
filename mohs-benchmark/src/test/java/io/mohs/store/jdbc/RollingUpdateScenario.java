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
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.job.JobKey;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.JobHandler;

/**
 * A deploy that ADDS a job: during the rollout window, part of the cluster does not have the
 * handler yet. Two scenarios, because they are two questions with different answers, and mixing
 * them would produce a permanently red gate that everyone learns to ignore:
 *
 * <ol>
 *   <li>the rollout FINISHES within the retry budget — does the work survive? This is the question
 *       that decides whether the release can ship;</li>
 *   <li>a node never learns the handler — what does that cost? This is the known gap
 *       (handler-aware claiming was left out), asserted at the value it HAS today, so that it
 *       shouts when it gets worse.</li>
 * </ol>
 *
 * <p>The second cost is sharding arithmetic, not bad luck: {@code Shards.of} is a function of the
 * id, {@code ownedBy} splits 64 shards evenly between the 2 nodes, and the retry re-derives the
 * SAME shard. Every execution landing in the blind node's 32 shards is redelivered to it until the
 * budget runs out — half the backlog, deterministically.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=RollingUpdateScenario}.
 */
class RollingUpdateScenario {

    private static final String JOB = "moving";
    private static final int SEED = 400;
    private static final int RETRIES = 3;
    private static final Duration OBSERVATION = Duration.ofSeconds(90);

    @Test
    void workSurvivesARolloutThatCompletesWithinTheRetryBudget() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        AtomicInteger executed = new AtomicInteger();
        JobHandler handler = (_, _) -> executed.incrementAndGet();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC())) {
            cluster.defineJob(JOB, spec -> spec.retries(RETRIES));
            ScenarioCluster.Node upgraded = cluster.addNode(settings(), List.of());
            ScenarioCluster.Node lagging = cluster.addNode(settings(), List.of());
            upgraded.handlers().register(JobKey.of(JOB), handler);

            cluster.seedReady(JOB, SEED, 20);
            cluster.startAll();

            // The rollout reaches the second node: that is what a real deploy does, and it is the
            // difference between "a rollout window" and "a permanently heterogeneous cluster"
            ScenarioCluster.awaitUntil(Duration.ofSeconds(10), () -> executed.get() > 0);
            lagging.handlers().register(JobKey.of(JOB), handler);

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            report("rollout completes", settled, executed.get(), succeeded, failed, cluster.countAttempts(),
                    cluster.failureKinds());

            assertThat(settled)
                    .as("the queue must drain — a stalled queue is a different bug from a lost execution, "
                            + "and the assertions below cannot tell them apart")
                    .isTrue();
            assertThat(failed)
                    .as("a rollout that completes inside the retry budget must lose nothing")
                    .isZero();
            assertThat(succeeded).isEqualTo(SEED);
        }
    }

    /**
     * The known gap, measured. It asserts the CURRENT number, so it stays green and still shouts if
     * it gets worse; a {@code failed == 0} assertion here would be a TODO disguised as a test,
     * because it would demand the handler-aware claiming that was never implemented.
     */
    @Test
    void aNodeThatNeverLearnsTheHandlerBurnsTheRetryBudgetOfItsShards() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        AtomicInteger executed = new AtomicInteger();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC())) {
            cluster.defineJob(JOB, spec -> spec.retries(RETRIES));
            ScenarioCluster.Node upgraded = cluster.addNode(settings(), List.of());
            cluster.addNode(settings(), List.of()); // blind forever
            upgraded.handlers().register(JobKey.of(JOB), (_, _) -> executed.incrementAndGet());

            cluster.seedReady(JOB, SEED, 20);
            cluster.startAll();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            report("node stays blind", settled, executed.get(), succeeded, failed, cluster.countAttempts(),
                    cluster.failureKinds());

            assertThat(settled).as("the queue must drain even when half of it fails").isTrue();
            assertThat(succeeded + failed).as("every seeded execution must reach a terminal state").isEqualTo(SEED);
            assertThat(failed)
                    .as("KNOWN GAP (there is no handler-aware claiming): the blind node owns "
                            + "half of the 64 shards and burns the whole retry budget of every execution that lands "
                            + "there. A number far from half the seed means the shard assignment changed")
                    .isBetween(SEED / 2 - SEED / 20, SEED / 2 + SEED / 20);
            // By MESSAGE, not by type: the engine uses IllegalStateException for a missing handler,
            // for shutdown and for a dead node — only the text separates the three, and without
            // that distinction the assertion attributes nothing
            assertThat(cluster.failureKinds().keySet())
                    .as("the loss must be attributable to the missing handler, not to a reclaim or a shutdown")
                    .anyMatch(kind -> kind.contains("no handler registered for job"));
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(String arm, boolean settled, int executed, int succeeded, int failed, int attempts,
            Map<String, Integer> failureKinds) {
        System.out.printf("""

                === S9 — rolling update, 2 nodes, 1 without the handler [%s] ===
                queue settled        : %s
                handler invocations  : %d of %d seeded
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                attempts written     : %d (%.2f per execution)
                failure kinds        : %s
                """, arm, settled ? "yes" : "NO — still draining at the timeout", executed, SEED, succeeded, failed,
                attempts, attempts / (double) SEED, failureKinds);
    }
}
