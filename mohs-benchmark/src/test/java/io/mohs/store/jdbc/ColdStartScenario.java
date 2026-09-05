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

import io.mohs.engine.EngineSettings;

/**
 * Cluster start-up with a backlog already waiting — the first minute of every deploy, and the most
 * ordinary scenario there is: N nodes come up at once and find a full queue. No chaos, no churn,
 * no node dying.
 *
 * <p>It exists to isolate a suspicion raised by {@link NodeChurnScenario}: the executions lost
 * there were stamped at START-UP, not at the node's exit. The hypothesis is the window between two
 * steps of a peer's tick — it reads {@code mohs_nodes} and only then runs the reaper, so a node
 * born INSIDE that window has already claimed work while being absent from the snapshot, and
 * absent from {@code mohs_nodes} means dead by definition. If the hypothesis holds, the peer
 * reclaims live work from a healthy node, and the fence — which guards by the DEAD node's
 * ownership — does not protect it.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=ColdStartScenario}.
 */
class ColdStartScenario {

    private static final int SEED = 4_000;
    private static final int NODES = 3;
    private static final Duration HANDLER_WORK = Duration.ofMillis(50);
    private static final Duration OBSERVATION = Duration.ofMinutes(2);

    @Test
    void nodesStartingTogetherOnABacklogLoseNothing() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        Clock clock = Clock.systemUTC();
        AtomicInteger invocations = new AtomicInteger();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock)) {
            // Budget declared as ZERO on purpose: this scenario measures LOSS on a cold start, and
            // inheriting the product default (1) would give a reclaim a second chance, let it end
            // SUCCEEDED, and make the `failed == 0` assertion tolerate exactly the event it exists
            // to catch
            cluster.defineJob("cold", spec -> spec.retries(0));
            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("cold", (_, _) -> {
                invocations.incrementAndGet();
                Thread.sleep(HANDLER_WORK);
            });

            cluster.seedReady("cold", SEED, 20);
            cluster.startAll();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);

            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            report(settled, invocations.get(), succeeded, failed, cluster.failureKinds());

            assertThat(settled)
                    .as("the queue must drain — a stalled queue is a different bug from a lost execution, "
                            + "and the assertions below cannot tell them apart")
                    .isTrue();
            assertThat(failed)
                    .as("nothing may be lost to a plain cluster start — no node died, no handler threw")
                    .isZero();
            assertThat(succeeded).isEqualTo(SEED);
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(boolean settled, int invocations, int succeeded, int failed,
            Map<String, Integer> failureKinds) {
        System.out.printf("""

                === Cold start — %d nodes starting together on a %d backlog ===
                queue settled        : %s
                handler invocations  : %d
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                failure kinds        : %s
                """, NODES, SEED, settled ? "yes" : "NO — still draining at the timeout", invocations,
                succeeded, failed, failureKinds);
    }
}
