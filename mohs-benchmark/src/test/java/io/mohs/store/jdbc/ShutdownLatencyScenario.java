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
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.engine.EngineSettings;
import io.mohs.engine.JobHandler;

/**
 * What a SIGTERM costs with the node FULL — the operational question {@link NodeChurnScenario} does
 * not answer, because there the interest is what is lost, not how long the wait is.
 *
 * <p>Here the node leaves with dispatch saturated ({@code dispatchConcurrency} executions in
 * flight, slow handler), and what is measured is the clock: an orchestrator waiting
 * {@code terminationGracePeriodSeconds} needs to know whether {@code stop(grace)} finishes in
 * handler time or in grace time.
 *
 * <p>The declared ceiling is the drain contract: draining means waiting for what is in flight to
 * FINISH, so the floor is one handler's duration and the ceiling is {@code grace}. What this
 * scenario protects is the middle ground — that the wait does not become the whole grace because
 * of work nobody is actually waiting for any more.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=ShutdownLatencyScenario}.
 */
class ShutdownLatencyScenario {

    private static final int SEED = 20_000;
    private static final int DISPATCH_CONCURRENCY = 256;
    /** Deliberately slow handler: it is what keeps the node FULL at the instant of the signal. */
    private static final Duration HANDLER_WORK = Duration.ofMillis(250);
    private static final Duration GRACE = Duration.ofSeconds(30);

    @Test
    void aFullNodeShutsDownInHandlerTimeNotInGraceTime() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger interrupted = new AtomicInteger();
        JobHandler handler = (_, _) -> {
            invocations.incrementAndGet();
            try {
                Thread.sleep(HANDLER_WORK);
            } catch (InterruptedException e) {
                interrupted.incrementAndGet();
                throw e;
            }
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC())) {
            // retries(0): the measured number is the COST of the stop, and a redelivery from a
            // reclaim would inflate `invocations` without the shutdown window — the experiment's
            // variable — having changed
            cluster.defineJob("slow", spec -> spec.retries(0));
            for (int i = 0; i < 2; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("slow", handler);
            cluster.seedReady("slow", SEED, 20);
            cluster.startAll();

            // The node has to be FULL: waiting for cluster ownership to approach both nodes'
            // ceiling is what makes the signal land at the worst instant
            ScenarioCluster.awaitUntil(Duration.ofSeconds(60),
                    () -> cluster.countLease() >= DISPATCH_CONCURRENCY);
            int inFlightAtSignal = cluster.countLease();

            long signalAt = System.nanoTime();
            cluster.nodes().getFirst().engine().stop(GRACE);
            Duration stopTook = Duration.ofNanos(System.nanoTime() - signalAt);

            System.out.printf("""

                    === Shutdown latency — one node of two, dispatch saturated ===
                    in flight at signal  : %d (cluster), dispatch cap %d per node
                    stop(grace=%s) took  : %.2fs
                    handler duration     : %s  <- the floor a graceful drain cannot beat
                    handlers interrupted : %d  <- non-zero means the grace was exhausted
                    invocations          : %d
                    """, inFlightAtSignal, DISPATCH_CONCURRENCY, GRACE, stopTook.toNanos() / 1e9, HANDLER_WORK,
                    interrupted.get(), invocations.get());

            assertThat(stopTook)
                    .as("a graceful stop must finish in handler time, not in grace time — %s means it waited for "
                            + "something nobody was waiting for", stopTook)
                    .isLessThan(GRACE.dividedBy(3));
            assertThat(interrupted.get())
                    .as("no handler may be interrupted: the grace was far longer than the work in flight")
                    .isZero();
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 256, DISPATCH_CONCURRENCY, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }
}
