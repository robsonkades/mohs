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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.Batch;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.Succeeded;
import io.mohs.core.job.JobRef;
import io.mohs.engine.BatchCompletionCallbacks;
import io.mohs.engine.EngineSettings;

/**
 * A large batch created through the public facade and drained by a three-node cluster.
 *
 * <p>The criterion is the batch counter: {@code BatchCompleted} is born from the
 * {@code UPDATE ... RETURNING} that closes the batch, so TWO nodes completing the second-to-last
 * and the last member at the same time must produce ONE event — not two, and not none, which is
 * what a lost increment would give.
 *
 * <p>Members that FAIL count just the same, because a batch closes on its total rather than on
 * successes: half the members throw on purpose, which also exercises the path where a
 * {@code Failed} closes a batch. The job declares {@code retries(1)}, and that is not a bench
 * detail — it is what makes the counter assertion FALSIFIABLE: each failing member is invoked
 * TWICE and must count ONCE, which is what separates "counted the terminal failure" from "counted
 * every attempt", the latter closing the batch early. With no budget ({@code retries = 0}) both
 * behaviours would produce the same number and the assertion would prove nothing.
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=BatchCompletionScenario}.
 */
class BatchCompletionScenario {

    private static final int MEMBERS = 20_000;
    /**
     * One retry beyond the first attempt: this is what makes the counter assertion FALSIFIABLE.
     *
     * <p>With no budget each member is invoked ONCE, and then "count the terminal failure" and
     * "count every attempt" produce the same number — a per-attempt counter would pass green. With
     * one retry the failing member is invoked TWICE and must count ONCE.
     */
    private static final int RETRIES = 1;
    private static final int NODES = 3;
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(3);
    /**
     * Extra window after closure: a SECOND event would arrive within it, and asserting "exactly
     * one" without waiting would be asserting about a race that has not finished.
     */
    private static final Duration SECOND_EVENT_WINDOW = Duration.ofSeconds(5);

    @Test
    void aLargeBatchClosesExactlyOnceUnderConcurrentCompletion() {
        ScenarioBackend backend = ScenarioBackend.current();
        DataSource dataSource = backend.freshSchema();
        Clock clock = Clock.systemUTC();
        List<BatchCompleted> completions = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger terminalEvents = new AtomicInteger();
        ExecutionListener collector = event -> {
            switch (event) {
                case BatchCompleted completed -> completions.add(completed);
                case Succeeded _, Failed _ -> terminalEvents.incrementAndGet();
                default -> {
                }
            }
        };
        BatchCompletionCallbacks callbacks = new BatchCompletionCallbacks();

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock, backend.dialect())) {
            cluster.defineJob("member", spec -> spec.retries(RETRIES));
            for (int i = 0; i < NODES; i++) {
                cluster.addNode(settings(), List.of(collector, callbacks));
            }
            cluster.registerEverywhere("member", (payload, _) -> {
                invocations.incrementAndGet();
                if (Integer.parseInt(payload.toString()) % 2 == 0) {
                    failures.incrementAndGet();
                    throw new IllegalStateException("deliberate member failure");
                }
            });
            cluster.startAll();

            JobRef<String> member = JobRef.of("member", String.class);
            long createdAt = System.nanoTime();
            Batch batch = cluster.facadeFor(cluster.nodes().getFirst(), callbacks)
                    .batch("release-validation", builder -> {
                        for (int i = 0; i < MEMBERS; i++) {
                            builder.add(member, String.valueOf(i));
                        }
                    });
            long createNanos = System.nanoTime() - createdAt;

            boolean closed = ScenarioCluster.awaitUntil(DRAIN_TIMEOUT, () -> !completions.isEmpty());
            long drainNanos = System.nanoTime() - createdAt;
            ScenarioCluster.awaitUntil(SECOND_EVENT_WINDOW, () -> completions.size() > 1);

            report(createNanos, drainNanos, closed, completions, invocations.get(), failures.get(),
                    cluster.countReady(), cluster.countLease(), terminalEvents.get());

            // The PRIMARY evidence comes from the table, not from the event: mohs_batches is the
            // source of truth and loses nothing. The event channel is best-effort by contract, so
            // it is verified (below) but is never the only witness.
            Map<String, Object> counters = cluster.jdbc().queryForMap(
                    "SELECT total, succeeded, failed FROM mohs_batches WHERE id = ?", batch.batchId());
            assertThat(counters.get("total")).isEqualTo(MEMBERS);
            assertThat(counters.get("failed"))
                    .as("the batch counter must count the TERMINAL failure once, not each attempt — with retries=%d "
                            + "each deliberate failure is invoked twice", RETRIES)
                    .isEqualTo(MEMBERS / 2);
            assertThat(counters.get("succeeded")).isEqualTo(MEMBERS / 2);

            // With one retry, every even member runs twice: STRICT equality is what separates
            // "the counter is terminal" from "the counter is per-attempt", which a loose
            // inequality would let through
            assertThat(invocations.get())
                    .as("every deliberate failure must be invoked twice with retries=%d — otherwise the counter "
                            + "assertion above cannot tell terminal counting from per-attempt counting", RETRIES)
                    .isEqualTo(MEMBERS + MEMBERS / 2);

            // Only now the channel: if every terminal event arrived, it dropped nothing, and the
            // BatchCompleted count means something
            assertThat(terminalEvents.get())
                    .as("the event channel dropped events — the BatchCompleted assertion below would be vacuous")
                    .isEqualTo(MEMBERS);
            assertThat(completions).as("BatchCompleted must fire exactly once for batch %s", batch.batchId())
                    .hasSize(1);
            BatchCompleted completed = completions.getFirst();
            assertThat(completed.total()).isEqualTo(MEMBERS);
            assertThat(completed.succeeded() + completed.failed())
                    .as("every member must be counted exactly once in the closing snapshot")
                    .isEqualTo(MEMBERS);
            assertThat(cluster.countReady() + cluster.countLease())
                    .as("queue and lease must be empty once the batch closed")
                    .isZero();
        }
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 256, 128, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    private static void report(long createNanos, long drainNanos, boolean closed, List<BatchCompleted> completions,
            int invocations, int failures, int ready, int lease, int terminalEvents) {
        System.out.printf("""

                === S10 — batch completion (%d members, %d nodes) ===
                batch creation       : %.1fs (%.0f members/s)
                drain to completion  : %s
                BatchCompleted count : %d %s
                handler invocations  : %d (%d deliberate failures)
                left in queue/lease  : %d / %d
                terminal events seen : %d of %d (a shortfall means the event channel dropped)
                """, MEMBERS, NODES, createNanos / 1e9, MEMBERS / (createNanos / 1e9),
                closed ? "%.1fs".formatted(drainNanos / 1e9) : "NOT REACHED", completions.size(),
                completions.isEmpty() ? "" : completions.getFirst().toString(), invocations, failures, ready, lease, terminalEvents, MEMBERS);
    }
}
