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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.mohs.core.job.JobKey;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.JobHandler;

/**
 * An everyday deploy: a cluster with a backlog, one node leaving through {@code stop(grace)} — what
 * an orchestrator's SIGTERM triggers — and a new node taking its place mid-drain. Nothing here is
 * chaos; it is a Tuesday.
 *
 * <p>What is asserted:
 * <ul>
 *   <li>no execution is lost — all of them finish, and in {@code SUCCEEDED};</li>
 *   <li>redelivery is BOUNDED: only what was in flight when the node left may run twice, which is
 *       the at-least-once contract, and {@code stop(grace)} exists precisely so that not even that
 *       happens — the expected damage with enough grace is ZERO;</li>
 *   <li>the departed node's shards are claimed again: if the derived assignment did not react to
 *       membership, the queue would stall with ready work in front of it — and that is what
 *       {@code settled} catches.</li>
 * </ul>
 *
 * <p>Run by name: {@code ./mvnw -pl mohs-benchmark test -Dtest=NodeChurnScenario}.
 */
class NodeChurnScenario {

    private static final int SEED = 8_000;
    private static final Duration OBSERVATION = Duration.ofMinutes(2);
    /** Deliberately slow handler: with no work in flight at the moment of exit, the scenario would not test the drain at all. */
    private static final Duration HANDLER_WORK = Duration.ofMillis(60);
    /** Minimum ownership before killing the node: leaving empty-handed would measure a two-node cluster and call it churn. */
    private static final int IN_FLIGHT_BEFORE_EXIT = 20;
    /** The grace between drain and stop: the experiment's independent variable, not lazy synchronisation. */
    private static final Duration SETTLE_BEFORE_STOP = Duration.ofSeconds(1);
    /**
     * One retry beyond the first attempt. It is what separates the two possible readings of the
     * finding: with a budget, reclaiming an orphan becomes a synthetic attempt plus a rebirth in
     * the queue (BOUNDED redelivery — the at-least-once contract this scenario claims to measure);
     * without a budget, the SAME mechanism becomes a terminal FAILED by configuration, and the
     * bench would call "loss" what the product calls "redelivery". The {@code retries=0} edge has
     * its own test below.
     */
    private static final int RETRIES = 1;

    @Test
    void aNodeLeavingAndAnotherJoiningMidDrainLosesNothing() {
        ScenarioBackend backend = ScenarioBackend.current();
        DataSource dataSource = backend.freshSchema();
        Clock clock = Clock.systemUTC();
        Map<String, Integer> invocationsPerExecution = new ConcurrentHashMap<>();
        AtomicInteger invocations = new AtomicInteger();
        JobHandler handler = (_, ctx) -> {
            invocations.incrementAndGet();
            invocationsPerExecution.merge(ctx.executionId().value(), 1, Integer::sum);
            Thread.sleep(HANDLER_WORK);
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock, backend.dialect())) {
            cluster.defineJob("churn", spec -> spec.retries(RETRIES));
            for (int i = 0; i < 3; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("churn", handler);

            cluster.seedReady("churn", SEED, 20);
            cluster.startAll();

            // The node must leave with work IN HAND: wait for ownership to fill up
            ScenarioCluster.awaitUntil(Duration.ofSeconds(30),
                    () -> cluster.countLease() >= IN_FLIGHT_BEFORE_EXIT);
            int inFlightAtExit = cluster.countLease();

            // Per-NODE ownership taken before the exit: the re-execution bound is about what the
            // DEPARTING node held, and a global countLease() is roughly 3x looser than the message
            // announces. Which rows were its own is only known afterwards — the departed node is
            // the only STOPPED one.
            Map<String, Integer> leasesByNodeAtExit = leasesByNode(cluster);
            ScenarioCluster.Node leaving = cluster.nodes().get(2);
            long exitAt = System.nanoTime();
            leaving.engine().stop(Duration.ofSeconds(20));
            long exitNanos = System.nanoTime() - exitAt;
            String leftNodeId = stoppedNodeId(cluster);
            int heldByLeavingNode = leasesByNodeAtExit.getOrDefault(leftNodeId, 0);

            ScenarioCluster.Node joining = cluster.addNode(settings(), List.of());
            joining.handlers().register(JobKey.of("churn"), handler);
            joining.engine().start();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);

            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");
            long reExecuted = invocationsPerExecution.values().stream().filter(count -> count > 1).count();

            report(settled, inFlightAtExit, exitNanos, invocations.get(), invocationsPerExecution.size(), succeeded,
                    failed, reExecuted, cluster.failureKinds());
            if (failed > 0) {
                dumpLostWork(cluster);
            }

            assertThat(settled).as("the leaving node's shards must be re-claimed — a stalled queue means the assignment did not react").isTrue();
            // With a retry budget, a reclaimed orphan is REBORN in the queue: the synthetic attempt
            // shows up in failureKinds (which is why that is not required to be empty), but the
            // EXECUTION must end well. The cause goes into the message so that a red build arrives
            // already attributed, without anyone having to read stdout.
            assertThat(failed)
                    .as("a graceful exit must not lose work — with retries=%d a reclaimed orphan is redelivered, "
                            + "not failed. Causes seen: %s", RETRIES, cluster.failureKinds())
                    .isZero();
            assertThat(succeeded).as("every seeded execution must end SUCCEEDED").isEqualTo(SEED);
            assertThat(reExecuted)
                    .as("re-execution must be bounded by what the LEAVING node held (%d) — that is the at-least-once "
                            + "at-least-once contract; a graceful drain should leave zero", heldByLeavingNode)
                    .isLessThanOrEqualTo(heldByLeavingNode);
        }
    }

    /**
     * The contrast arm. The ONLY variable that changes relative to the test above is {@code drain}
     * plus the grace before {@code stop} — the new node joins the same way, at the same point.
     * Without that discipline the pair could not tell the {@code stop} hypothesis apart from the
     * nascent-node hypothesis (the same time-of-check/time-of-use window {@link ColdStartScenario}
     * hunts: {@code findAll} and {@code findOrphaned} are separate reads within one tick).
     *
     * <p>The hypothesis: {@code stop} calls {@code drain}, which waits for whatever is in
     * {@code inFlight} AT THAT INSTANT; the batch the current tick already claimed but has not yet
     * submitted arrives later ({@code Engine} registers it in {@code inFlight} AFTER the
     * {@code runAsync}), and the final heartbeat — which expires the lease by design — declares the
     * node dead with that batch still running. Giving the node time in {@code DRAINING}, where the
     * tick claims nothing further, should drive the loss to zero.
     */
    @Test
    void drainingBeforeStoppingLosesNothing() throws Exception {
        ScenarioBackend backend = ScenarioBackend.current();
        DataSource dataSource = backend.freshSchema();
        Clock clock = Clock.systemUTC();
        AtomicInteger invocations = new AtomicInteger();
        JobHandler handler = (_, _) -> {
            invocations.incrementAndGet();
            Thread.sleep(HANDLER_WORK);
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, clock, backend.dialect())) {
            cluster.defineJob("churn", spec -> spec.retries(RETRIES));
            for (int i = 0; i < 3; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("churn", handler);
            cluster.seedReady("churn", SEED, 20);
            cluster.startAll();

            ScenarioCluster.awaitUntil(Duration.ofSeconds(30),
                    () -> cluster.countLease() >= IN_FLIGHT_BEFORE_EXIT);
            ScenarioCluster.Node leaving = cluster.nodes().get(2);
            leaving.engine().drain(Duration.ofSeconds(20));
            Thread.sleep(SETTLE_BEFORE_STOP.toMillis()); // the slack the hypothesis says is missing inside the stop
            leaving.engine().stop(Duration.ofSeconds(20));

            ScenarioCluster.Node joining = cluster.addNode(settings(), List.of());
            joining.handlers().register(JobKey.of("churn"), handler);
            joining.engine().start();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int succeeded = cluster.countTerminal("SUCCEEDED");
            int failed = cluster.countTerminal("FAILED");

            System.out.printf("""

                    === Node churn — drain, settle, then stop (seed %d) ===
                    queue settled        : %s
                    handler invocations  : %d
                    terminal SUCCEEDED   : %d
                    terminal FAILED      : %d
                    failure kinds        : %s
                    """, SEED, settled ? "yes" : "NO", invocations.get(), succeeded, failed, cluster.failureKinds());

            assertThat(settled).as("the queue must drain — a stall is a different bug from a loss").isTrue();
            assertThat(failed).as("a drain that is allowed to settle must lose nothing").isZero();
            assertThat(succeeded).isEqualTo(SEED);
        }
    }

    /**
     * The worst case for {@code stop(grace)}: {@code retries = 0} — the choice of anyone who
     * prefers at-most-once to the risk of redelivery — combined with a node leaving with work in
     * hand. With no budget, reclaiming the orphan has nowhere to reschedule and becomes a terminal
     * {@code FAILED}: in this configuration, what would be redelivery is loss.
     *
     * <p>The assertion is a CEILING, not a floor, and that is deliberate. Once the second wait in
     * {@code Engine.stop} closed the window between {@code runAsync} and {@code inFlight.add}, the
     * expected value is precisely ZERO — and that is what it now measures. The scenario stopped
     * being a "demonstration of the loss" and became its GUARD: if the window reopens, this is
     * where the damage shows up first, bounded by what the node was holding. A floor
     * ({@code failed > 0}) would turn a fix in the product into a red test.
     */
    @Test
    void aGracefulStopWithNoRetryBudgetLosesAtMostWhatTheNodeHeld() {
        ScenarioBackend backend = ScenarioBackend.current();
        DataSource dataSource = backend.freshSchema();
        Map<String, Integer> invocationsPerExecution = new ConcurrentHashMap<>();
        AtomicInteger invocations = new AtomicInteger();
        // Handler IDENTICAL to the first test's, map merge included: the two may differ only in
        // `retries`, otherwise the pair isolates nothing — and merging per execution changes the
        // time spent inside the handler, which is exactly the window the experiment observes
        JobHandler handler = (_, ctx) -> {
            invocations.incrementAndGet();
            invocationsPerExecution.merge(ctx.executionId().value(), 1, Integer::sum);
            Thread.sleep(HANDLER_WORK);
        };

        try (ScenarioCluster cluster = new ScenarioCluster(dataSource, Clock.systemUTC(), backend.dialect())) {
            // Zero budget DECLARED: it is this pair's independent variable — inheriting it from
            // the product default would make both tests measure the same thing the day that
            // default changed
            cluster.defineJob("churn", spec -> spec.retries(0));
            for (int i = 0; i < 3; i++) {
                cluster.addNode(settings(), List.of());
            }
            cluster.registerEverywhere("churn", handler);
            cluster.seedReady("churn", SEED, 20);
            cluster.startAll();

            ScenarioCluster.awaitUntil(Duration.ofSeconds(30),
                    () -> cluster.countLease() >= IN_FLIGHT_BEFORE_EXIT);
            Map<String, Integer> leasesByNodeAtExit = leasesByNode(cluster);
            cluster.nodes().get(2).engine().stop(Duration.ofSeconds(20));
            int heldByLeavingNode = leasesByNodeAtExit.getOrDefault(stoppedNodeId(cluster), 0);

            ScenarioCluster.Node joining = cluster.addNode(settings(), List.of());
            joining.handlers().register(JobKey.of("churn"), handler);
            joining.engine().start();

            boolean settled = ScenarioCluster.awaitUntil(OBSERVATION, cluster::isDrained);
            int failed = cluster.countTerminal("FAILED");
            int succeeded = cluster.countTerminal("SUCCEEDED");

            System.out.printf("""

                    === Node churn — graceful stop with retries=0 (seed %d) ===
                    queue settled        : %s
                    handler invocations  : %d
                    held by leaving node : %d
                    terminal SUCCEEDED   : %d
                    terminal FAILED      : %d   <- work lost, bounded by what the node held
                    failure kinds        : %s
                    """, SEED, settled ? "yes" : "NO", invocations.get(), heldByLeavingNode, succeeded, failed,
                    cluster.failureKinds());

            assertThat(settled).as("the queue must drain even when the exit costs work").isTrue();
            // Conservation BEFORE the ceiling: `isDrained` only looks at an empty queue and empty
            // ownership, and the ceiling alone is satisfied both by "nothing was lost" and by
            // "8,000 vanished without ever becoming terminal". Without this line the accepted
            // interval is [0, 32] over a universe of unknown size.
            assertThat(succeeded + failed)
                    .as("conservation: every seeded execution must reach a terminal state — one that leaves "
                            + "mohs_ready without one is a loss the FAILED ceiling cannot see")
                    .isEqualTo(SEED);
            assertThat(failed)
                    .as("KNOWN EDGE: with retries=0 a reclaimed orphan has nowhere to be rescheduled and becomes a "
                            + "terminal FAILED. The damage must stay bounded by what the leaving node held (%d) — "
                            + "more than that means the loss is not the shutdown window", heldByLeavingNode)
                    .isLessThanOrEqualTo(heldByLeavingNode);
        }
    }

    /** How many leases each node holds RIGHT NOW — the snapshot the re-execution bound uses. */
    private static Map<String, Integer> leasesByNode(ScenarioCluster cluster) {
        return cluster.jdbc().query("SELECT node_id, count(*) AS held FROM mohs_lease GROUP BY node_id", rs -> {
            Map<String, Integer> held = new java.util.LinkedHashMap<>();
            while (rs.next()) {
                held.put(rs.getString("node_id"), rs.getInt("held"));
            }
            return held;
        });
    }

    /**
     * The departed node is the only {@code STOPPED} one, which is how the scenario identifies it
     * without reaching into the Engine's {@code nodeId}.
     *
     * <p>Zero rows is not a mystery: it is a race accepted on purpose (a tick that read the state
     * before {@code state.set(STOPPED)} commits its heartbeat AFTER the final one and overwrites
     * the row). Naming the race here keeps it from turning into distrust of the whole scenario.
     */
    private static String stoppedNodeId(ScenarioCluster cluster) {
        List<String> stopped = cluster.jdbc().queryForList(
                "SELECT node_id FROM mohs_nodes WHERE state = 'STOPPED'", String.class);
        assertThat(stopped)
                .as("expected exactly one STOPPED row; zero means the in-flight tick's heartbeat overwrote the final "
                        + "one (an accepted race), and the re-execution bound has no baseline")
                .hasSize(1);
        return stopped.getFirst();
    }

    private static EngineSettings settings() {
        return new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(200), 64, 32, 3,
                Duration.ofSeconds(30), Duration.ofSeconds(15), null, Duration.ofSeconds(60));
    }

    /** What is left when something was lost: who owned it, and the full history of a few dead executions. */
    private static void dumpLostWork(ScenarioCluster cluster) {
        System.out.println("nodes at the end (the leaving one is the STOPPED row):");
        printRows(cluster, "SELECT node_id, state, last_heartbeat_at, expires_at FROM mohs_nodes");
        System.out.println("who owned the lost work:");
        printRows(cluster, """
                SELECT a.node_id, count(*) AS lost
                  FROM mohs_execution e JOIN mohs_attempt a ON a.execution_id = e.execution_id
                 WHERE e.state = 'FAILED'
                 GROUP BY a.node_id
                """);
        System.out.println("failed executions (up to 5, with every attempt):");
        printRows(cluster, """
                SELECT e.execution_id, e.state, e.scheduled_at, a.number, a.outcome, a.error_type, a.error,
                       a.started_at, a.finished_at, a.node_id
                  FROM mohs_execution e
                  JOIN mohs_attempt a ON a.execution_id = e.execution_id
                 WHERE e.state = 'FAILED'
                   AND e.execution_id IN (SELECT execution_id FROM mohs_execution WHERE state = 'FAILED' LIMIT 5)
                 ORDER BY e.execution_id, a.number
                """);
    }

    private static void printRows(ScenarioCluster cluster, String sql) {
        cluster.jdbc().queryForList(sql).forEach(row -> System.out.println("  " + row));
    }

    private static void report(boolean settled, int inFlightAtExit, long exitNanos, int invocations, int distinct,
            int succeeded, int failed, long reExecuted, Map<String, Integer> failureKinds) {
        System.out.printf("""

                === Node churn — one node leaves gracefully, one joins mid-drain (seed %d) ===
                queue settled        : %s
                in flight at exit    : %d
                graceful stop took   : %.2fs
                handler invocations  : %d over %d distinct executions
                re-executed          : %d
                terminal SUCCEEDED   : %d
                terminal FAILED      : %d
                failure kinds        : %s
                """, SEED, settled ? "yes" : "NO — still draining at the timeout", inFlightAtExit, exitNanos / 1e9,
                invocations, distinct, reExecuted, succeeded, failed, failureKinds);
    }
}
