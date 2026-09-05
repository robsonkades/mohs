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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.MohsRunner;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcHistoryStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcNodeStore;
import io.mohs.store.jdbc.JdbcRateLimitStore;
import io.mohs.store.jdbc.JdbcTriggerFirer;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Two engines over ONE database — the cluster contract the single-engine suite cannot state: work
 * is claimed by exactly one node (Competing Consumers, EIP), and a node that stops heartbeating has
 * its executions reclaimed by a peer and retried there (a lease is failure detection, DDIA). The
 * engines share the stores, the handler registry and the clock; each has its own node id,
 * dispatcher and heartbeat path — which is what a real cluster shares and does not share.
 */
class ClusterEngineTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final int BATCH_SIZE = 10;
    private static final JobKey WELCOME = JobKey.of("welcome-email");

    private final H2JdbcDelegate delegate = new H2JdbcDelegate();

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcJobStore jobStore;
    private JdbcHistoryStore historyStore;
    private JdbcWorkQueue workQueue;
    private JdbcLeaseStore leaseStore;
    private JdbcRateLimitStore rateLimitStore;
    private JdbcNodeStore nodeStore;
    private HandlerRegistry handlerRegistry;

    @BeforeEach
    void setUp() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:cluster-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        dataSource = h2;
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        jobStore = new JdbcJobStore(dataSource, clock, delegate);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock, delegate);
        historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), delegate);
        workQueue = new JdbcWorkQueue(dataSource, delegate, batchStore);
        leaseStore = new JdbcLeaseStore(dataSource, delegate, batchStore);
        rateLimitStore = new JdbcRateLimitStore(dataSource, clock, delegate);
        nodeStore = new JdbcNodeStore(dataSource, delegate);
        handlerRegistry = new HandlerRegistry();
    }

    /** One node of the cluster: its own id, dispatcher and heartbeat path over the shared stores. */
    private Engine node(NodeStore heartbeatPath, List<ExecutionListener> listeners) {
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        EngineMetrics metrics = new EngineMetrics(new SimpleMeterRegistry());
        Dispatcher dispatcher = new Dispatcher(leaseStore, jobStore, handlerRegistry, clock, List.of(), listeners, eventExecutor, metrics);
        RunnerRegistry runners = new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build()));
        return new Engine(workQueue, dispatcher, historyStore, leaseStore, jobStore, heartbeatPath,
                new JdbcTriggerFirer(dataSource, historyStore, workQueue, delegate),
                new ExecutionWindowRegistry(List.of()), rateLimitStore, clock,
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL), runners, metrics);
    }

    private void offer(String id) {
        int shard = Shards.of(ExecutionId.of(id));
        historyStore.record(List.of(new HistoryStore.NewExecution(ExecutionId.of(id), WELCOME, shard, 20,
                NOW.minusSeconds(1), NOW, "test", null, null, id)));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), WELCOME, shard, 20, 1, NOW.minusSeconds(1))));
    }

    /** Observe each engine's own membership read before offering work, avoiding a startup overlap. */
    private NodeStore observingMembership(CompletableFuture<Void> joined) {
        NodeStore observing = mock(NodeStore.class, delegatesTo(nodeStore));
        doAnswer(_ -> {
            List<StoredNode> nodes = nodeStore.findAll();
            if (nodes.size() == 2 && nodes.stream().allMatch(node -> node.state() == EngineState.RUNNING)) {
                joined.complete(null);
            }
            return nodes;
        }).when(observing).findAll();
        return observing;
    }

    private static ExecutionListener countDownOnSucceeded(CountDownLatch succeeded) {
        return event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
    }

    /** The heartbeat path of a node that lost its network: every heartbeat fails, and with it the whole tick — the node neither claims nor reaps while it lasts. */
    private static final class PartitionableNodeStore implements NodeStore {
        private final NodeStore delegate;
        private volatile boolean partitioned;

        PartitionableNodeStore(NodeStore delegate) {
            this.delegate = delegate;
        }

        void partition() {
            partitioned = true;
        }

        @Override
        public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
            if (partitioned) {
                throw new IllegalStateException("simulated network partition: the heartbeat never reaches the database");
            }
            delegate.heartbeat(nodeId, state, epoch, at, expiresAt);
        }

        @Override
        public List<StoredNode> findAll() {
            return delegate.findAll();
        }

        @Override
        public int deleteHeartbeatsBefore(Instant cutoff) {
            return delegate.deleteHeartbeatsBefore(cutoff);
        }
    }

    /**
     * Competing Consumers: one execution per shard after both nodes see the cluster. Both nodes must
     * finish work, and every execution has exactly one attempt row and one handler call.
     */
    @Test
    void everyExecutionRunsExactlyOnceAcrossTwoLiveNodes() throws Exception {
        jobStore.upsert(JobDefinition.of(WELCOME.value(), Handler.class, spec -> spec.onDemand().retries(0)));
        Map<Integer, String> executionByShard = new HashMap<>();
        for (int i = 1; i <= 10_000 && executionByShard.size() < Shards.SHARD_COUNT; i++) {
            String id = "exec-" + i;
            executionByShard.putIfAbsent(Shards.of(ExecutionId.of(id)), id);
        }
        assertThat(executionByShard).hasSize(Shards.SHARD_COUNT);
        int executions = executionByShard.size();
        Map<String, Integer> handlerCalls = new ConcurrentHashMap<>();
        handlerRegistry.register(WELCOME, (payload, ctx) -> handlerCalls.merge((String) payload, 1, Integer::sum));
        CountDownLatch succeeded = new CountDownLatch(executions);
        CompletableFuture<Void> joinedA = new CompletableFuture<>();
        CompletableFuture<Void> joinedB = new CompletableFuture<>();
        Engine nodeA = node(observingMembership(joinedA), List.of(countDownOnSucceeded(succeeded)));
        Engine nodeB = node(observingMembership(joinedB), List.of(countDownOnSucceeded(succeeded)));

        nodeA.start();
        nodeB.start();
        try {
            CompletableFuture.allOf(joinedA, joinedB).get(10, TimeUnit.SECONDS);
            executionByShard.values().forEach(this::offer);
            assertThat(succeeded.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            nodeA.stop(Duration.ofSeconds(5));
            nodeB.stop(Duration.ofSeconds(5));
        }

        assertThat(handlerCalls).hasSize(executions);
        assertThat(handlerCalls.values()).containsOnly(1);
        assertThat(rawJdbcTemplate.queryForList(
                "SELECT execution_id FROM mohs_attempt GROUP BY execution_id HAVING COUNT(*) <> 1", String.class))
                .as("an execution with more or less than one attempt").isEmpty();
        assertThat(rawJdbcTemplate.queryForList("SELECT DISTINCT node_id FROM mohs_attempt", String.class))
                .containsExactlyInAnyOrder(nodeA.nodeId(), nodeB.nodeId());
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_execution WHERE state = 'SUCCEEDED'", Integer.class))
                .isEqualTo(executions);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isZero();
    }

    /**
     * Failure detection, then the fence. Node A claims and starts a handler, then loses its network —
     * its heartbeat stops reaching the database while the handler is still running. Once A's lease
     * expires, node B reclaims the execution (attempt 1 fails as the node presumed dead) and retries
     * it (the budget allows one). While B's attempt 2 is RUNNING, A's handler finally returns: its
     * completion carries {@code (A, epoch, attempt 1)}, the lease now says {@code (B, attempt 2)},
     * and the fenced delete matches nothing — the zombie writes no attempt and touches no lease. Only
     * then does B finish. A zombie released after B had already completed would prove nothing: the
     * lease would be gone by any predicate.
     */
    @Test
    void aPeerReclaimsAndRetriesTheExecutionOfANodeThatStoppedHeartbeatingAndFencesItsZombie() throws Exception {
        jobStore.upsert(JobDefinition.of(WELCOME.value(), Handler.class, spec -> spec.onDemand().retries(1)));
        offer("exec-1");
        AtomicInteger handlerCalls = new AtomicInteger();
        CountDownLatch firstAttemptStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstAttempt = new CountDownLatch(1);
        CountDownLatch secondAttemptStarted = new CountDownLatch(1);
        CountDownLatch releaseSecondAttempt = new CountDownLatch(1);
        handlerRegistry.register(WELCOME, (payload, ctx) -> {
            switch (handlerCalls.incrementAndGet()) {
                case 1 -> {
                    firstAttemptStarted.countDown();
                    releaseFirstAttempt.await(10, TimeUnit.SECONDS);
                }
                case 2 -> {
                    secondAttemptStarted.countDown();
                    releaseSecondAttempt.await(10, TimeUnit.SECONDS);
                }
                default -> throw new AssertionError("a third attempt ran");
            }
        });
        PartitionableNodeStore heartbeatOfA = new PartitionableNodeStore(nodeStore);
        CompletableFuture<Instant> retryAtOnB = new CompletableFuture<>();
        CountDownLatch succeededOnB = new CountDownLatch(1);
        ExecutionListener recordRetryAt = event -> {
            if (event instanceof RetryScheduled scheduled) {
                retryAtOnB.complete(scheduled.retryAt());
            }
        };
        Engine nodeA = node(heartbeatOfA, List.of());
        Engine nodeB = node(nodeStore, List.of(recordRetryAt, countDownOnSucceeded(succeededOnB)));

        nodeA.start();
        try {
            assertThat(firstAttemptStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // A loses its network while its handler runs; the shared clock then passes A's lease
            heartbeatOfA.partition();
            clock.advance(LEASE_TTL.plusSeconds(1));
            nodeB.start();
            try {
                // The reclaim requeues attempt 2 behind the retry backoff; on a frozen clock that
                // instant never arrives by itself
                clock.setTo(retryAtOnB.get(10, TimeUnit.SECONDS).plusSeconds(1));
                assertThat(secondAttemptStarted.await(10, TimeUnit.SECONDS)).isTrue();
                // The zombie finishes while B owns attempt 2; stop returns only after A's dispatch —
                // and its fenced delete — ran
                releaseFirstAttempt.countDown();
                nodeA.stop(Duration.ofSeconds(5));
                List<Map<String, Object>> leases = rawJdbcTemplate.queryForList(
                        "SELECT node_id, attempt_number FROM mohs_lease WHERE execution_id = 'exec-1'");
                assertThat(leases).as("B's lease survives the zombie's completion").singleElement()
                        .satisfies(lease -> assertThat(lease).containsEntry("NODE_ID", nodeB.nodeId()).containsEntry("ATTEMPT_NUMBER", 2));
                assertThat(rawJdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM mohs_attempt WHERE execution_id = 'exec-1' AND outcome = 'SUCCEEDED'", Integer.class))
                        .as("the zombie's SUCCEEDED must not have been written").isZero();
                releaseSecondAttempt.countDown();
                assertThat(succeededOnB.await(10, TimeUnit.SECONDS)).isTrue();
            } finally {
                releaseSecondAttempt.countDown();
                nodeB.stop(Duration.ofSeconds(5));
            }
        } finally {
            releaseFirstAttempt.countDown();
            if (nodeA.state() != EngineState.STOPPED) {
                nodeA.stop(Duration.ofSeconds(5));
            }
        }

        assertThat(handlerCalls).hasValue(2);
        assertThat(historyStore.find(ExecutionId.of("exec-1"), clock.instant()).orElseThrow().state())
                .isEqualTo(ExecutionState.SUCCEEDED);
        List<Map<String, Object>> attempts = rawJdbcTemplate.queryForList(
                "SELECT number, node_id, outcome, error FROM mohs_attempt WHERE execution_id = 'exec-1' ORDER BY number");
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0)).containsEntry("NUMBER", 1).containsEntry("NODE_ID", nodeA.nodeId()).containsEntry("OUTCOME", "FAILED");
        assertThat((String) attempts.get(0).get("ERROR")).contains("presumed dead");
        assertThat(attempts.get(1)).containsEntry("NUMBER", 2).containsEntry("NODE_ID", nodeB.nodeId()).containsEntry("OUTCOME", "SUCCEEDED");
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isZero();
    }
}
