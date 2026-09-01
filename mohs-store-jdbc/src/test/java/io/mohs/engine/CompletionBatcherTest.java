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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcTimestamps;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * The completion's group commit against the real store (H2) — each flush trigger, the fallback and the
 * close's drain are observable behaviour; synchronised through latches and timeouts, never sleeps.
 *
 * <p>The ownership is born through the real path (queue, then claim), so the results'
 * {@code (node_id, epoch)} fence is the real one.
 */
class CompletionBatcherTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration LONG_INTERVAL = Duration.ofMinutes(5);
    private static final String NODE = "node-a";
    private static final long EPOCH = 1;

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcJobStore jobStore;
    private JdbcLeaseStore leaseStore;
    private JdbcWorkQueue workQueue;
    private CompletionBatcher batcher;

    @BeforeEach
    void setUp() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:completion-batcher-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        dataSource = h2;
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        jobStore = new JdbcJobStore(dataSource, clock, new H2JdbcDelegate());
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock, new H2JdbcDelegate());
        leaseStore = new JdbcLeaseStore(dataSource, new H2JdbcDelegate(), batchStore);
        workQueue = new JdbcWorkQueue(dataSource, new H2JdbcDelegate(), batchStore);
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
    }

    @AfterEach
    void tearDown() {
        if (batcher != null) {
            batcher.close();
        }
    }

    /** Creates and starts the batcher in the field — {@code tearDown} closes whatever the test did not. */
    private void startBatcher(LeaseStore store, int flushSize, Duration flushInterval) {
        batcher = new CompletionBatcher(store, jobStore, flushSize, flushInterval);
        batcher.start();
    }

    /** History, queue and claim — the ownership is born through the real path, with the (NODE, EPOCH) fence. */
    private void seedLeasedExecution(String id) {
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES (?, 'welcome-email', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, id, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("welcome-email"), 0, 20, 1, NOW.minusSeconds(1))));
        workQueue.claim(0, NODE, EPOCH, 10, List.of(), NOW);
    }

    private LeaseStore.CompletionResult successResult(String id) {
        return new LeaseStore.CompletionResult(ExecutionId.of(id), JobKey.of("welcome-email"), NODE, EPOCH, 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, ExecutionState.SUCCEEDED, null);
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, id));
    }

    /** The N trigger: the interval is deliberately long — if the flush depended on it, the await would time out. */
    @Test
    void flushesWhenTheBatchFillsBeforeTheInterval() throws Exception {
        seedLeasedExecution("exec-1");
        seedLeasedExecution("exec-2");
        startBatcher(leaseStore, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());
        batcher.submit(successResult("exec-2"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** The T trigger: a batch far from full — the result becomes durable within the interval rather than waiting for neighbours. */
    @Test
    void flushesOnTheIntervalWhenTheBatchDoesNotFill() throws Exception {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, Duration.ofMillis(50));
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** The fence's verdict crosses the batch: whoever already lost ownership (a reaper or requeue got there first) receives FENCED_OUT, never silence. */
    @Test
    void deliversFencedOutWhenTheIncarnationWasLost() throws Exception {
        seedLeasedExecution("exec-1");
        // Ownership changed hands: a reaper dropped the lease and another node re-claimed it
        new JdbcTemplate(dataSource).update("UPDATE mohs_lease SET node_id = 'node-b', epoch = 9 WHERE execution_id = 'exec-1'");
        startBatcher(leaseStore, 1, LONG_INTERVAL);
        ConcurrentLinkedQueue<LeaseStore.Completion> outcomes = new ConcurrentLinkedQueue<>();
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("exec-1"), completion -> {
            outcomes.add(completion);
            delivered.countDown();
        });

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(outcomes).containsExactly(LeaseStore.Completion.FENCED_OUT);
        // The loser wrote nothing: the advisory intact, the new incarnation's lease standing
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("PENDING");
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT node_id FROM mohs_lease WHERE execution_id = 'exec-1'", String.class)).isEqualTo("node-b");
    }

    /** A batch flush failure discards no result: it falls back to individual completion, in the same transaction as ever. */
    @Test
    void fallsBackToPerResultCompletionWhenTheBatchFlushFails() throws Exception {
        seedLeasedExecution("exec-1");
        seedLeasedExecution("exec-2");
        LeaseStore failingOnce = mock(LeaseStore.class, delegatesTo(leaseStore));
        doThrow(new IllegalStateException("simulated flush failure"))
                .when(failingOnce).complete(argThatIsBatchOfTwo(), any());
        startBatcher(failingOnce, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());
        batcher.submit(successResult("exec-2"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private static List<LeaseStore.CompletionResult> argThatIsBatchOfTwo() {
        return org.mockito.ArgumentMatchers.argThat(list -> list != null && list.size() == 2);
    }

    /** The reconcile's state-based guard: an id stays in transit between the submit and the verdict, and disappears on EVERY outcome. */
    @Test
    void completionInTransitTracksSubmitToOutcome() throws Exception {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, LONG_INTERVAL); // a batch far from full: only the close flushes
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("exec-1"), completion -> delivered.countDown());
        assertThat(batcher.completionInTransit(ExecutionId.of("exec-1"))).isTrue();

        batcher.close();
        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(batcher.completionInTransit(ExecutionId.of("exec-1"))).isFalse();
    }

    /** The close is the shutdown's drain: whatever was in the queue becomes durable before it returns. */
    @Test
    void closeDrainsWhatIsStillQueued() {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, LONG_INTERVAL);
        AtomicBoolean deliveredBeforeCloseReturned = new AtomicBoolean();

        batcher.submit(successResult("exec-1"), completion -> deliveredBeforeCloseReturned.set(true));
        batcher.close();

        assertThat(deliveredBeforeCloseReturned).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** A zombie finishing after the shutdown does not lose its result: a submit after the close completes synchronously, on the calling thread. */
    @Test
    void submitAfterCloseCompletesSynchronously() {
        seedLeasedExecution("exec-1");
        startBatcher(leaseStore, 100, LONG_INTERVAL);
        batcher.close();
        List<LeaseStore.Completion> outcomes = new ArrayList<>();

        batcher.submit(successResult("exec-1"), outcomes::add);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0).owned()).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }
}
