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
 * {@code (node_id, epoch, attempt_number)} fence is the real one.
 */
class CompletionBatcherTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration LONG_INTERVAL = Duration.ofMinutes(5);
    private static final String NODE = "01991337-0000-7000-8000-000000000065";
    private final java.util.Map<String, String> claimTokens = new java.util.HashMap<>();

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
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, new H2JdbcDelegate());
        leaseStore = new JdbcLeaseStore(dataSource, new H2JdbcDelegate());
        workQueue = new JdbcWorkQueue(dataSource, new H2JdbcDelegate());
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new H2JdbcDelegate(), NODE, io.mohs.core.EngineState.RUNNING, Duration.ofMinutes(5));
        jobStore.upsert(JobDefinition.of("welcome-email", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
    }

    @AfterEach
    void tearDown() {
        if (batcher != null) {
            batcher.close();
        }
    }

    /** Creates and starts the batcher in the field — {@code tearDown} closes whatever the test did not. */
    private void startBatcher(LeaseStore store, int flushSize, Duration flushInterval) {
        batcher = new CompletionBatcher(store, flushSize, flushInterval);
        batcher.start();
    }

    /** History, queue and claim — the ownership is born through the real path, with the (NODE, EPOCH) fence. */
    private void seedLeasedExecution(String id) {
        new JdbcTemplate(dataSource).update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES (?, 'welcome-email', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, id, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        io.mohs.store.jdbc.TestWork.ready(dataSource, new H2JdbcDelegate(), List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("welcome-email"), 0, 20, 1, NOW.minusSeconds(1))));
        workQueue.claim(0, NODE, 10, List.of(), NOW).forEach(work -> claimTokens.put(work.executionId().value(), work.claimToken()));
    }

    private LeaseStore.CompletionResult successResult(String id) {
        return new LeaseStore.CompletionResult(ExecutionId.of(id), JobKey.of("welcome-email"), NODE, claimTokens.get(id), 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
    }

    @Test
    void anOldCompletionCannotClearANewerGrantsTransitMarker() throws Exception {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        var first = successResult("01991337-0000-7000-8000-00000000001c");
        var second = new LeaseStore.CompletionResult(first.executionId(), first.jobKey(), NODE, "01991337-0000-7000-8000-000000000060",
                1, NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
        var firstEntered = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var secondEntered = new CountDownLatch(1);
        var releaseSecond = new CountDownLatch(1);
        var delivered = new CountDownLatch(2);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        LeaseStore controlled = mock(LeaseStore.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            if (calls.incrementAndGet() == 1) {
                firstEntered.countDown();
                assertThat(releaseFirst.await(5, TimeUnit.SECONDS)).isTrue();
            } else {
                secondEntered.countDown();
                assertThat(releaseSecond.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return java.util.Map.of(first.executionId(), LeaseStore.Completion.FENCED_OUT);
        }).when(controlled).complete(any());
        startBatcher(controlled, 1, LONG_INTERVAL);
        try {
            batcher.submit(first, _ -> delivered.countDown());
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            batcher.submit(second, _ -> delivered.countDown());
            releaseFirst.countDown();
            assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(batcher.completionInTransit(first.executionId(), first.claimToken())).isFalse();
            assertThat(batcher.completionInTransit(second.executionId(), second.claimToken())).isTrue();
            releaseSecond.countDown();
            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(batcher.completionInTransit(second.executionId(), second.claimToken())).isFalse();
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
        }
    }

    @Test
    void twoGrantsForOneExecutionReceiveTheirOwnVerdicts() throws Exception {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        var old = successResult("01991337-0000-7000-8000-00000000001c");
        var current = new LeaseStore.CompletionResult(old.executionId(), old.jobKey(), NODE, "01991337-0000-7000-8000-000000000018",
                1, NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
        new JdbcTemplate(dataSource).update("UPDATE mohs_work SET claim_token = '01991337-0000-7000-8000-000000000018' WHERE execution_id = '01991337-0000-7000-8000-00000000001c'");
        var oldVerdict = new java.util.concurrent.CompletableFuture<LeaseStore.Completion>();
        var currentVerdict = new java.util.concurrent.CompletableFuture<LeaseStore.Completion>();
        startBatcher(leaseStore, 2, LONG_INTERVAL);
        batcher.submit(old, oldVerdict::complete);
        batcher.submit(current, currentVerdict::complete);
        assertThat(oldVerdict.get(5, TimeUnit.SECONDS)).isEqualTo(LeaseStore.Completion.FENCED_OUT);
        assertThat(currentVerdict.get(5, TimeUnit.SECONDS).owned()).isTrue();
        assertThat(stateOf("01991337-0000-7000-8000-00000000001c")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, id));
    }

    /** The N trigger: the interval is deliberately long — if the flush depended on it, the await would time out. */
    @Test
    void flushesWhenTheBatchFillsBeforeTheInterval() throws Exception {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        seedLeasedExecution("01991337-0000-7000-8000-00000000001e");
        startBatcher(leaseStore, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successResult("01991337-0000-7000-8000-00000000001c"), completion -> delivered.countDown());
        batcher.submit(successResult("01991337-0000-7000-8000-00000000001e"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("01991337-0000-7000-8000-00000000001c")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("01991337-0000-7000-8000-00000000001e")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** The T trigger: a batch far from full — the result becomes durable within the interval rather than waiting for neighbours. */
    @Test
    void flushesOnTheIntervalWhenTheBatchDoesNotFill() throws Exception {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        startBatcher(leaseStore, 100, Duration.ofMillis(50));
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("01991337-0000-7000-8000-00000000001c"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("01991337-0000-7000-8000-00000000001c")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** The fence's verdict crosses the batch: whoever already lost ownership (a reaper or requeue got there first) receives FENCED_OUT, never silence. */
    @Test
    void deliversFencedOutWhenTheIncarnationWasLost() throws Exception {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        // Ownership changed hands: a reaper dropped the lease and another node re-claimed it
        new JdbcTemplate(dataSource).update("UPDATE mohs_work SET node_id = '01991337-0000-7000-8000-000000000067', claim_token = '01991337-0001-7000-8000-000000000097' WHERE execution_id = '01991337-0000-7000-8000-00000000001c'");
        startBatcher(leaseStore, 1, LONG_INTERVAL);
        ConcurrentLinkedQueue<LeaseStore.Completion> outcomes = new ConcurrentLinkedQueue<>();
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("01991337-0000-7000-8000-00000000001c"), completion -> {
            outcomes.add(completion);
            delivered.countDown();
        });

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(outcomes).containsExactly(LeaseStore.Completion.FENCED_OUT);
        // The loser wrote nothing: the advisory intact, the new incarnation's lease standing
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("PENDING");
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT node_id FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("01991337-0000-7000-8000-000000000067");
    }

    /** A transient error retries the same fenced group once. */
    @Test
    void retriesTheWholeGroupWhenTheFirstFlushFails() throws Exception {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        seedLeasedExecution("01991337-0000-7000-8000-00000000001e");
        LeaseStore failingOnce = mock(LeaseStore.class, delegatesTo(leaseStore));
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        org.mockito.Mockito.doAnswer(call -> {
            if (calls.incrementAndGet() == 1) throw new org.springframework.dao.DataAccessResourceFailureException("simulated flush failure");
            return leaseStore.complete(call.getArgument(0));
        }).when(failingOnce).complete(argThatIsBatchOfTwo());
        startBatcher(failingOnce, 2, LONG_INTERVAL);
        CountDownLatch delivered = new CountDownLatch(2);

        batcher.submit(successResult("01991337-0000-7000-8000-00000000001c"), completion -> delivered.countDown());
        batcher.submit(successResult("01991337-0000-7000-8000-00000000001e"), completion -> delivered.countDown());

        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(stateOf("01991337-0000-7000-8000-00000000001c")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("01991337-0000-7000-8000-00000000001e")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private static List<LeaseStore.CompletionResult> argThatIsBatchOfTwo() {
        return org.mockito.ArgumentMatchers.argThat(list -> list != null && list.size() == 2);
    }

    /** The reconcile's state-based guard: an id stays in transit between the submit and the verdict, and disappears on EVERY outcome. */
    @Test
    void completionInTransitTracksSubmitToOutcome() throws Exception {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        startBatcher(leaseStore, 100, LONG_INTERVAL); // a batch far from full: only the close flushes
        CountDownLatch delivered = new CountDownLatch(1);

        batcher.submit(successResult("01991337-0000-7000-8000-00000000001c"), completion -> delivered.countDown());
        assertThat(batcher.completionInTransit(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), claimTokens.get("01991337-0000-7000-8000-00000000001c"))).isTrue();

        batcher.close();
        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(batcher.completionInTransit(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), claimTokens.get("01991337-0000-7000-8000-00000000001c"))).isFalse();
    }

    /** The close is the shutdown's drain: whatever was in the queue becomes durable before it returns. */
    @Test
    void closeDrainsWhatIsStillQueued() {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        startBatcher(leaseStore, 100, LONG_INTERVAL);
        AtomicBoolean deliveredBeforeCloseReturned = new AtomicBoolean();

        batcher.submit(successResult("01991337-0000-7000-8000-00000000001c"), completion -> deliveredBeforeCloseReturned.set(true));
        batcher.close();

        assertThat(deliveredBeforeCloseReturned).isTrue();
        assertThat(stateOf("01991337-0000-7000-8000-00000000001c")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** Shutdown ends local acceptance of results; peers recover the unresolved grant. */
    @Test
    void submitAfterCloseLeavesTheGrantForRecovery() {
        seedLeasedExecution("01991337-0000-7000-8000-00000000001c");
        startBatcher(leaseStore, 100, LONG_INTERVAL);
        batcher.close();
        List<LeaseStore.Completion> outcomes = new ArrayList<>();

        batcher.submit(successResult("01991337-0000-7000-8000-00000000001c"), outcomes::add);

        assertThat(outcomes).isEmpty();
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("PENDING");
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = '01991337-0000-7000-8000-00000000001c'", Long.class)).isEqualTo(1L);
    }
}
