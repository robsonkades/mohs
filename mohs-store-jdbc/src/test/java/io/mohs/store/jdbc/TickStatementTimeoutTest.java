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

import io.mohs.engine.SubmissionStore;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The engine's loop thread carries the node lease, so every statement it issues is bounded in time
 * ({@link JdbcSupport#TICK_STATEMENT_TIMEOUT_SECONDS}) — and every statement a host thread issues
 * is not. This test pins WHICH is which by recording the query timeout each prepared statement was
 * given: a tick statement that loses its ceiling, or a host write that gains one, fails here rather
 * than at 3 a.m.
 */
class TickStatementTimeoutTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");
    private static final String NODE = "01991337-0000-7000-8000-000000000065";

    /** One record per prepared statement, in order — the key is the statement, not its SQL text, so two preparations of the same text with different ceilings cannot mask each other. */
    private final List<Prepared> prepared = new ArrayList<>();

    private static final class Prepared {
        final String sql;
        @Nullable Integer timeout;

        Prepared(String sql) {
            this.sql = sql;
        }
    }

    private DataSource recording;
    private JdbcNodeStore nodeStore;
    private String claimToken;
    private JdbcLeaseStore leaseStore;
    private JdbcWorkQueue workQueue;
    private JdbcSubmissionStore triggerFirer;
    private JdbcJobStore jobStore;
    private JdbcHistoryStore historyStore;
    private JdbcBatchStore batchStore;

    @BeforeEach
    void setUp() {
        DataSource h2 = freshH2DataSource();
        recording = recordingDataSource(h2);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        H2JdbcDelegate delegate = new H2JdbcDelegate();
        batchStore = new JdbcBatchStore(recording, delegate);
        nodeStore = new JdbcNodeStore(recording, delegate);
        leaseStore = new JdbcLeaseStore(recording, delegate);
        workQueue = new JdbcWorkQueue(recording, delegate);
        historyStore = new JdbcHistoryStore(recording, delegate);
        triggerFirer = new JdbcSubmissionStore(recording, delegate, java.time.Duration.ofDays(7));
        jobStore = new JdbcJobStore(recording, clock, delegate);
        TestSessions.seed(recording, delegate, NODE, EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        forgetSetupStatements();
    }

    @Test
    void theHeartbeatAndThePeersReadAreBounded() {
        nodeStore.renew(NODE, EngineState.RUNNING, java.time.Duration.ofSeconds(15));
        nodeStore.findAll();

        assertEveryStatementIsBounded();
    }

    @Test
    void theLeaseReadsTheTickIssuesAreBounded() {
        leaseStore.findByNodes(List.of(NODE));
        leaseStore.findOrphaned( 10);
        leaseStore.findCancelRequested(List.of(ExecutionId.of("01991337-0000-7000-8000-00000000001b")));

        assertEveryStatementIsBounded();
    }

    /**
     * The reaper's completion runs on the loop thread too, but its cost is a whole transaction — the
     * fenced delete, the attempt, the terminal update and batch counters — so the ceiling is a
     * transaction deadline, which Spring hands to each statement as the time that remains.
     */
    @Test
    void theReapersCompletionIsBoundedByItsTransactionDeadline() {
        TestWork.batch(recording, new H2JdbcDelegate(), "01991337-0000-7000-8000-00000000000e", "nightly", 1);
        claimOneExecution("01991337-0000-7000-8000-00000000000e");
        nodeStore.stop(NODE);
        forgetSetupStatements();

        leaseStore.reclaim(List.of(new LeaseStore.CompletionResult(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), JobKey.of("job-a"), NODE, claimToken, 1,
                NOW, NOW, ExecutionState.FAILED, "java.lang.IllegalStateException", "reaped", null)));

        // The batch-counter template has no separate ceiling: it inherits this transaction's deadline.
        assertThat(prepared).extracting(statement -> statement.sql)
                .anyMatch(sql -> sql.contains("mohs_batches"));
        assertThat(prepared).allSatisfy(statement -> assertThat(statement.timeout).as(statement.sql)
                .isBetween(1, JdbcSupport.TICK_STATEMENT_TIMEOUT_SECONDS));
    }

    @Test
    void theReapersRecurringRearmInheritsItsTransactionDeadline() {
        var key = JobKey.of("job-a");
        var id = ExecutionId.of("01991337-0000-7000-8000-00000000001b");
        var interval = java.time.Duration.ofSeconds(60);
        jobStore.upsert(JobDefinition.of(key.value(), new io.mohs.core.schedule.IntervalSpec(interval, true),
                io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        var observed = jobStore.find(key).orElseThrow();
        Instant started = observed.nextFireAt();
        assertThat(triggerFirer.fire(key, observed.scheduleRevision(), started, null,
                List.of(io.mohs.core.execution.Execution.enqueued(id, key, started,
                        io.mohs.core.execution.Execution.SCHEDULER_ACTOR, io.mohs.core.execution.Priority.NORMAL)), started)).isTrue();
        var grant = workQueue.claim(io.mohs.engine.Shards.of(id), NODE, 1, List.of(), started).getFirst();
        nodeStore.stop(NODE);
        forgetSetupStatements();

        Instant finished = started.plusSeconds(1);
        leaseStore.reclaim(List.of(new LeaseStore.CompletionResult(id, key, NODE, grant.claimToken(), 1,
                started, finished, ExecutionState.FAILED, "java.lang.IllegalStateException", "reaped", null)));

        assertThat(prepared).extracting(statement -> statement.sql)
                .anyMatch(sql -> sql.contains("e.schedule_revision = d.schedule_revision"))
                .anyMatch(sql -> sql.contains("UPDATE mohs_job_definitions") && sql.contains("next_fire_at"));
        assertThat(prepared).allSatisfy(statement -> assertThat(statement.timeout).as(statement.sql)
                .isBetween(1, JdbcSupport.TICK_STATEMENT_TIMEOUT_SECONDS));
        assertThat(jobStore.find(key).orElseThrow().nextFireAt()).isEqualTo(finished.plus(interval));
    }

    /** The flusher's completion also bounds the entire transaction, including a group commit. */
    @Test
    void theFlushersCompletionAlsoHasATransactionDeadline() {
        claimOneExecution(null);

        leaseStore.complete(List.of(new LeaseStore.CompletionResult(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), JobKey.of("job-a"), NODE, claimToken, 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, null)));

        assertEveryStatementIsBounded();
    }

    @Test
    void theAtomicCancelRequestHasATransactionDeadline() {
        workQueue.cancel(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), NOW);

        assertEveryStatementIsBounded();
    }

    @Test
    void theClaimTheProbeAndTheRequeueAreBounded() {
        jobStore.upsert(JobDefinition.of("job-a", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        TestWork.history(recording, new H2JdbcDelegate(), List.of(new SubmissionStore.NewExecution(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), JobKey.of("job-a"), 0, 20,
                NOW, NOW, "test", null, null, io.mohs.test.InputFixtures.encode(JobKey.of("job-a"), "payload"), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)));
        TestWork.ready(recording, new H2JdbcDelegate(), List.of(new WorkQueue.ReadyEntry(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), JobKey.of("job-a"), 0, 20, 1, NOW)));
        forgetSetupStatements();

        workQueue.hasVisibleWork(List.of(0), NOW);
        List<WorkQueue.ClaimedWork> claimed = workQueue.claim(0, NODE, 10, List.of(), NOW);
        int requeued = workQueue.requeue(List.of(new WorkQueue.Requeue(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), NODE, claimed.getFirst().claimToken(), 1, NOW)));

        assertThat(claimed).hasSize(1);
        assertThat(requeued).isEqualTo(1);
        assertEveryStatementIsBounded();
    }

    @Test
    void completeAcceptanceBoundsEveryWriteByOneDeadline() {
        jobStore.upsert(JobDefinition.of("job-a", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        forgetSetupStatements();

        triggerFirer.accept(List.of(new SubmissionStore.NewExecution(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), JobKey.of("job-a"), 0, 20,
                NOW, NOW, "test", null, "acceptance-key", io.mohs.test.InputFixtures.encode(JobKey.of("job-a"), "payload"), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)), null, () -> {});
        assertEveryStatementIsBounded();
    }

    @Test
    void theFiringCasIsBounded() {
        // A stale observed value: the CAS loses, so only the UPDATE is issued — the record and the
        // offer that follow a win belong to their own stores and are covered above
        boolean fired = triggerFirer.fire(JobKey.of("job-a"), 1L, NOW, NOW.plusSeconds(60), List.of(), NOW);

        assertThat(fired).isFalse();
        assertEveryStatementIsBounded();
    }

    /**
     * The scans run on the loop thread but their cost is rows, not a lock: 2.8 s measured for 1M
     * definitions, which a ceiling would turn into a tick that dies every cycle. Deliberately out.
     */
    @Test
    void coordinatorPolicyAndTriggerReadsHaveDeadlinesWhileAdministrativeStreamingDoesNot() {
        jobStore.findDueRecurring(NOW, 10);
        jobStore.findByKeys(List.of(JobKey.of("job-a")));
        jobStore.nextFireAfter(NOW);
        workQueue.findCandidateJobs(0, null, 128, NOW);
        assertEveryStatementIsBounded();
        prepared.clear();
        try (var all = jobStore.findAll()) { all.toList(); }
        assertNoStatementIsBounded();
    }

    /** Registration takes job locks and must leave enough time for heartbeat and completion traffic. */
    @Test
    void theDefinitionUpsertHasATransactionDeadline() {
        jobStore.upsert(JobDefinition.of("job-a", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        assertEveryStatementIsBounded();
    }

    private void assertEveryStatementIsBounded() {
        assertThat(prepared).isNotEmpty();
        assertThat(prepared).allSatisfy(statement ->
                assertThat(statement.timeout).as(statement.sql).isEqualTo(JdbcSupport.TICK_STATEMENT_TIMEOUT_SECONDS));
    }

    private void assertNoStatementIsBounded() {
        assertThat(prepared).isNotEmpty();
        assertThat(prepared).allSatisfy(statement -> assertThat(statement.timeout).as(statement.sql).isNull());
    }

    /** The setup's statements are the fixture, not the subject — they leave the record before the assertions. */
    private void forgetSetupStatements() {
        prepared.clear();
    }

    /** One execution of {@code job-a}, enqueued and claimed by {@link #NODE} as attempt 1 — the fixture every completion test starts from. */
    private void claimOneExecution(@Nullable String batchId) {
        jobStore.upsert(JobDefinition.of("job-a", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        TestWork.history(recording, new H2JdbcDelegate(), List.of(new SubmissionStore.NewExecution(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), JobKey.of("job-a"), 0, 20,
                NOW, NOW, "test", batchId, null, io.mohs.test.InputFixtures.encode(JobKey.of("job-a"), "payload"), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)));
        TestWork.ready(recording, new H2JdbcDelegate(), List.of(new WorkQueue.ReadyEntry(ExecutionId.of("01991337-0000-7000-8000-00000000001b"), JobKey.of("job-a"), 0, 20, 1, NOW)));
        claimToken = workQueue.claim(0, NODE, 10, List.of(), NOW).getFirst().claimToken();
        forgetSetupStatements();
    }

    /**
     * A {@code DataSource} whose prepared statements record the SQL they were created for and the
     * query timeout the template gave them — pass-through otherwise (the same proxy technique as
     * {@code JdbcJobStoreTest}'s SQL recorder, one level deeper).
     */
    private DataSource recordingDataSource(DataSource target) {
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (_, method, args) -> {
                    Object result = method.invoke(target, args);
                    return result instanceof Connection connection ? recordingConnection(connection) : result;
                });
    }

    private Connection recordingConnection(Connection target) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (_, method, args) -> {
                    if (method.getName().equals("createStatement")) {
                        throw new AssertionError("a static Statement bypasses this record — every store prepares");
                    }
                    Object result = method.invoke(target, args);
                    if (method.getName().equals("prepareStatement") && args[0] instanceof String sql
                            && result instanceof PreparedStatement statement) {
                        Prepared record = new Prepared(sql);
                        prepared.add(record);
                        return recordingStatement(statement, record);
                    }
                    return result;
                });
    }

    private PreparedStatement recordingStatement(PreparedStatement target, Prepared record) {
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (_, method, args) -> {
                    if (method.getName().equals("setQueryTimeout") && args[0] instanceof Integer seconds) {
                        record.timeout = seconds;
                    }
                    return method.invoke(target, args);
                });
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:tick-timeout-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }
}
