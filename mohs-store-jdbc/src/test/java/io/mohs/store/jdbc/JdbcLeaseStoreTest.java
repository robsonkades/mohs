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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcLeaseStoreTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");
    private static final Instant CREATED_AT = NOW.minusSeconds(60);

    private JdbcTemplate rawJdbcTemplate;
    private JdbcLeaseStore store;
    private JdbcWorkQueue queue;
    private JdbcNodeStore nodes;
    private final Map<String, String> tokens = new java.util.HashMap<>();
    private JdbcJobStore jobStore;

    @BeforeEach
    void setUp() {
        DataSource dataSource = freshH2DataSource();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        nodes = new JdbcNodeStore(dataSource, new H2JdbcDelegate());
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, new H2JdbcDelegate());
        store = new JdbcLeaseStore(dataSource, new H2JdbcDelegate());
        queue = new JdbcWorkQueue(dataSource, new H2JdbcDelegate());
        jobStore = new JdbcJobStore(dataSource, clock, new H2JdbcDelegate());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:lease-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    /** Seeds the ownership through the real path (queue, then claim) plus the history row the advisory terminal UPDATE reaches. */
    private void seedLeased(String id, String jobKey, String nodeId, String tokenLabel, int attempt) {
        jobStore.upsert(io.mohs.core.definition.JobDefinition.of(jobKey, new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES (?, ?, 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(CREATED_AT), JdbcTimestamps.toUtcLocalDateTime(CREATED_AT));
        TestWork.ready(rawJdbcTemplate.getDataSource(), new H2JdbcDelegate(), List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, 20, attempt, NOW.minusSeconds(1))));
        TestSessions.seed(rawJdbcTemplate.getDataSource(), new H2JdbcDelegate(), nodeId, io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        var token = queue.claim(0, nodeId, 10, List.of(), NOW).getFirst().claimToken();
        tokens.put(id + ":" + tokenLabel, token);
    }

    private LeaseStore.CompletionResult terminal(String id, String jobKey, String nodeId, String tokenLabel, int attempt,
            ExecutionState state, @Nullable String errorType, @Nullable String error) {
        return new LeaseStore.CompletionResult(ExecutionId.of(id), JobKey.of(jobKey), nodeId, tokens.getOrDefault(id + ":" + tokenLabel, "01991337-0000-7000-8000-000000000085"), attempt,
                NOW.minusSeconds(2), NOW, state, errorType, error, null);
    }

    @Test
    void completeDeletesTheLeaseRecordsTheAttemptAndUpdatesTheAdvisoryState() {
        seedLeased("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(
                terminal("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1, ExecutionState.SUCCEEDED, null, null)));

        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-00000000001c")).owned()).isTrue();
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT last_attempt_outcome FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c' AND last_attempt_number = 1", String.class))
                .isEqualTo("SUCCEEDED");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("SUCCEEDED");
    }

    /** The fence: a result with a stale epoch (a zombie) loses EVERYTHING — neither the attempt, nor the state, nor the new incarnation's lease. */
    @Test
    void completeWithAStaleEpochIsFencedOutEntirely() {
        seedLeased("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-2", 1);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(
                terminal("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1, ExecutionState.SUCCEEDED, null, null)));

        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-00000000001c"))).isEqualTo(LeaseStore.Completion.FENCED_OUT);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COALESCE(SUM(last_attempt_number), 0) FROM mohs_execution", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("PENDING");
    }

    /**
     * The fence's third component. A healthy node's epoch never moves, so after a Watchdog Bound
     * released attempt 1 the SAME node re-claims attempt 2 with the same {@code (node_id, epoch)} — and
     * the zombie's completion, carrying attempt 1, must lose to the attempt number alone, leaving the
     * new incarnation's lease untouched. Before the attempt joined the fence, only the attempt table's
     * primary key stood in the way, aborting the whole group commit with a misleading error.
     */
    @Test
    void completeWithAStaleAttemptIsFencedOutEvenFromTheSameNodeAndEpoch() {
        seedLeased("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1);
        store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-a"), "01991337-0000-7000-8000-000000000065", tokens.get("01991337-0000-7000-8000-00000000001c:grant-1"), 1,
                NOW.minusSeconds(2), NOW, ExecutionState.FAILED, "java.lang.IllegalStateException", "watchdog bound", NOW)));
        queue.claim(0, "01991337-0000-7000-8000-000000000065", 10, List.of(), NOW);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(
                terminal("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1, ExecutionState.SUCCEEDED, null, null)));

        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-00000000001c"))).isEqualTo(LeaseStore.Completion.ALREADY_APPLIED);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = '01991337-0000-7000-8000-00000000001c'", Integer.class)).isEqualTo(2);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COALESCE(SUM(last_attempt_number), 0) FROM mohs_execution", Integer.class)).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("PENDING");
    }

    /** A mixed batch: the zombie is discarded row by row and never contaminates its neighbours (the same discipline as completeAll's). */
    @Test
    void completePartitionsFenceWinnersFromLosersWithinOneBatch() {
        seedLeased("01991337-0000-7000-8000-00000000004b", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1);
        seedLeased("01991337-0000-7000-8000-000000000037", "job-a", "01991337-0000-7000-8000-000000000067", "grant-5", 1);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(
                terminal("01991337-0000-7000-8000-00000000004b", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1, ExecutionState.SUCCEEDED, null, null),
                terminal("01991337-0000-7000-8000-000000000037", "job-a", "01991337-0000-7000-8000-000000000067", "grant-4", 1, ExecutionState.SUCCEEDED, null, null)));

        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-00000000004b")).owned()).isTrue();
        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-000000000037"))).isEqualTo(LeaseStore.Completion.FENCED_OUT);
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_work WHERE work_state = 'LEASED'", String.class))
                .containsExactly("01991337-0000-7000-8000-000000000037");
    }

    /** A non-terminal retry: the queue rebirth is IN THE SAME transaction — no window for an orphaned execution (see CompletionResult.retry's Javadoc). */
    @Test
    void completeWithARetryRebirthsTheReadyEntryAtomically() {
        seedLeased("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-a"), "01991337-0000-7000-8000-000000000065", tokens.get("01991337-0000-7000-8000-00000000001c:grant-1"), 1,
                NOW.minusSeconds(2), NOW, ExecutionState.FAILED, "java.lang.IllegalStateException", "boom", NOW.plusSeconds(30))));

        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-00000000001c")).owned()).isTrue();
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT last_attempt_error_type FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class))
                .isEqualTo("java.lang.IllegalStateException");
        // History stays PENDING (non-terminal) and the queue holds the attempt-2 entry with its backoff
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("PENDING");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_work WHERE work_state = 'READY' AND  execution_id = '01991337-0000-7000-8000-00000000001c'", Integer.class)).isEqualTo(2);
    }

    @Test
    void findByNodesReturnsTheNodesLeases() {
        var named = new org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate(rawJdbcTemplate);
        Instant before = JdbcDatabaseTime.now(named, new H2JdbcDelegate());
        seedLeased("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1);
        seedLeased("01991337-0000-7000-8000-00000000001e", "job-a", "01991337-0000-7000-8000-000000000067", "grant-3", 2);

        List<LeaseStore.Lease> leases = store.findByNodes(List.of("01991337-0000-7000-8000-000000000067"));

        assertThat(leases).singleElement().satisfies(lease -> {
            assertThat(lease.executionId().value()).isEqualTo("01991337-0000-7000-8000-00000000001e");
            assertThat(lease.claimToken()).isEqualTo(tokens.get("01991337-0000-7000-8000-00000000001e:grant-3"));
            assertThat(lease.attemptNumber()).isEqualTo(2);
            assertThat(lease.claimedAt()).isBetween(before, JdbcDatabaseTime.now(named, new H2JdbcDelegate()));
        });
    }

    /** Cooperative cancellation now lives on the ownership: a flag on the lease; an execution with no lease is not running — false. */
    @Test
    void requestCancellationFlagsOnlyALiveLease() {
        seedLeased("01991337-0000-7000-8000-00000000001c", "job-a", "01991337-0000-7000-8000-000000000065", "grant-1", 1);

        assertThat(queue.cancel(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), NOW)).isTrue();
        assertThat(queue.cancel(ExecutionId.of("01991337-0000-7000-8000-000000000050"), NOW)).isFalse();
        assertThat(store.findCancelRequested(List.of(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), ExecutionId.of("01991337-0000-7000-8000-000000000050"))))
                .containsExactly(ExecutionId.of("01991337-0000-7000-8000-00000000001c"));
    }

    @Test
    void completeOfNothingIsANoOp() {
        assertThat(store.complete(List.of())).isEmpty();
    }
}
