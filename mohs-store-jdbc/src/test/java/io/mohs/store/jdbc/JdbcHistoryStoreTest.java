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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcHistoryStoreTest {

    record WelcomeEmail(String to, int template) {
    }

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private JdbcTemplate rawJdbcTemplate;
    private JdbcHistoryStore store;

    @BeforeEach
    void setUp() {
        DataSource dataSource = freshH2DataSource();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        store = new JdbcHistoryStore(dataSource, new H2JdbcDelegate());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:history-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private SubmissionStore.NewExecution execution(String id, String jobKey, @Nullable String idempotencyKey) {
        return new SubmissionStore.NewExecution(ExecutionId.of(id), JobKey.of(jobKey), 0, 20,
                NOW, NOW, "test", null, idempotencyKey, io.mohs.test.InputFixtures.encode(JobKey.of(jobKey), new WelcomeEmail("a@b.c", 1)), io.mohs.engine.SubmissionStore.Origin.MANUAL, null);
    }

    @Test
    void recordWritesThePendingBirthRow() {
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001c", "job-a", null)));

        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("PENDING");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT input_contract FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class))
                .isEqualTo("job-a");
    }

    /** Idempotent Receiver (EIP): the primary-key conflict in mohs_idempotency IS the check — it propagates before any history row is born. */
    @Test
    void recordWithADuplicateIdempotencyKeyFailsBeforeWritingHistory() {
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001c", "job-a", "key-1")));

        assertThatThrownBy(() -> TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001e", "job-a", "key-1"))))
                .isInstanceOf(DuplicateKeyException.class);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001e'", Integer.class)).isZero();
        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("job-a"), "key-1").map(io.mohs.core.event.Enqueued::executionId))
                .contains(ExecutionId.of("01991337-0000-7000-8000-00000000001c"));
    }

    /** The same key on different JOBS does not collide — the deduplication's scope is (job, key), as in the earlier era's unique index. */
    @Test
    void idempotencyKeysAreScopedPerJob() {
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001c", "job-a", "key-1")));
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001e", "job-b", "key-1")));

        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("job-b"), "key-1").map(io.mohs.core.event.Enqueued::executionId)).contains(ExecutionId.of("01991337-0000-7000-8000-00000000001e"));
    }

    @Test
    void findByIdempotencyKeyIsEmptyForAnUnknownKey() {
        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("job-a"), "never-used").map(io.mohs.core.event.Enqueued::executionId)).isEmpty();
    }

    /** Preserved on the new port: an unreadable row enters unreadable WITH its cause; its neighbours hydrate normally. */
    @Test
    void findPayloadsIsolatesUnreadableRows() {
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-000000000041", "job-a", null)));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES ('01991337-0000-7000-8000-000000000026', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'invalid contract')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));

        HistoryStore.PayloadBatch batch = store.findPayloads(List.of(ExecutionId.of("01991337-0000-7000-8000-000000000041"), ExecutionId.of("01991337-0000-7000-8000-000000000026")));

        assertThat(batch.rows()).containsOnlyKeys(ExecutionId.of("01991337-0000-7000-8000-000000000041"));
        assertThat(io.mohs.test.InputFixtures.decode(batch.rows().get(ExecutionId.of("01991337-0000-7000-8000-000000000041")).payload(), WelcomeEmail.class)).isEqualTo(new WelcomeEmail("a@b.c", 1));
        assertThat(batch.rows().get(ExecutionId.of("01991337-0000-7000-8000-000000000041")).head().createdAt()).isEqualTo(NOW);
        assertThat(batch.unreadable()).containsOnlyKeys(ExecutionId.of("01991337-0000-7000-8000-000000000026"));
        assertThat(batch.unreadable().get(ExecutionId.of("01991337-0000-7000-8000-000000000026"))).hasMessageContaining("inputContract");
    }

    @Test
    void detailReturnsTheLatestCompletedAttemptAndSummariesOmitIt() {
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001c", "job-a", null)));
        insertAttempt("01991337-0000-7000-8000-00000000001c", 3, NOW);
        assertThat(store.find(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), NOW).orElseThrow().lastAttempt()).hasValueSatisfying(attempt -> {
            assertThat(attempt.number()).isEqualTo(3);
            assertThat(attempt.outcome()).isEqualTo(ExecutionState.SUCCEEDED);
            assertThat(attempt.finishedAt()).isEqualTo(NOW);
        });
        assertThat(store.findPage(null, null, null, null, null, 10, NOW)).singleElement()
                .satisfies(execution -> assertThat(execution.lastAttempt()).isEmpty());
    }

    /** The pruning follows the idempotency window, not history's retention — a strict cutoff. */
    @Test
    void pruneIdempotencyRemovesOnlyExpiredKeys() {
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001c", "job-a", "old-key")));
        rawJdbcTemplate.update("UPDATE mohs_idempotency SET expires_at = ? WHERE idempotency_key = 'old-key'",
                JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(90_000)));
        TestSubmissions.accept(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(execution("01991337-0000-7000-8000-00000000001e", "job-a", "fresh-key")));

        int pruned = store.pruneExpiredIdempotency();

        assertThat(pruned).isEqualTo(1);
        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("job-a"), "old-key").map(io.mohs.core.event.Enqueued::executionId)).isEmpty();
        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("job-a"), "fresh-key").map(io.mohs.core.event.Enqueued::executionId)).contains(ExecutionId.of("01991337-0000-7000-8000-00000000001e"));
    }

    // --- the history sweep -------------------------------------------------------------------------
    // These fixtures carry realistic UUIDv7-shaped ids, unlike the '01991337-0000-7000-8000-00000000001c' ones above: the sweep's
    // candidate read is a range of the primary key derived from the id's time prefix, so an id that
    // does not encode its birth instant would silently fall outside every window.

    private static final Instant CUTOFF = NOW.minus(Duration.ofDays(30));
    private static final Instant OLD = NOW.minus(Duration.ofDays(60));
    private static final Instant RECENT = NOW.minus(Duration.ofDays(1));

    /** A UUIDv7-shaped id born at {@code instant} — above the sweep's lower bound for the same millisecond, below any later one. */
    private static String v7At(Instant instant, int sequence) {
        long ms = instant.toEpochMilli();
        return "%08x-%04x-7fff-8fff-%012x".formatted(ms >>> 16, ms & 0xFFFF, sequence);
    }

    private void insertExecution(String id, String state, @Nullable Instant finishedAt, @Nullable String batchId) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, finished_at,
                    actor, batch_id, payload, input_contract)
                VALUES (?, 'job-a', ?, ?, ?, ?, 'test', ?, '{}', ?)
                """, id, state, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW),
                finishedAt == null ? null : JdbcTimestamps.toUtcLocalDateTime(finishedAt), batchId,
                WelcomeEmail.class.getName());
    }

    private void insertAttempt(String executionId, int number, Instant finishedAt) {
        assertThat(rawJdbcTemplate.update("""
                UPDATE mohs_execution SET last_attempt_number = ?, last_attempt_node_id = '01991337-0000-7000-8000-000000000065',
                    last_attempt_started_at = ?, last_attempt_finished_at = ?, last_attempt_outcome = 'SUCCEEDED'
                WHERE execution_id = ?
                """, number, JdbcTimestamps.toUtcLocalDateTime(finishedAt),
                JdbcTimestamps.toUtcLocalDateTime(finishedAt), executionId)).isEqualTo(1);
    }

    private void insertBatch(String id, Instant createdAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_batches (id, name, total, succeeded, failed, created_at)
                VALUES (?, 'nightly', 1, 1, 0, ?)
                """, id, JdbcTimestamps.toUtcLocalDateTime(createdAt));
    }

    private int countRows(String table, String id) {
        String column = table.equals("mohs_batches") ? "id" : "execution_id";
        Integer count = rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + table + " WHERE " + column + " = ?", Integer.class, id);
        return count == null ? 0 : count;
    }

    @Test
    void sweepDeletesTerminalHistoryOutsideTheWindowAndSparesEverythingElse() {
        String oldDone = v7At(OLD, 1);
        insertExecution(oldDone, "SUCCEEDED", OLD, null);
        insertAttempt(oldDone, 1, OLD);
        String oldPending = v7At(OLD, 2);
        insertExecution(oldPending, "PENDING", null, null);
        // Born long ago, finished yesterday — a long retry. Its finish decides, not its birth.
        String longRetry = v7At(OLD, 3);
        insertExecution(longRetry, "FAILED", RECENT, null);
        insertAttempt(longRetry, 1, RECENT);
        String recent = v7At(RECENT, 4);
        insertExecution(recent, "SUCCEEDED", RECENT, null);

        HistoryStore.PrunedHistory pruned = store.pruneHistoryBefore(CUTOFF, 100);

        assertThat(pruned.executions()).isEqualTo(1);
        assertThat(pruned.drained(100)).isTrue();
        assertThat(countRows("mohs_execution", oldDone)).isZero();
        assertThat(countRows("mohs_execution", oldPending)).isOne();
        assertThat(countRows("mohs_execution", longRetry)).isOne();
        assertThat(store.find(ExecutionId.of(longRetry), NOW).orElseThrow().lastAttempt()).isPresent();
        assertThat(countRows("mohs_execution", recent)).isOne();
    }

    @Test
    void sweepIsBoundedPerStatementAndReportsWhenNotDrained() {
        for (int i = 1; i <= 3; i++) {
            String id = v7At(OLD, i);
            insertExecution(id, "SUCCEEDED", OLD, null);
            insertAttempt(id, 1, OLD);
        }

        HistoryStore.PrunedHistory firstPass = store.pruneHistoryBefore(CUTOFF, 2);
        assertThat(firstPass.executions()).isEqualTo(2);
        assertThat(firstPass.drained(2)).isFalse();

        HistoryStore.PrunedHistory secondPass = store.pruneHistoryBefore(CUTOFF, 2);
        assertThat(secondPass.executions()).isEqualTo(1);
        assertThat(firstPass.executions() + secondPass.executions()).isEqualTo(3);
        assertThat(store.pruneHistoryBefore(CUTOFF, 2).drained(2)).isTrue();
    }

    /** A batch lives exactly as long as its last visible member — open batches keep a live member by construction, and closed ones stay readable while any member's history does. */
    @Test
    void sweepCollectsABatchOnlyWhenNoMemberRemains() {
        String goneBatch = v7At(OLD, 10);
        insertBatch(goneBatch, OLD);
        String goneMember = v7At(OLD, 11);
        insertExecution(goneMember, "SUCCEEDED", OLD, goneBatch);
        String keptBatch = v7At(OLD, 12);
        insertBatch(keptBatch, OLD);
        String keptMember = v7At(OLD, 13);
        insertExecution(keptMember, "FAILED", RECENT, keptBatch);

        HistoryStore.PrunedHistory pruned = store.pruneHistoryBefore(CUTOFF, 100);

        assertThat(pruned.batches()).isEqualTo(1);
        assertThat(countRows("mohs_batches", goneBatch)).isZero();
        assertThat(countRows("mohs_batches", keptBatch)).isOne();
    }

    /** The dedup window is {@code idempotency-retention}'s contract: the key row outlives its execution's history, and that is correct — reusing the key still deduplicates. */
    @Test
    void sweepLeavesIdempotencyRowsToTheirOwnWindow() {
        String id = v7At(OLD, 20);
        insertExecution(id, "SUCCEEDED", OLD, null);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at, scheduled_at, actor)
                VALUES ('job-a', 'key-old', ?, ?, ?, 'test')
                """, id, JdbcTimestamps.toUtcLocalDateTime(OLD), JdbcTimestamps.toUtcLocalDateTime(OLD));

        store.pruneHistoryBefore(CUTOFF, 100);

        assertThat(countRows("mohs_execution", id)).isZero();
        assertThat(store.findReceiptByIdempotencyKey(JobKey.of("job-a"), "key-old").map(io.mohs.core.event.Enqueued::executionId)).contains(ExecutionId.of(id));
    }

    /**
     * The id bound is deliberately conservative: an id born AT the cutoff millisecond compares
     * {@code >=} the synthesized lower bound and is spared, whatever the rest of the row says — the
     * next hourly slot collects that one-millisecond sliver. The row is synthetic on purpose (its
     * {@code finished_at} alone would qualify it), so only the bound can be doing the sparing; this
     * pins the boundary against a future "correction" of {@code <} to {@code <=}.
     */
    @Test
    void anExecutionBornAtTheCutoffMillisecondIsSpared() {
        String atCutoff = v7At(CUTOFF, 40);
        insertExecution(atCutoff, "SUCCEEDED", OLD, null);

        HistoryStore.PrunedHistory pruned = store.pruneHistoryBefore(CUTOFF, 100);

        assertThat(pruned.executions()).isZero();
        assertThat(countRows("mohs_execution", atCutoff)).isOne();
    }

    /** Latest attempt fields share the execution row, so cleanup cannot leave detached detail. */
    @Test
    void sweepDeletesLatestAttemptTogetherWithItsExecution() {
        String id = v7At(OLD, 30);
        insertExecution(id, "SUCCEEDED", OLD, null);
        insertAttempt(id, 7, OLD);
        assertThat(store.find(ExecutionId.of(id), NOW).orElseThrow().lastAttempt()).hasValueSatisfying(
                attempt -> assertThat(attempt.number()).isEqualTo(7));
        assertThat(store.pruneHistoryBefore(CUTOFF, 100).executions()).isEqualTo(1);
        assertThat(store.find(ExecutionId.of(id), NOW)).isEmpty();
    }
}
