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
        store = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new H2JdbcDelegate());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:history-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private HistoryStore.NewExecution execution(String id, String jobKey, @Nullable String idempotencyKey) {
        return new HistoryStore.NewExecution(ExecutionId.of(id), JobKey.of(jobKey), 0, 20,
                NOW, NOW, "test", null, idempotencyKey, new WelcomeEmail("a@b.c", 1));
    }

    @Test
    void recordWritesThePendingBirthRow() {
        store.record(List.of(execution("exec-1", "job-a", null)));

        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("PENDING");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT payload_type FROM mohs_execution WHERE execution_id = 'exec-1'", String.class))
                .isEqualTo(WelcomeEmail.class.getName());
    }

    /** Idempotent Receiver (EIP): the primary-key conflict in mohs_idempotency IS the check — it propagates before any history row is born. */
    @Test
    void recordWithADuplicateIdempotencyKeyFailsBeforeWritingHistory() {
        store.record(List.of(execution("exec-1", "job-a", "key-1")));

        assertThatThrownBy(() -> store.record(List.of(execution("exec-2", "job-a", "key-1"))))
                .isInstanceOf(DuplicateKeyException.class);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_execution WHERE execution_id = 'exec-2'", Integer.class)).isZero();
        assertThat(store.findByIdempotencyKey(JobKey.of("job-a"), "key-1"))
                .contains(ExecutionId.of("exec-1"));
    }

    /** The same key on different JOBS does not collide — the deduplication's scope is (job, key), as in the earlier era's unique index. */
    @Test
    void idempotencyKeysAreScopedPerJob() {
        store.record(List.of(execution("exec-1", "job-a", "key-1")));
        store.record(List.of(execution("exec-2", "job-b", "key-1")));

        assertThat(store.findByIdempotencyKey(JobKey.of("job-b"), "key-1")).contains(ExecutionId.of("exec-2"));
    }

    @Test
    void findByIdempotencyKeyIsEmptyForAnUnknownKey() {
        assertThat(store.findByIdempotencyKey(JobKey.of("job-a"), "never-used")).isEmpty();
    }

    /** Preserved on the new port: an unreadable row enters unreadable WITH its cause; its neighbours hydrate normally. */
    @Test
    void findPayloadsIsolatesUnreadableRows() {
        store.record(List.of(execution("exec-ok", "job-a", null)));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-bad', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'com.example.DoesNotExist')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));

        HistoryStore.PayloadBatch batch = store.findPayloads(List.of(ExecutionId.of("exec-ok"), ExecutionId.of("exec-bad")));

        assertThat(batch.rows()).containsOnlyKeys(ExecutionId.of("exec-ok"));
        assertThat(batch.rows().get(ExecutionId.of("exec-ok")).payload()).isEqualTo(new WelcomeEmail("a@b.c", 1));
        assertThat(batch.rows().get(ExecutionId.of("exec-ok")).head().createdAt()).isEqualTo(NOW);
        assertThat(batch.unreadable()).containsOnlyKeys(ExecutionId.of("exec-bad"));
        assertThat(batch.unreadable().get(ExecutionId.of("exec-bad"))).hasMessageContaining("com.example.DoesNotExist");
    }

    @Test
    void findAttemptsReturnsThemInNumberOrder() {
        store.record(List.of(execution("exec-1", "job-a", null)));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
                VALUES ('exec-1', 2, 'node-a', ?, ?, 'SUCCEEDED', NULL, NULL)
                """, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(5)), JdbcTimestamps.toUtcLocalDateTime(NOW));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
                VALUES ('exec-1', 1, 'node-a', ?, ?, 'FAILED', 'java.lang.IllegalStateException', 'boom')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(20)), JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(15)));

        assertThat(store.findAttempts(ExecutionId.of("exec-1"))).satisfiesExactly(
                first -> {
                    assertThat(first.number()).isEqualTo(1);
                    assertThat(first.outcome()).isEqualTo(ExecutionState.FAILED);
                    assertThat(first.error()).isEqualTo("boom");
                },
                second -> assertThat(second.outcome()).isEqualTo(ExecutionState.SUCCEEDED));
    }

    /** The pruning follows the idempotency window, not history's retention — a strict cutoff. */
    @Test
    void pruneIdempotencyRemovesOnlyStrictlyOlderKeys() {
        store.record(List.of(execution("exec-1", "job-a", "old-key")));
        rawJdbcTemplate.update("UPDATE mohs_idempotency SET created_at = ? WHERE idempotency_key = 'old-key'",
                JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(90_000)));
        store.record(List.of(execution("exec-2", "job-a", "fresh-key")));

        int pruned = store.pruneIdempotencyBefore(NOW.minusSeconds(86_400));

        assertThat(pruned).isEqualTo(1);
        assertThat(store.findByIdempotencyKey(JobKey.of("job-a"), "old-key")).isEmpty();
        assertThat(store.findByIdempotencyKey(JobKey.of("job-a"), "fresh-key")).contains(ExecutionId.of("exec-2"));
    }

    // --- the history sweep -------------------------------------------------------------------------
    // These fixtures carry realistic UUIDv7-shaped ids, unlike the 'exec-1' ones above: the sweep's
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

    private void insertExecution(String id, String state, @Nullable Instant finishedAt, @Nullable String correlationId) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, finished_at,
                    actor, correlation_id, payload, payload_type)
                VALUES (?, 'job-a', ?, ?, ?, ?, 'test', ?, '{}', ?)
                """, id, state, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW),
                finishedAt == null ? null : JdbcTimestamps.toUtcLocalDateTime(finishedAt), correlationId,
                WelcomeEmail.class.getName());
    }

    private void insertAttempt(String executionId, int number, Instant finishedAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome)
                VALUES (?, ?, 'node-a', ?, ?, 'SUCCEEDED')
                """, executionId, number, JdbcTimestamps.toUtcLocalDateTime(finishedAt),
                JdbcTimestamps.toUtcLocalDateTime(finishedAt));
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
        assertThat(pruned.attempts()).isEqualTo(1);
        assertThat(pruned.drained(100)).isTrue();
        assertThat(countRows("mohs_execution", oldDone)).isZero();
        assertThat(countRows("mohs_attempt", oldDone)).isZero();
        assertThat(countRows("mohs_execution", oldPending)).isOne();
        assertThat(countRows("mohs_execution", longRetry)).isOne();
        assertThat(countRows("mohs_attempt", longRetry)).isOne();
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
        assertThat(firstPass.attempts() + secondPass.attempts()).isEqualTo(3);
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
                INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at)
                VALUES ('job-a', 'key-old', ?, ?)
                """, id, JdbcTimestamps.toUtcLocalDateTime(OLD));

        store.pruneHistoryBefore(CUTOFF, 100);

        assertThat(countRows("mohs_execution", id)).isZero();
        assertThat(store.findByIdempotencyKey(JobKey.of("job-a"), "key-old")).contains(ExecutionId.of(id));
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

    /** A crash between the sweep's statements leaves orphans; the next sweep's own predicate collects them without any carried state. */
    @Test
    void sweepCollectsOrphanedAttemptsLeftBehind() {
        String vanished = v7At(OLD, 30);
        insertAttempt(vanished, 1, OLD);

        HistoryStore.PrunedHistory pruned = store.pruneHistoryBefore(CUTOFF, 100);

        assertThat(pruned.executions()).isZero();
        assertThat(pruned.attempts()).isEqualTo(1);
        assertThat(countRows("mohs_attempt", vanished)).isZero();
    }
}
