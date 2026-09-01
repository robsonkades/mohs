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
}
