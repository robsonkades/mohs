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

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The completion transaction against the REAL Tier 1 database, and the TIMESTAMPTZ crossing that H2
 * cannot express. It used to cover the weekly partitions, and then the {@code created_at} equality
 * they left behind in the primary key; both are gone, and what replaces them is the guarantee that
 * arrived with the natural key — {@code execution_id} is unique, so the terminal UPDATE matching by
 * id can only ever touch one row. The shared semantics live in {@code JdbcLeaseStoreTest} (H2).
 */
@Tag("docker")
class JdbcLeaseStorePostgresTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");
    private static final Instant CREATED_AT = Instant.parse("2026-08-22T11:59:00.123456Z");

    private JdbcTemplate rawJdbcTemplate;
    private JdbcLeaseStore store;
    private JdbcWorkQueue queue;
    private JdbcJobStore jobStore;

    @BeforeEach
    void setUp() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock, new PostgresJdbcDelegate());
        store = new JdbcLeaseStore(dataSource, new PostgresJdbcDelegate(), batchStore);
        queue = new JdbcWorkQueue(dataSource, new PostgresJdbcDelegate(), batchStore);
        jobStore = new JdbcJobStore(dataSource, clock, new PostgresJdbcDelegate());
    }

    @Test
    void terminalUpdateFindsTheRowByItsPrimaryKey() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-1', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT), JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT));
        queue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of("exec-1"), JobKey.of("job-a"), 0, 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-pg", 1, 10, List.of(), NOW);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("exec-1"), JobKey.of("job-a"), "node-pg", 1, 1,
                NOW.minusSeconds(2), NOW, ExecutionState.SUCCEEDED, null, null,
                ExecutionState.SUCCEEDED, null)), jobStore);

        assertThat(verdicts.get(ExecutionId.of("exec-1")).owned()).isTrue();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("SUCCEEDED");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT outcome FROM mohs_attempt WHERE execution_id = 'exec-1'", String.class)).isEqualTo("SUCCEEDED");
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", Integer.class)).isZero();
    }

    @Test
    void epochFenceDiscardsTheZombieOnPostgresToo() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-1', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT), JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT));
        queue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of("exec-1"), JobKey.of("job-a"), 0, 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-pg", 2, 10, List.of(), NOW);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("exec-1"), JobKey.of("job-a"), "node-pg", 1, 1,
                NOW.minusSeconds(2), NOW, ExecutionState.SUCCEEDED, null, null,
                ExecutionState.SUCCEEDED, null)), jobStore);

        assertThat(verdicts.get(ExecutionId.of("exec-1"))).isEqualTo(LeaseStore.Completion.FENCED_OUT);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("PENDING");
    }

    /**
     * The guarantee the normalised key BUYS, and the reason the terminal UPDATE may match by id
     * alone. Under the old key — {@code (created_at, execution_id)} — this insert succeeded, and two
     * rows shared an id: nothing in the code produced that, but nothing in the schema forbade it
     * either, and it was the only reason the completion carried {@code created_at} all the way from
     * the claim.
     */
    @Test
    void theSchemaRefusesASecondRowWithTheSameExecutionId() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-1', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT), JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT));

        assertThatThrownBy(() -> rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-1', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcOffsetDateTime(NOW), JdbcTimestamps.toUtcOffsetDateTime(NOW)))
                .isInstanceOf(DuplicateKeyException.class);
    }

    /** The same, one level down: an attempt is identified by its execution and its number, with no finished_at in the key. */
    @Test
    void theSchemaRefusesASecondAttemptWithTheSameNumber() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome)
                VALUES ('exec-1', 1, 'node-pg', ?, ?, 'SUCCEEDED')
                """, JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT), JdbcTimestamps.toUtcOffsetDateTime(NOW));

        assertThatThrownBy(() -> rawJdbcTemplate.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome)
                VALUES ('exec-1', 1, 'node-pg', ?, ?, 'FAILED')
                """, JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT), JdbcTimestamps.toUtcOffsetDateTime(NOW.plusSeconds(1))))
                .isInstanceOf(DuplicateKeyException.class);
    }
}
