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
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new PostgresJdbcDelegate(), "01991337-0000-7000-8000-00000000006f", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, new PostgresJdbcDelegate());
        store = new JdbcLeaseStore(dataSource, new PostgresJdbcDelegate());
        queue = new JdbcWorkQueue(dataSource, new PostgresJdbcDelegate());
        jobStore = new JdbcJobStore(dataSource, clock, new PostgresJdbcDelegate());
        jobStore.upsert(io.mohs.core.definition.JobDefinition.of("job-a", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
    }

    @Test
    void terminalUpdateFindsTheRowByItsPrimaryKey() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES ('01991337-0000-7000-8000-00000000001c', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(CREATED_AT), JdbcTimestamps.toUtcLocalDateTime(CREATED_AT));
        TestWork.ready(rawJdbcTemplate.getDataSource(), new PostgresJdbcDelegate(), List.of(new WorkQueue.ReadyEntry(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-a"), 0, 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "01991337-0000-7000-8000-00000000006f", 10, List.of(), NOW);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-a"), "01991337-0000-7000-8000-00000000006f", rawJdbcTemplate.queryForObject("SELECT claim_token FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = '01991337-0000-7000-8000-00000000001c'", String.class), 1,
                NOW.minusSeconds(2), NOW, ExecutionState.SUCCEEDED, null, null, null)));

        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-00000000001c")).owned()).isTrue();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("SUCCEEDED");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT last_attempt_outcome FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("SUCCEEDED");
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isZero();
    }

    @Test
    void epochFenceDiscardsTheZombieOnPostgresToo() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES ('01991337-0000-7000-8000-00000000001c', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(CREATED_AT), JdbcTimestamps.toUtcLocalDateTime(CREATED_AT));
        TestWork.ready(rawJdbcTemplate.getDataSource(), new PostgresJdbcDelegate(), List.of(new WorkQueue.ReadyEntry(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-a"), 0, 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "01991337-0000-7000-8000-00000000006f", 10, List.of(), NOW);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-a"), "01991337-0000-7000-8000-00000000006f", "01991337-0000-7000-8000-000000000085", 1,
                NOW.minusSeconds(2), NOW, ExecutionState.SUCCEEDED, null, null, null)));

        assertThat(verdicts.get(ExecutionId.of("01991337-0000-7000-8000-00000000001c"))).isEqualTo(LeaseStore.Completion.FENCED_OUT);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", String.class)).isEqualTo("PENDING");
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
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES ('01991337-0000-7000-8000-00000000001c', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(CREATED_AT), JdbcTimestamps.toUtcLocalDateTime(CREATED_AT));

        assertThatThrownBy(() -> rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES ('01991337-0000-7000-8000-00000000001c', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW)))
                .isInstanceOf(DuplicateKeyException.class);
    }

    /** A partial latest-attempt update cannot leave a number without its corresponding result. */
    @Test
    void theSchemaRejectsAnIncompleteLatestAttempt() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                VALUES ('01991337-0000-7000-8000-00000000001c', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(CREATED_AT), JdbcTimestamps.toUtcLocalDateTime(CREATED_AT));
        assertThatThrownBy(() -> rawJdbcTemplate.update(
                "UPDATE mohs_execution SET last_attempt_number = 2 WHERE execution_id = '01991337-0000-7000-8000-00000000001c'"))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class)
                .hasMessageContaining("ck_mohs_execution_attempt");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT last_attempt_number FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-00000000001c'", Integer.class)).isZero();
    }
}
