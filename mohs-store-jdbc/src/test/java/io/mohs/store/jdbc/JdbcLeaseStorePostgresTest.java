package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A conclusão do §7.5-3 contra o Tier 1 REAL: partições semanais no lugar
 * da tabela plana, poda por igualdade de {@code created_at} (PLAN.md
 * S5.1) e a travessia TIMESTAMPTZ. A semântica compartilhada mora em
 * {@code JdbcLeaseStoreTest} (H2).
 */
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
        new PostgresPartitionManager(dataSource).ensureWeeklyPartitions(NOW);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock);
        store = new JdbcLeaseStore(dataSource, new PostgresJdbcDialect(), batchStore);
        queue = new JdbcWorkQueue(dataSource, new PostgresJdbcDialect(), batchStore);
        jobStore = new JdbcJobStore(dataSource, clock);
    }

    @Test
    void terminalUpdateLandsOnThePartitionedHistoryThroughTheCreatedAtEqualityPrune() {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-1', 'job-a', 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT), JdbcTimestamps.toUtcOffsetDateTime(CREATED_AT));
        queue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of("exec-1"), JobKey.of("job-a"), 0, 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-pg", 1, 10, List.of(), NOW);

        Map<ExecutionId, LeaseStore.Completion> verdicts = store.complete(List.of(new LeaseStore.CompletionResult(
                ExecutionId.of("exec-1"), JobKey.of("job-a"), "node-pg", 1, 1,
                NOW.minusSeconds(2), NOW, ExecutionState.SUCCEEDED, null, null,
                ExecutionState.SUCCEEDED, CREATED_AT, null)), jobStore);

        assertThat(verdicts.get(ExecutionId.of("exec-1")).owned()).isTrue();
        // a poda por IGUALDADE achou a linha na partição — created_at com precisão de micros atravessou verbatim
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
                ExecutionState.SUCCEEDED, CREATED_AT, null)), jobStore);

        assertThat(verdicts.get(ExecutionId.of("exec-1"))).isEqualTo(LeaseStore.Completion.FENCED_OUT);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = 'exec-1'", String.class)).isEqualTo("PENDING");
    }
}
