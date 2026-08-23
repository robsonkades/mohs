package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.SqlServerJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/** A varredura T-SQL do claim (§5.4 — {@code TOP} + {@code UPDLOCK, ROWLOCK, READPAST}) contra SQL Server real (Tier 2). */
class JdbcWorkQueueSqlServerTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = SqlServerTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        queue = new JdbcWorkQueue(dataSource, new SqlServerJdbcDialect(), new JdbcBatchStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC)), Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, priority, attempt, visibleAt);
    }

    @Test
    void claimMovesQueueEntriesIntoLeasesInOrder() {
        queue.offer(List.of(
                entry("exec-normal", "job-a", 20, 1, NOW.minusSeconds(20)),
                entry("exec-high", "job-a", 10, 2, NOW.minusSeconds(10)),
                entry("exec-inadmissible", "job-closed", 0, 1, NOW.minusSeconds(30))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "node-mssql", 1, 10, List.of(JobKey.of("job-closed")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("exec-high", "exec-normal");
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_ready", String.class))
                .containsExactly("exec-inadmissible");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt_number FROM mohs_lease WHERE execution_id = 'exec-high'", Integer.class)).isEqualTo(2);
    }

    /**
     * A parte mais sutil da emulação T-SQL: {@code READPAST} é quem faz o
     * claim PULAR a linha lockada por outro nó em vez de bloquear —
     * perdê-lo no hint passaria verde no resto da suíte e viraria
     * lock-wait no claim multi-nó (review S5.2; JCIP cap. 12, interleaving
     * controlado).
     */
    @Test
    void claimSkipsRowsLockedByAConcurrentClaimant() throws Exception {
        queue.offer(List.of(
                entry("exec-locked", "job-a", 10, 1, NOW.minusSeconds(2)),
                entry("exec-free", "job-a", 20, 1, NOW.minusSeconds(1))));
        try (java.sql.Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (java.sql.Statement st = other.createStatement()) {
                st.execute("SELECT execution_id FROM mohs_ready WITH (UPDLOCK, ROWLOCK) WHERE execution_id = 'exec-locked'");

                assertThat(queue.claim(0, "node-b", 1, 10, List.of(), NOW))
                        .extracting(w -> w.executionId().value())
                        .containsExactly("exec-free");
            } finally {
                other.rollback();
            }
        }
    }

    @Test
    void requeueFenceHoldsOnSqlServerToo() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-mssql", 1, 10, List.of(), NOW);

        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-mssql", 99, entry("exec-1", "job-a", 20, 2, NOW))))).isZero();
        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-mssql", 1, entry("exec-1", "job-a", 20, 2, NOW))))).isEqualTo(1);
    }
}
