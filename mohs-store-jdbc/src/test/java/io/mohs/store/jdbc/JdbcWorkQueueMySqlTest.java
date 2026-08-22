package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.MySqlJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/** A forma portátil do claim (§5.4) contra MySQL real (Tier 2) — semântica completa em {@code JdbcWorkQueueTest} (H2, mesma forma). */
class JdbcWorkQueueMySqlTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        DataSource dataSource = MySqlTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        queue = new JdbcWorkQueue(dataSource, new MySqlJdbcDialect());
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, priority, attempt, visibleAt);
    }

    @Test
    void claimMovesQueueEntriesIntoLeasesInOrder() {
        queue.offer(List.of(
                entry("exec-normal", "job-a", 20, 1, NOW.minusSeconds(20)),
                entry("exec-high", "job-a", 10, 2, NOW.minusSeconds(10)),
                entry("exec-future", "job-a", 10, 1, NOW.plusSeconds(60))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "node-mysql", 1, 10, List.of(), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("exec-high", "exec-normal");
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_ready", String.class))
                .containsExactly("exec-future");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt_number FROM mohs_lease WHERE execution_id = 'exec-high'", Integer.class)).isEqualTo(2);
    }

    @Test
    void requeueFenceHoldsOnMySqlToo() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-mysql", 1, 10, List.of(), NOW);

        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-mysql", 99, entry("exec-1", "job-a", 20, 2, NOW))))).isZero();
        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-mysql", 1, entry("exec-1", "job-a", 20, 2, NOW))))).isEqualTo(1);
    }
}
