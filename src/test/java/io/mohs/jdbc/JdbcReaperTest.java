package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Reaper;
import io.mohs.engine.StoredJob;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcReaperTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcJobStore jobStore;
    private JdbcReaper reaper;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        jobStore = new JdbcJobStore(dataSource, clock);
        JdbcExecutionStore executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new H2JdbcDialect());
        reaper = new JdbcReaper(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:reaper-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private void seedJob(String jobKey) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
    }

    private void seedRunningExecution(String id, String jobKey, Instant leaseExpiresAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, node_id, lease_expires_at, payload, payload_type, created_at)
                VALUES (?, ?, 'RUNNING', ?, 'test', 'node-a', ?, '{}', 'java.lang.Object', ?)
                """, id, jobKey, JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(60)),
                JdbcTimestamps.toUtcTimestamp(leaseExpiresAt), JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(60)));
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_executions WHERE id = ?", String.class, id));
    }

    @Test
    void reclaimExpiredTransitionsExpiredRunningExecutionToFailed() {
        seedJob("welcome-email");
        seedRunningExecution("exec-1", "welcome-email", NOW.minusSeconds(1));

        List<Reaper.Reclaimed> reclaimed = reaper.reclaimExpired();

        assertThat(reclaimed).hasSize(1);
        Execution execution = reclaimed.get(0).execution();
        assertThat(execution.id().value()).isEqualTo("exec-1");
        assertThat(execution.state()).isEqualTo(ExecutionState.FAILED);
        assertThat(execution.attempts()).hasSize(1);
        assertThat(execution.attempts().get(0).outcome()).isEqualTo(ExecutionState.FAILED);
        assertThat(execution.attempts().get(0).error()).contains("lease expired");
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
    }

    @Test
    void reclaimExpiredIgnoresExecutionsWithAStillValidLease() {
        seedJob("welcome-email");
        seedRunningExecution("exec-1", "welcome-email", NOW.plusSeconds(30));

        List<Reaper.Reclaimed> reclaimed = reaper.reclaimExpired();

        assertThat(reclaimed).isEmpty();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
    }

    @Test
    void reclaimExpiredReleasesTheJobConcurrencySlot() {
        jobStore.upsert(JobDefinition.of("report-summary", Handler.class, spec -> spec.onDemand().preventOverlap()));
        jobStore.tryIncrementRunningExecutions(JobKey.of("report-summary"));
        seedRunningExecution("exec-1", "report-summary", NOW.minusSeconds(1));

        reaper.reclaimExpired();

        assertThat(jobStore.find(JobKey.of("report-summary"))).map(StoredJob::runningExecutionCount).contains(0);
    }

    @Test
    void reclaimExpiredReturnsEmptyWhenNothingIsRunning() {
        assertThat(reaper.reclaimExpired()).isEmpty();
    }

    /** ADR-0033 (revoga a restrição da ADR-0026): nó morto com orçamento restante reagenda com backoff — a garantia sob falha de nó vira at-least-once. */
    @Test
    void reclaimSchedulesARetryWhenTheJobStillHasBudget() {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(2)));
        seedRunningExecution("exec-1", "welcome-email", NOW.minusSeconds(1));

        List<Reaper.Reclaimed> reclaimed = reaper.reclaimExpired();

        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.get(0).execution().state()).isEqualTo(ExecutionState.RETRY_SCHEDULED);
        assertThat(reclaimed.get(0).execution().attempts()).hasSize(1);
        assertThat(reclaimed.get(0).execution().attempts().get(0).error()).contains("lease expired");
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RETRY_SCHEDULED);
        Timestamp retryAt = rawJdbcTemplate.queryForObject(
                "SELECT scheduled_at FROM mohs_executions WHERE id = ?", Timestamp.class, "exec-1");
        // bound do backoff da 1ª falha (full jitter): [now, now + 1s]
        assertThat(JdbcTimestamps.fromUtcTimestamp(retryAt)).isBetween(NOW, NOW.plusSeconds(1));
    }

    /** Job aposentado nunca reagenda: RETRY_SCHEDULED de job removido ficaria preso pra sempre (claim filtra retired, o cancel do remove já passou) — mesmo com orçamento sobrando, o reclaim é terminal, e o flag diz que NÃO foi por orçamento. */
    @Test
    void reclaimOfARetiredJobIsTerminalEvenWithBudgetRemaining() {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(5)));
        seedRunningExecution("exec-1", "welcome-email", NOW.minusSeconds(1));
        jobStore.remove(JobKey.of("welcome-email")); // não alcança a RUNNING; marca retired

        List<Reaper.Reclaimed> reclaimed = reaper.reclaimExpired();

        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.get(0).execution().state()).isEqualTo(ExecutionState.FAILED);
        assertThat(reclaimed.get(0).attemptsExhausted()).isFalse();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
    }

    /** Teto por ciclo: morte de nó em massa drena em passadas, mais antigo primeiro (ORDER BY em id UUIDv7) — nunca uma transação sem limite. */
    @Test
    void reclaimIsCappedPerCycleAndDrainsTheRestOnTheNextCall() {
        seedJob("welcome-email");
        List<Object[]> batch = new ArrayList<>();
        for (int i = 0; i < JdbcReaper.RECLAIM_LIMIT + 5; i++) {
            batch.add(new Object[] { String.format("exec-%05d", i), "welcome-email",
                    JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(60)), JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(1)),
                    JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(60)) });
        }
        rawJdbcTemplate.batchUpdate("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, node_id, lease_expires_at, payload, payload_type, created_at)
                VALUES (?, ?, 'RUNNING', ?, 'test', 'node-a', ?, '{}', 'java.lang.Object', ?)
                """, batch);

        assertThat(reaper.reclaimExpired()).hasSize(JdbcReaper.RECLAIM_LIMIT);
        assertThat(reaper.reclaimExpired()).hasSize(5);
        assertThat(reaper.reclaimExpired()).isEmpty();
    }

    /** O attempt sintético do reclaim conta no orçamento como qualquer outro — reclaim da última tentativa é FAILED terminal. */
    @Test
    void reclaimFailsTerminallyWhenTheBudgetIsExhausted() {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1)));
        seedRunningExecution("exec-1", "welcome-email", NOW.minusSeconds(1));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_attempts (execution_id, number, started_at, finished_at, outcome, error)
                VALUES (?, 1, ?, ?, 'FAILED', 'boom')
                """, "exec-1", JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(50)), JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(49)));

        List<Reaper.Reclaimed> reclaimed = reaper.reclaimExpired();

        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.get(0).execution().state()).isEqualTo(ExecutionState.FAILED);
        assertThat(reclaimed.get(0).execution().attempts()).hasSize(2);
        assertThat(reclaimed.get(0).attemptsExhausted()).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
    }
}
