package io.mohs.store.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.store.jdbc.dialect.MySqlJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reexecuta os dois testes de {@link JdbcClaimerTest} que provam a
 * garantia de corretude da ADR-0018/ADR-0020 contra MySQL 8+ real —
 * ADR-0023 — mais quatro testes dos contratos do par de estados
 * claimáveis (ADR-0033): retry devido reivindicado, merge global por
 * {@code (priority, scheduled_at)} entre estados e truncamento do batch.
 * São contratos de qualquer forma de template — a candidata
 * {@code UNION ALL} por estado foi medida e rejeitada (BASELINE.md,
 * 2026-08-15), e estes testes pinam o comportamento que qualquer
 * substituta futura (ex.: hint de índice) precisa preservar. Requer
 * Docker (Testcontainers).
 */
class JdbcClaimerMySqlTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-13T12:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcExecutionStore executionStore;
    private JdbcJobStore jobStore;

    @BeforeEach
    void setUp() {
        dataSource = MySqlTestSupport.freshSchema();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new MySqlJdbcDialect());
        jobStore = new JdbcJobStore(dataSource, clock);
    }

    private JdbcClaimer newClaimer() {
        return new JdbcClaimer(dataSource, new MySqlJdbcDialect(), clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()), new JdbcRateLimitStore(dataSource, clock));
    }

    private void seedJob(String jobKey, Consumer<PolicySpec> policyConfigurer) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> policyConfigurer.accept(spec.onDemand())));
    }

    private void seedExecution(String id, String jobKey, Instant scheduledAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, 'ENQUEUED', ?, 'test', '{}', 'java.lang.Object', ?)
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(scheduledAt), JdbcTimestamps.toUtcLocalDateTime(NOW));
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_executions WHERE id = ?", String.class, id));
    }

    private void seedRetryScheduledExecution(String id, String jobKey, Instant retryAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, 'RETRY_SCHEDULED', ?, 'test', '{}', 'java.lang.Object', ?)
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(retryAt), JdbcTimestamps.toUtcLocalDateTime(NOW));
    }

    /** Retry devido reivindica como qualquer ENQUEUED — contrato da ADR-0033 que qualquer template do dialeto precisa preservar. */
    @Test
    void claimsRetryScheduledExecutionWhoseTimeHasCome() {
        seedJob("report", policy -> {
        });
        seedRetryScheduledExecution("exec-retry", "report", NOW.minusSeconds(5));

        List<Execution> claimed = newClaimer().claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-retry");
        assertThat(stateOf("exec-retry")).isEqualTo(ExecutionState.RUNNING);
    }

    /** A ordem de claim é global por (priority, scheduled_at) entre os dois estados — um retry mais antigo vem antes de um ENQUEUED mais novo. */
    @Test
    void mergesEnqueuedAndRetryScheduledInGlobalScheduledAtOrder() {
        seedJob("report", policy -> {
        });
        seedExecution("exec-enqueued", "report", NOW.minusSeconds(1));
        seedRetryScheduledExecution("exec-retry", "report", NOW.minusSeconds(10));

        List<Execution> claimed = newClaimer().claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-retry", "exec-enqueued");
    }

    /** batchSize trunca sobre o topo global, não por estado — com batch 1, só o retry mais antigo sai; o ENQUEUED fica intacto pro próximo tick. */
    @Test
    void batchSizeTruncatesTheGlobalTopAcrossStates() {
        seedJob("report", policy -> {
        });
        seedExecution("exec-enqueued", "report", NOW.minusSeconds(1));
        seedRetryScheduledExecution("exec-retry", "report", NOW.minusSeconds(10));

        List<Execution> claimed = newClaimer().claim("node-a", 1);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-retry");
        assertThat(stateOf("exec-enqueued")).isEqualTo(ExecutionState.ENQUEUED);
    }

    /** priority vem antes de scheduled_at na ordem global — ENQUEUED HIGH mais novo vence RETRY_SCHEDULED NORMAL mais antigo (a regressão silenciosa clássica de um template substituto). */
    @Test
    void higherPriorityEnqueuedBeatsOlderNormalPriorityRetry() {
        seedJob("report", policy -> {
        });
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                VALUES (?, ?, 'ENQUEUED', ?, 'test', ?, '{}', 'java.lang.Object', ?)
                """, "exec-high", "report", JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)),
                Priority.HIGH.value(), JdbcTimestamps.toUtcLocalDateTime(NOW));
        seedRetryScheduledExecution("exec-retry", "report", NOW.minusSeconds(10));

        List<Execution> claimed = newClaimer().claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-high", "exec-retry");
    }

    /** Ver javadoc do teste homônimo em {@link JdbcClaimerTest} — mesma garantia, banco diferente. */
    @Test
    void claimIsMutuallyExclusiveAcrossConcurrentNodes() throws Exception {
        seedJob("welcome-email", PolicySpec::preventOverlap);
        for (int i = 0; i < 5; i++) {
            seedExecution("exec-" + i, "welcome-email", NOW.minusSeconds(5 - i));
        }
        JdbcClaimer claimerA = newClaimer();
        JdbcClaimer claimerB = newClaimer();
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        Future<List<Execution>> futureA = executor.submit(() -> {
            barrier.await();
            return claimerA.claim("node-a", 5);
        });
        Future<List<Execution>> futureB = executor.submit(() -> {
            barrier.await();
            return claimerB.claim("node-b", 5);
        });

        List<Execution> claimedA = futureA.get(10, TimeUnit.SECONDS);
        List<Execution> claimedB = futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(claimedA.size() + claimedB.size()).isEqualTo(1);
    }

    @Test
    void claimStopsAtMaxConcurrentExecutionsButAllowsMoreAfterOneIsReleased() {
        JdbcClaimer claimer = newClaimer();
        seedJob("report-summary", policy -> policy.maxConcurrentExecutions(3));
        for (int i = 0; i < 5; i++) {
            seedExecution("exec-" + i, "report-summary", NOW.minusSeconds(5 - i));
        }

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).hasSize(3);
        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-0", "exec-1", "exec-2");
        assertThat(stateOf("exec-3")).isEqualTo(ExecutionState.ENQUEUED);
        assertThat(stateOf("exec-4")).isEqualTo(ExecutionState.ENQUEUED);

        assertThat(claimer.claim("node-a", 10)).isEmpty();

        jobStore.decrementRunningExecutions(JobKey.of("report-summary"));
        List<Execution> claimedAfterRelease = claimer.claim("node-a", 10);
        assertThat(claimedAfterRelease).extracting(e -> e.id().value()).containsExactly("exec-3");
    }
}
