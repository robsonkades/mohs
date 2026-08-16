package io.mohs.jdbc;

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
import io.mohs.jdbc.dialect.PostgresJdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reexecuta os dois testes de {@link JdbcClaimerTest} que provam a
 * garantia de corretude da ADR-0018/ADR-0020 (CAS guardado, não
 * dependente de lock) contra Postgres real, não só H2 — ADR-0022. Não
 * duplica a suíte inteira: os outros testes de {@link JdbcClaimerTest}
 * (ordenação, batch size, etc.) não exercitam nada sensível a dialeto
 * que já não esteja coberto por eles contra H2. Requer Docker
 * (Testcontainers).
 *
 * <p>DBTUNE-16 adicionou dois testes próprios (não reexecuções): o CAS
 * em lote do {@code PostgresJdbcDialect} tornou ordenação e intercalação
 * mutex/concorrente sensíveis a dialeto — pinadas aqui contra Postgres
 * real.
 */
class JdbcClaimerPostgresTest {

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
        dataSource = PostgresTestSupport.freshSchema();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new PostgresJdbcDialect());
        jobStore = new JdbcJobStore(dataSource, clock);
    }

    private JdbcClaimer newClaimer() {
        return new JdbcClaimer(dataSource, new PostgresJdbcDialect(), clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()));
    }

    private void seedJob(String jobKey, Consumer<PolicySpec> policyConfigurer) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> policyConfigurer.accept(spec.onDemand())));
    }

    private void seedExecution(String id, String jobKey, Instant scheduledAt) {
        seedExecution(id, jobKey, scheduledAt, Priority.NORMAL);
    }

    private void seedExecution(String id, String jobKey, Instant scheduledAt, Priority priority) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at)
                VALUES (?, ?, 'ENQUEUED', ?, 'test', ?, '{}', 'java.lang.Object', ?)
                """, id, jobKey, JdbcTimestamps.toUtcTimestamp(scheduledAt), priority.value(), JdbcTimestamps.toUtcTimestamp(NOW));
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_executions WHERE id = ?", String.class, id));
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

    /**
     * DBTUNE-16: pina o contrato observável do lote sem mutex de job contra
     * Postgres real — ordem por (prioridade, scheduled_at), estado RUNNING,
     * node_id e lease gravados, não-devido fica de fora. É o comportamento
     * que qualquer implementação do CAS (linha a linha ou em lote) precisa
     * preservar.
     */
    @Test
    void claimsAConcurrentAllowedBatchPreservingCandidateOrder() {
        JdbcClaimer claimer = newClaimer();
        seedJob("bulk", policy -> {
        });
        seedExecution("exec-normal-old", "bulk", NOW.minusSeconds(10));
        seedExecution("exec-normal-new", "bulk", NOW.minusSeconds(2));
        seedExecution("exec-critical", "bulk", NOW.minusSeconds(1), Priority.CRITICAL);
        seedExecution("exec-future", "bulk", NOW.plusSeconds(60));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value())
                .containsExactly("exec-critical", "exec-normal-old", "exec-normal-new");
        assertThat(claimed).allSatisfy(execution -> assertThat(execution.state()).isEqualTo(ExecutionState.RUNNING));
        assertThat(rawJdbcTemplate.queryForList(
                "SELECT id FROM mohs_executions WHERE node_id = 'node-a' AND lease_expires_at IS NOT NULL", String.class))
                .containsExactlyInAnyOrder("exec-critical", "exec-normal-old", "exec-normal-new");
        assertThat(stateOf("exec-future")).isEqualTo(ExecutionState.ENQUEUED);
    }

    /** DBTUNE-16: candidatos com e sem mutex de job intercalados no mesmo lote saem na ordem dos candidatos, e o mutex continua valendo. */
    @Test
    void claimInterleavesMutexAndConcurrentCandidatesInCandidateOrder() {
        JdbcClaimer claimer = newClaimer();
        seedJob("mutex-job", PolicySpec::preventOverlap);
        seedJob("bulk", policy -> {
        });
        seedExecution("exec-bulk-1", "bulk", NOW.minusSeconds(4));
        seedExecution("exec-mutex-1", "mutex-job", NOW.minusSeconds(3));
        seedExecution("exec-bulk-2", "bulk", NOW.minusSeconds(2));
        seedExecution("exec-mutex-2", "mutex-job", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value())
                .containsExactly("exec-bulk-1", "exec-mutex-1", "exec-bulk-2");
        assertThat(stateOf("exec-mutex-2")).isEqualTo(ExecutionState.ENQUEUED);
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
