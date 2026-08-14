package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcClaimerTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-13T12:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcExecutionStore executionStore;
    private JdbcJobStore jobStore;
    private JdbcClaimer claimer;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build());
        jobStore = new JdbcJobStore(dataSource, clock);
        claimer = newClaimer();
    }

    private JdbcClaimer newClaimer() {
        return new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, LEASE_TTL);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:claimer-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
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
                """, id, jobKey, Timestamp.from(scheduledAt), priority.value(), Timestamp.from(NOW));
    }

    /** Omite a coluna {@code priority} pra exercitar o DEFAULT do schema (20 = NORMAL), não um valor explícito. */
    private void seedExecutionWithDefaultPriority(String id, String jobKey, Instant scheduledAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, 'ENQUEUED', ?, 'test', '{}', 'java.lang.Object', ?)
                """, id, jobKey, Timestamp.from(scheduledAt), Timestamp.from(NOW));
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_executions WHERE id = ?", String.class, id));
    }

    @Test
    void claimReturnsDueEnqueuedExecutionsAsRunning() {
        seedJob("welcome-email", policy -> {
        });
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1");
        assertThat(claimed.get(0).state()).isEqualTo(ExecutionState.RUNNING);
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
        assertThat(rawJdbcTemplate.queryForObject("SELECT node_id FROM mohs_executions WHERE id = ?", String.class, "exec-1"))
                .isEqualTo("node-a");
        Timestamp leaseExpiresAt = rawJdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM mohs_executions WHERE id = ?", Timestamp.class, "exec-1");
        assertThat(leaseExpiresAt.toInstant()).isEqualTo(NOW.plus(LEASE_TTL));
    }

    @Test
    void claimIgnoresExecutionsNotYetDue() {
        seedJob("welcome-email", policy -> {
        });
        seedExecution("exec-1", "welcome-email", NOW.plusSeconds(60));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).isEmpty();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.ENQUEUED);
    }

    @Test
    void claimRespectsBatchSize() {
        seedJob("bulk", policy -> {
        });
        seedExecution("exec-1", "bulk", NOW.minusSeconds(3));
        seedExecution("exec-2", "bulk", NOW.minusSeconds(2));
        seedExecution("exec-3", "bulk", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 2);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1", "exec-2");
        assertThat(stateOf("exec-3")).isEqualTo(ExecutionState.ENQUEUED);
    }

    @Test
    void claimOrdersHigherPriorityFirst() {
        seedJob("alerts", policy -> {
        });
        seedExecution("exec-low", "alerts", NOW.minusSeconds(10), Priority.LOW);
        seedExecution("exec-critical", "alerts", NOW.minusSeconds(1), Priority.CRITICAL);
        seedExecution("exec-normal", "alerts", NOW.minusSeconds(5), Priority.NORMAL);

        List<Execution> claimed = claimer.claim("node-a", 2);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-critical", "exec-normal");
        assertThat(stateOf("exec-low")).isEqualTo(ExecutionState.ENQUEUED);
    }

    /**
     * TEST-4: cobre as 5 prioridades explícitas (o teste irmão só exercita
     * LOW/NORMAL/CRITICAL) mais uma execução sem prioridade especificada no
     * mesmo lote, provando de uma vez a ordem inteira por {@code
     * Priority.value()} e que o DEFAULT do schema (20 = NORMAL) intercala
     * corretamente com as explícitas.
     */
    @Test
    void claimOrdersAllFivePrioritiesPlusDefaultAsNormal() {
        seedJob("alerts", policy -> {
        });
        seedExecution("exec-background", "alerts", NOW.minusSeconds(6), Priority.BACKGROUND);
        seedExecution("exec-low", "alerts", NOW.minusSeconds(5), Priority.LOW);
        seedExecutionWithDefaultPriority("exec-default", "alerts", NOW.minusSeconds(4));
        seedExecution("exec-high", "alerts", NOW.minusSeconds(3), Priority.HIGH);
        seedExecution("exec-critical", "alerts", NOW.minusSeconds(2), Priority.CRITICAL);
        seedExecution("exec-normal", "alerts", NOW.minusSeconds(1), Priority.NORMAL);

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly(
                "exec-critical", "exec-high", "exec-default", "exec-normal", "exec-low", "exec-background");
    }

    @Test
    void claimNeverClaimsTwoSiblingsOfTheSameJobInOneBatchWhenOverlapIsPrevented() {
        seedJob("welcome-email", PolicySpec::preventOverlap);
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(2));
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1");
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.ENQUEUED);
    }

    @Test
    void claimSkipsAJobThatAlreadyHasARunningExecution() {
        seedJob("welcome-email", PolicySpec::preventOverlap);
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(5));
        claimer.claim("node-a", 10);
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-b", 10);

        assertThat(claimed).isEmpty();
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.ENQUEUED);
    }

    @Test
    void claimAllowsMultipleRunningExecutionsByDefault() {
        seedJob("bulk", policy -> {
        });
        seedExecution("exec-1", "bulk", NOW.minusSeconds(2));
        seedExecution("exec-2", "bulk", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactlyInAnyOrder("exec-1", "exec-2");
    }

    @Test
    void claimStopsAtMaxConcurrentExecutionsButAllowsMoreAfterOneIsReleased() {
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

    @Test
    void claimRejectsNonPositiveBatchSize() {
        assertThatThrownBy(() -> claimer.claim("node-a", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * O teste mais importante desta etapa: duas transações concorrentes
     * disputando siblings do mesmo job com {@code preventOverlap()} ligado.
     * O CAS em {@code running_execution_count} (ADR-0018/0020) garante que,
     * seja qual for o entrelaçamento, exatamente uma sobrevive — nunca duas
     * {@code RUNNING} ao mesmo tempo, nunca as duas ficam de fora.
     */
    @Test
    void claimIsMutuallyExclusiveAcrossConcurrentNodes() throws Exception {
        seedJob("welcome-email", PolicySpec::preventOverlap);
        for (int i = 0; i < 5; i++) {
            seedExecution("exec-" + i, "welcome-email", NOW.minusSeconds(5 - i));
        }
        JdbcClaimer claimerA = newClaimer();
        JdbcClaimer claimerB = newClaimer();
        CyclicBarrier barrier = new CyclicBarrier(2);

        List<Execution> claimedA;
        List<Execution> claimedB;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<List<Execution>> futureA = executor.submit(() -> {
                barrier.await();
                return claimerA.claim("node-a", 5);
            });
            Future<List<Execution>> futureB = executor.submit(() -> {
                barrier.await();
                return claimerB.claim("node-b", 5);
            });

            claimedA = futureA.get(10, TimeUnit.SECONDS);
            claimedB = futureB.get(10, TimeUnit.SECONDS);
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(claimedA.size() + claimedB.size()).isEqualTo(1);
    }

    @Test
    void concurrentClaimsOnDisjointJobsDoNotInterfere() throws Exception {
        seedJob("job-a", policy -> {
        });
        seedJob("job-b", policy -> {
        });
        seedExecution("exec-a", "job-a", NOW.minusSeconds(1));
        seedExecution("exec-b", "job-b", NOW.minusSeconds(1));
        CyclicBarrier barrier = new CyclicBarrier(2);

        List<Execution> claimedA;
        List<Execution> claimedB;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<List<Execution>> futureA = executor.submit(() -> {
                barrier.await();
                return claimer.claim("node-a", 5);
            });
            Future<List<Execution>> futureB = executor.submit(() -> {
                barrier.await();
                return claimer.claim("node-b", 5);
            });

            claimedA = futureA.get(10, TimeUnit.SECONDS);
            claimedB = futureB.get(10, TimeUnit.SECONDS);
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        Set<String> allClaimedIds = Stream.concat(claimedA.stream(), claimedB.stream())
                .map(e -> e.id().value())
                .collect(Collectors.toSet());
        assertThat(claimedA.size() + claimedB.size()).isEqualTo(2);
        assertThat(allClaimedIds).containsExactlyInAnyOrder("exec-a", "exec-b");
    }
}
