package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.engine.RateLimitStore;
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
    private JdbcRateLimitStore rateLimitStore;
    private JdbcClaimer claimer;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new H2JdbcDialect());
        jobStore = new JdbcJobStore(dataSource, clock);
        rateLimitStore = new JdbcRateLimitStore(dataSource, clock);
        claimer = newClaimer();
    }

    private JdbcClaimer newClaimer() {
        return newClaimer(new ExecutionWindowRegistry(List.of()));
    }

    private JdbcClaimer newClaimer(ExecutionWindowRegistry windowRegistry) {
        return new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, LEASE_TTL, windowRegistry, rateLimitStore);
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
                """, id, jobKey, JdbcTimestamps.toUtcTimestamp(scheduledAt), priority.value(), JdbcTimestamps.toUtcTimestamp(NOW));
    }

    /** Omite a coluna {@code priority} pra exercitar o DEFAULT do schema (20 = NORMAL), não um valor explícito. */
    private void seedExecutionWithDefaultPriority(String id, String jobKey, Instant scheduledAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, 'ENQUEUED', ?, 'test', '{}', 'java.lang.Object', ?)
                """, id, jobKey, JdbcTimestamps.toUtcTimestamp(scheduledAt), JdbcTimestamps.toUtcTimestamp(NOW));
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
        assertThat(JdbcTimestamps.fromUtcTimestamp(leaseExpiresAt)).isEqualTo(NOW.plus(LEASE_TTL));
        // ADR-0047: fired_at nasce no CAS do claim — o round trip próprio do
        // markFired foi fundido aqui
        Timestamp firedAt = rawJdbcTemplate.queryForObject(
                "SELECT fired_at FROM mohs_executions WHERE id = ?", Timestamp.class, "exec-1");
        assertThat(JdbcTimestamps.fromUtcTimestamp(firedAt)).isEqualTo(NOW);
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

    /** ADR-0033 (destrava a ADR-0026): retry viaja pelo mesmo caminho do claim — RETRY_SCHEDULED com scheduled_at vencido é candidato como qualquer ENQUEUED. */
    @Test
    void claimsRetryScheduledExecutionWhoseTimeHasCome() {
        seedJob("report", policy -> {
        });
        seedRetryScheduledExecution("exec-1", "report", NOW.minusSeconds(5));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1");
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
    }

    /** O backoff é o scheduled_at da linha reagendada — retry futuro espera a vez, mesmo predicado de due dos ENQUEUED. */
    @Test
    void doesNotClaimRetryScheduledBeforeItsRetryTime() {
        seedJob("report", policy -> {
        });
        seedRetryScheduledExecution("exec-1", "report", NOW.plusSeconds(60));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).isEmpty();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RETRY_SCHEDULED);
    }

    private void seedRetryScheduledExecution(String id, String jobKey, Instant retryAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES (?, ?, 'RETRY_SCHEDULED', ?, 'test', '{}', 'java.lang.Object', ?)
                """, id, jobKey, JdbcTimestamps.toUtcTimestamp(retryAt), JdbcTimestamps.toUtcTimestamp(NOW));
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

    @Test
    void claimSkipsExecutionsInsideAnExcludedWindow() {
        ExecutionWindow maintenance = ExecutionWindow.named("maintenance").exclude(instant -> true).build();
        JdbcClaimer windowedClaimer = newClaimer(new ExecutionWindowRegistry(List.of(maintenance)));
        seedJob("welcome-email", policy -> policy.window("maintenance"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(1));

        List<Execution> claimed = windowedClaimer.claim("node-a", 10);

        assertThat(claimed).isEmpty();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.ENQUEUED);
    }

    @Test
    void claimClaimsTheExecutionOnceTheWindowStopsExcluding() {
        Instant windowEnd = NOW.plusSeconds(30);
        ExecutionWindow maintenance = ExecutionWindow.named("maintenance").exclude(instant -> instant.isBefore(windowEnd)).build();
        JdbcClaimer windowedClaimer = newClaimer(new ExecutionWindowRegistry(List.of(maintenance)));
        seedJob("welcome-email", policy -> policy.window("maintenance"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(1));
        assertThat(windowedClaimer.claim("node-a", 10)).isEmpty();

        clock.setTo(windowEnd);
        List<Execution> claimed = windowedClaimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1");
    }

    /** ADR-0042: o lote leva só o que o balde concede; o excedente fica ENQUEUED, sem transição nem attempt. */
    @Test
    void claimTakesOnlyWhatTheRateLimitGrants() {
        rateLimitStore.upsert(new RateLimit("smtp", 2, Duration.ofMinutes(1)));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(3));
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(2));
        seedExecution("exec-3", "welcome-email", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1", "exec-2");
        assertThat(stateOf("exec-3")).isEqualTo(ExecutionState.ENQUEUED);
    }

    /** Balde vazio não solta nada; passado um intervalo (30s para 2/min), o token seguinte libera exatamente uma execução. */
    @Test
    void claimResumesWhenTheBucketRefills() {
        rateLimitStore.upsert(new RateLimit("smtp", 2, Duration.ofMinutes(1)));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(3));
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(2));
        seedExecution("exec-3", "welcome-email", NOW.minusSeconds(1));
        claimer.claim("node-a", 10);
        assertThat(claimer.claim("node-a", 10)).isEmpty();

        clock.advance(Duration.ofSeconds(30));

        assertThat(claimer.claim("node-a", 10)).extracting(e -> e.id().value()).containsExactly("exec-3");
    }

    /** Job sem limite não paga pedágio NA ADMISSÃO — o balde do vizinho não o barra. Se a fase 2 do vizinho falhar, porém, ele é desfeito junto: o rollback é da rodada, não do candidato. */
    @Test
    void claimIgnoresTheRateLimitForJobsThatDoNotDeclareOne() {
        rateLimitStore.upsert(new RateLimit("smtp", 1, Duration.ofMinutes(1)));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp"));
        seedJob("report", policy -> {
        });
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(3));
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(2));
        seedExecution("exec-3", "report", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1", "exec-3");
    }

    /**
     * Dois limites no MESMO lote: a contabilidade é por limite (decisão 4 da
     * ADR-0042), não um saldo global — e os dois são consumidos na mesma
     * rodada, em ordem determinística de nome. Sem a separação por chave,
     * um limite generoso pagaria pelo vizinho apertado.
     */
    @Test
    void claimAccountsForEachRateLimitSeparatelyWithinTheSameBatch() {
        rateLimitStore.upsert(new RateLimit("smtp", 1, Duration.ofMinutes(1)));
        rateLimitStore.upsert(new RateLimit("sms", 2, Duration.ofMinutes(1)));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp"));
        seedJob("alert", policy -> policy.rateLimit("sms"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(5));
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(4));
        seedExecution("exec-3", "alert", NOW.minusSeconds(3));
        seedExecution("exec-4", "alert", NOW.minusSeconds(2));
        seedExecution("exec-5", "alert", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).extracting(e -> e.id().value()).containsExactly("exec-1", "exec-3", "exec-4");
        assertThat(rateLimitStore.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(0);
        assertThat(rateLimitStore.find("sms")).get().extracting(RateLimitSnapshot::available).isEqualTo(0);
    }

    /**
     * Lock ordering (JCIP §10.1.1): os limites são consumidos em ordem de
     * nome, sempre a mesma em todo nó — é o que impede dois nós com lotes
     * que cruzam os mesmos dois limites de deadlockar. Sem este teste,
     * trocar o {@code TreeMap} por {@code HashMap} mantém a suíte verde e
     * reintroduz o deadlock em silêncio.
     */
    @Test
    void consumesRateLimitsInDeterministicNameOrder() {
        List<String> consumedOrder = new ArrayList<>();
        JdbcClaimer orderRecordingClaimer = new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore,
                LEASE_TTL, new ExecutionWindowRegistry(List.of()), recordingConsumeOrder(consumedOrder));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp"));
        seedJob("alert", policy -> policy.rateLimit("sms"));
        seedJob("billing", policy -> policy.rateLimit("api"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(3));
        seedExecution("exec-2", "alert", NOW.minusSeconds(2));
        seedExecution("exec-3", "billing", NOW.minusSeconds(1));

        orderRecordingClaimer.claim("node-a", 10);

        assertThat(consumedOrder).containsExactly("api", "sms", "smtp");
    }

    /**
     * O ganho estrutural da revisão de duas fases: a cobrança é do que foi
     * REIVINDICADO, não do que foi admitido. Com {@code preventOverlap}, três
     * candidatos entram no lote e só um vence o mutex do job — o desenho de
     * fase única cobrava os três e queimava dois tokens em execuções que
     * nunca rodaram. Sem este teste, voltar a cobrar o admitido passa verde.
     */
    @Test
    void chargesOnlyTheExecutionsThatActuallyWonTheJobMutex() {
        rateLimitStore.upsert(new RateLimit("smtp", 5, Duration.ofMinutes(1)));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp").preventOverlap());
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(3));
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(2));
        seedExecution("exec-3", "welcome-email", NOW.minusSeconds(1));

        List<Execution> claimed = claimer.claim("node-a", 10);

        assertThat(claimed).hasSize(1);
        assertThat(rateLimitStore.find("smtp")).get().extracting(RateLimitSnapshot::available).isEqualTo(4);
    }

    /**
     * O caminho de desfazer a rodada (ADR-0042, fase 2): quando o balde não
     * cobre o que JÁ foi reivindicado, entregar seria sobre-entrega — a
     * única violação inaceitável. A transação inteira volta atrás, e é isso
     * que este teste prova: nenhuma execução fica {@code RUNNING}, todas
     * voltam a {@code ENQUEUED}. Sem ele, o rollback seria código que só
     * roda sob contenção e que ninguém nunca viu rodar.
     */
    @Test
    void aChargeThatCannotBePaidRollsTheWholeRoundBack() {
        JdbcClaimer starvedClaimer = new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore,
                LEASE_TTL, new ExecutionWindowRegistry(List.of()), refusesToCharge(new AtomicInteger()));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(2));
        seedExecution("exec-2", "welcome-email", NOW.minusSeconds(1));

        List<Execution> claimed = starvedClaimer.claim("node-a", 10);

        // rodada vazia, NÃO exceção: contenção de balde é o limitador
        // funcionando (ADR-0042 §7), e propagar viraria "engine tick failed"
        // com pilha em metade das rodadas do regime throttlado
        assertThat(claimed).isEmpty();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.ENQUEUED);
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.ENQUEUED);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_executions WHERE node_id IS NOT NULL", Integer.class)).isZero();
    }

    /**
     * O rollback tem que devolver TAMBÉM a vaga de concorrência do job — um
     * slot vazado tranca o job para sempre, e nada no sistema acusaria.
     * Analiticamente sai de graça ({@code tryIncrementRunningExecutions} é
     * UPDATE na mesma transação), mas raciocínio não é rede: trocar o store
     * por um não-transacional passaria verde sem esta asserção.
     */
    @Test
    void aChargeThatCannotBePaidGivesTheJobConcurrencySlotBack() {
        JdbcClaimer starvedClaimer = new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore,
                LEASE_TTL, new ExecutionWindowRegistry(List.of()), refusesToCharge(new AtomicInteger()));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp").preventOverlap());
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(1));

        assertThat(starvedClaimer.claim("node-a", 10)).isEmpty();

        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT running_execution_count FROM mohs_job_definitions WHERE job_key = 'welcome-email'",
                Integer.class)).isZero();
    }

    /**
     * Prova que o ramo retenta UMA vez (sem o backoff de deadlock — ver o
     * comentário do catch), não só desiste com o tipo certo:
     * a asserção anterior (por tipo de exceção) passava idêntica num mundo
     * sem retry nenhum — foi o que deixou o defeito do multi-catch escapar.
     */
    @Test
    void aFailedChargeIsRetriedBeforeTheRoundIsGivenUp() {
        AtomicInteger chargeCalls = new AtomicInteger();
        JdbcClaimer starvedClaimer = new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore,
                LEASE_TTL, new ExecutionWindowRegistry(List.of()), refusesToCharge(chargeCalls));
        seedJob("welcome-email", policy -> policy.rateLimit("smtp"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(1));

        starvedClaimer.claim("node-a", 10);

        assertThat(chargeCalls.get()).isEqualTo(2);
    }

    /** Fase 1 diz que cabe, fase 2 recusa — a corrida em que outro nó drenou o balde entre as duas; o contador registra quantas vezes o retry tentou. */
    private RateLimitStore refusesToCharge(AtomicInteger chargeCalls) {
        return new RateLimitStore() {

            @Override
            public RateLimit upsert(RateLimit rateLimit) {
                throw new UnsupportedOperationException("not used by claim");
            }

            @Override
            public int available(String name, Instant now) {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean charge(String name, int permits, Instant now) {
                chargeCalls.incrementAndGet();
                return false;
            }

            @Override
            public Optional<RateLimitSnapshot> find(String name) {
                throw new UnsupportedOperationException("not used by claim");
            }

            @Override
            public Stream<RateLimitSnapshot> findAll() {
                throw new UnsupportedOperationException("not used by claim");
            }
        };
    }

    /** Só registra a ordem e concede tudo — o que está sob teste é a SEQUÊNCIA das chamadas, não o balde. */
    private RateLimitStore recordingConsumeOrder(List<String> consumedOrder) {
        return new RateLimitStore() {

            @Override
            public RateLimit upsert(RateLimit rateLimit) {
                throw new UnsupportedOperationException("not used by claim");
            }

            @Override
            public int available(String name, Instant now) {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean charge(String name, int permits, Instant now) {
                consumedOrder.add(name);
                return true;
            }

            @Override
            public Optional<RateLimitSnapshot> find(String name) {
                throw new UnsupportedOperationException("not used by claim");
            }

            @Override
            public Stream<RateLimitSnapshot> findAll() {
                throw new UnsupportedOperationException("not used by claim");
            }
        };
    }

    /** Fail-safe da ADR-0042: limite inexistente barra o job em vez de deixá-lo rodar sem o limite que alguém pediu. */
    @Test
    void claimGrantsNothingToAJobPointingAtAnUndeclaredRateLimit() {
        seedJob("welcome-email", policy -> policy.rateLimit("ghost"));
        seedExecution("exec-1", "welcome-email", NOW.minusSeconds(1));

        assertThat(claimer.claim("node-a", 10)).isEmpty();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.ENQUEUED);
    }
}
