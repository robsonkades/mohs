package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.read.ListAppender;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.schedule.Schedule;
import io.mohs.store.jdbc.JdbcClaimer;
import io.mohs.store.jdbc.JdbcExecutionStore;
import io.mohs.store.jdbc.JdbcRateLimitStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcNodeStore;
import io.mohs.store.jdbc.JdbcReaper;
import io.mohs.store.jdbc.JdbcTimestamps;
import io.mohs.store.jdbc.JdbcTriggerFirer;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class EngineTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final int BATCH_SIZE = 10;

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcJobStore jobStore;
    private JdbcExecutionStore executionStore;
    private JdbcNodeStore nodeStore;
    private HandlerRegistry handlerRegistry;

    private final ch.qos.logback.classic.Logger engineLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Engine.class);

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        jobStore = new JdbcJobStore(dataSource, clock);
        executionStore = new JdbcExecutionStore(dataSource, clock, JsonMapper.builder().build(), new H2JdbcDialect());
        nodeStore = new JdbcNodeStore(dataSource);
        handlerRegistry = new HandlerRegistry();
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:engine-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private Engine newEngine(NodeStore nodeStoreOverride, List<ExecutionListener> listeners) {
        return newEngine(nodeStoreOverride, listeners, defaultRunnerRegistry());
    }

    private Engine newEngine(NodeStore nodeStoreOverride, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry) {
        return newEngine(nodeStoreOverride, listeners, runnerRegistry, new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));
    }

    /** O claimer nasce com o MESMO {@code leaseTtl} das settings — lease de claim e de renovação nunca divergem, igual à auto-config real. */
    private Engine newEngine(NodeStore nodeStoreOverride, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(newClaimer(settings.leaseTtl()), nodeStoreOverride, listeners, runnerRegistry, settings);
    }

    /** Montagem comum aos helpers que só variam claimer/node store: reaper, dispatcher e tick scheduler reais sobre as portas do fixture. */
    private Engine assembleEngine(Claimer claimer, NodeStore nodeStoreOverride, List<ExecutionListener> listeners,
            RunnerRegistry runnerRegistry, EngineSettings settings) {
        JdbcReaper reaper = new JdbcReaper(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, LEASE_TTL);
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        EngineMetrics metrics = new EngineMetrics(new SimpleMeterRegistry());
        Dispatcher dispatcher = new Dispatcher(executionStore, jobStore, handlerRegistry, clock, List.of(), listeners, eventExecutor, metrics);
        ThreadPoolTaskScheduler tickScheduler = MohsExecutors.scheduler("mohs-engine-tick-test", 1);
        return new Engine(claimer, dispatcher, executionStore, jobStore, nodeStoreOverride, reaper,
                new JdbcTriggerFirer(dataSource, executionStore), clock, settings, tickScheduler, runnerRegistry, metrics);
    }

    private JdbcClaimer newClaimer(Duration leaseTtl) {
        return new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, leaseTtl, new ExecutionWindowRegistry(List.of()), new JdbcRateLimitStore(dataSource, clock));
    }

    private static RunnerRegistry defaultRunnerRegistry() {
        return new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build()));
    }

    /**
     * Engine com claimer e node store gravando uma trilha única de
     * {@code tick}/{@code claim:N} — como os dois só rodam na thread do
     * tick, a ordem da trilha É a estrutura do tick, e prova quantos
     * rounds de claim aconteceram DENTRO de cada um (ADR-0040).
     */
    private Engine newEngineWithTickTrace(List<String> trace, CountingNodeStore counting, List<ExecutionListener> listeners, EngineSettings settings) {
        JdbcClaimer realClaimer = newClaimer(settings.leaseTtl());
        Claimer tracingClaimer = (nodeId, batchSize) -> {
            List<Execution> claimed = realClaimer.claim(nodeId, batchSize);
            trace.add("claim:" + claimed.size());
            return claimed;
        };
        NodeStore tracingNodeStore = new NodeStore() {
            @Override
            public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
                trace.add("tick");
                counting.heartbeat(nodeId, state, epoch, at, expiresAt);
            }

            @Override
            public List<StoredNode> findAll() {
                return counting.findAll();
            }

            @Override
            public int deleteHeartbeatsBefore(Instant cutoff) {
                return counting.deleteHeartbeatsBefore(cutoff);
            }
        };
        return assembleEngine(tracingClaimer, tracingNodeStore, listeners, defaultRunnerRegistry(), settings);
    }

    /** Janelas completas da trilha: claims agrupados por tick, descartando a janela ainda aberta no fim. */
    private static List<List<Integer>> claimsPerTick(List<String> trace) {
        List<String> snapshot;
        synchronized (trace) {
            snapshot = List.copyOf(trace);
        }
        List<List<Integer>> ticks = new ArrayList<>();
        List<Integer> current = null;
        for (String entry : snapshot) {
            if (entry.equals("tick")) {
                current = new ArrayList<>();
                ticks.add(current);
            } else if (current != null) {
                current.add(Integer.parseInt(entry.substring("claim:".length())));
            }
        }
        if (!ticks.isEmpty()) {
            ticks.removeLast();
        }
        return ticks;
    }

    /** Claim/reclaim usam as portas reais (o claim precisa funcionar); os overrides simulam falha só no caminho do dispatch. */
    private Engine newEngineWith(JobStore jobStoreOverride, ExecutionStore executionStoreOverride, List<ExecutionListener> listeners) {
        return newEngineWith(jobStoreOverride, executionStoreOverride, listeners, nodeStore);
    }

    private Engine newEngineWith(JobStore jobStoreOverride, ExecutionStore executionStoreOverride, List<ExecutionListener> listeners,
            NodeStore nodeStoreOverride) {
        JdbcClaimer claimer = newClaimer(LEASE_TTL);
        JdbcReaper reaper = new JdbcReaper(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, LEASE_TTL);
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        EngineMetrics metrics = new EngineMetrics(new SimpleMeterRegistry());
        Dispatcher dispatcher = new Dispatcher(executionStoreOverride, jobStoreOverride, handlerRegistry, clock, List.of(), listeners, eventExecutor, metrics);
        ThreadPoolTaskScheduler tickScheduler = MohsExecutors.scheduler("mohs-engine-tick-test", 1);
        return new Engine(claimer, dispatcher, executionStoreOverride, jobStoreOverride, nodeStoreOverride, reaper,
                new JdbcTriggerFirer(dataSource, executionStore), clock,
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL), tickScheduler, defaultRunnerRegistry(), metrics);
    }

    private void seedEnqueuedExecution(String id, String jobKey, Object payload) {
        seedEnqueuedExecution(id, jobKey, payload, NOW.minusSeconds(1), null);
    }

    private void seedEnqueuedExecution(String id, String jobKey, Object payload, Instant scheduledAt) {
        seedEnqueuedExecution(id, jobKey, payload, scheduledAt, null);
    }

    private void seedEnqueuedExecution(String id, String jobKey, Object payload, Instant scheduledAt, @Nullable String runner) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> {
            PolicySpec policySpec = spec.onDemand();
            if (runner != null) {
                policySpec.runner(runner);
            }
        }));
        Execution execution = new Execution(ExecutionId.of(id), JobKey.of(jobKey), ExecutionState.ENQUEUED, scheduledAt, null, List.of(), "test");
        executionStore.insert(execution, payload);
    }

    private ExecutionState stateOf(String id) {
        return ExecutionState.valueOf(rawJdbcTemplate.queryForObject("SELECT state FROM mohs_executions WHERE id = ?", String.class, id));
    }

    private int countByState(ExecutionState state) {
        Integer count = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_executions WHERE state = ?", Integer.class, state.name());
        return count == null ? 0 : count;
    }

    @Test
    void startClaimsAndDispatchesUntilSuccess() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /** ADR-0035, ponta a ponta: trigger devido materializa a ocorrência no tick, o claim do MESMO tick reivindica e o dispatch executa — e o trigger avança na série. */
    @Test
    void recurringJobFiresWhenDueAndAdvancesTheTrigger() throws Exception {
        jobStore.upsert(JobDefinition.of("poll", Handler.class, spec -> spec.every(Duration.ofSeconds(10))));
        handlerRegistry.register(JobKey.of("poll"), (payload, ctx) -> {
        });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        // o disparo armado no upsert (NOW+10s) fica devido, 5s de atraso — dentro do threshold, dispara normal
        clock.advance(Duration.ofSeconds(15));
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        // exatamente uma ocorrência (relógio congelado em NOW+15s: a próxima, NOW+20s, ainda não é devida)
        String actor = rawJdbcTemplate.queryForObject(
                "SELECT actor FROM mohs_executions WHERE job_key = 'poll'", String.class);
        assertThat(actor).isEqualTo(Execution.SCHEDULER_ACTOR);
        Instant scheduledAt = JdbcTimestamps.fromUtcLocalDateTime(rawJdbcTemplate.queryForObject(
                "SELECT scheduled_at FROM mohs_executions WHERE job_key = 'poll'", LocalDateTime.class));
        assertThat(scheduledAt).isEqualTo(NOW.plusSeconds(10)); // identidade da ocorrência, não o instante da inserção
        Instant nextFireAt = JdbcTimestamps.fromUtcLocalDateTime(rawJdbcTemplate.queryForObject(
                "SELECT next_fire_at FROM mohs_job_definitions WHERE job_key = 'poll'", LocalDateTime.class));
        assertThat(nextFireAt).isEqualTo(NOW.plusSeconds(20));
    }

    @Test
    void heartbeatReflectsCurrentStateAcrossPauseAndResume() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::state).containsExactly(EngineState.RUNNING);

            counting.resetLatch(new CountDownLatch(2));
            engine.pause();
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::state).containsExactly(EngineState.PAUSED);

            counting.resetLatch(new CountDownLatch(2));
            engine.resume();
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::state).containsExactly(EngineState.RUNNING);
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    @Test
    void pauseStopsNewClaimsUntilResumed() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            engine.pause();

            // Ticks são serializados por scheduleWithFixedDelay (o próximo só
            // agenda quando o anterior retorna). Dentro do tick, state.get() —
            // o gate do claim — vem ANTES do heartbeat: o 1º heartbeat fresco
            // pode ser do tick em voo que já leu RUNNING e ainda vai reivindicar.
            // O 2º heartbeat é necessariamente de um tick que só começou depois
            // do em-voo terminar, claim incluso — só então é seguro semear
            // (flake real observado com a semeadura antes deste marco).
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            seedEnqueuedExecution("exec-1", "welcome-email", "hello");
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.ENQUEUED);

            // sem listener aqui — a asserção final é sobre o estado persistido, não sobre evento;
            // esperar mais alguns ticks reais já é suficiente pra um handler no-op ser reivindicado e concluído.
            engine.resume();
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * ADR-0051: a liveness mora no NÓ — handler mais lento que a lease da
     * própria execução sobrevive enquanto o node ticka, sem nenhuma
     * renovação por execução. O avanço do relógio ultrapassa
     * {@code lease-ttl} de propósito: a lease da EXECUÇÃO vence e nada
     * acontece, porque o heartbeat roda ANTES do reaper no mesmo tick
     * (a ordem que mata o self-reap do S8) e a promessa do nó segue fresca.
     */
    @Test
    void aHandlerOutlivingItsExecutionLeaseSurvivesWhileTheNodeTicks() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            Instant initialLease = leaseOf("exec-1");
            clock.advance(LEASE_TTL.plusSeconds(5));
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
            // a lease da execução venceu e NÃO foi renovada — quem mantém o
            // trabalho vivo é a promessa do nó, não esta coluna
            assertThat(leaseOf("exec-1")).isEqualTo(initialLease);
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * ADR-0039: o claim de cada tick é limitado pela folga de dispatch
     * ({@code dispatchConcurrency − in-flight}) — node saturado para de
     * reivindicar em vez de estourar o teto do runner (a rejeição deixava
     * a execução RUNNING presa até o reaper reclamá-la na lease — a
     * patologia medida no drain de 50k do BASELINE). O excedente fica
     * ENQUEUED no banco, reivindicável por qualquer node com folga.
     */
    @Test
    void claimIsBoundedByTheFreeDispatchCapacity() throws Exception {
        for (int i = 1; i <= 5; i++) {
            seedEnqueuedExecution("exec-" + i, "welcome-email", "hello");
        }
        int dispatchConcurrency = 2;
        CountDownLatch handlersStarted = new CountDownLatch(dispatchConcurrency);
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch allSucceeded = new CountDownLatch(5);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlersStarted.countDown();
            releaseHandlers.await(10, TimeUnit.SECONDS);
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                allSucceeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of(listener), defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, dispatchConcurrency, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(handlersStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // >= 2 ticks completos com o node saturado: folga 0, nenhum claim novo pode acontecer
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(countByState(ExecutionState.RUNNING)).isEqualTo(dispatchConcurrency);
            assertThat(countByState(ExecutionState.ENQUEUED)).isEqualTo(3);

            releaseHandlers.countDown();
            // com a folga de volta, os ticks seguintes drenam o excedente
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(countByState(ExecutionState.SUCCEEDED)).isEqualTo(5);
    }

    /**
     * ADR-0033/M3, o caminho de falha do caminho de falha, de ponta a ponta:
     * execução FAILED com attempts 1..2 já gravados é rearmada manualmente,
     * o claim a reivindica, o attempt 3 grava SEM colisão de PK
     * (execution_id, number) e, com o orçamento já exaurido, a nova falha
     * termina FAILED terminal — o retry manual compra exatamente uma
     * tentativa, nunca um loop.
     */
    @Test
    void aManuallyRearmedExecutionRunsOnceMoreAndFailsTerminally() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            throw new RuntimeException("still broken");
        });
        rawJdbcTemplate.update("UPDATE mohs_executions SET state = 'FAILED' WHERE id = 'exec-1'");
        for (int number = 1; number <= 2; number++) {
            rawJdbcTemplate.update(
                    "INSERT INTO mohs_attempts (execution_id, number, started_at, finished_at, outcome, error) VALUES (?, ?, ?, ?, 'FAILED', 'boom')",
                    "exec-1", number, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        }
        CountDownLatch failedTerminally = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Failed failed && failed.attemptsExhausted()) {
                failedTerminally.countDown();
            }
        };
        assertThat(executionStore.rearmForManualRetry(ExecutionId.of("exec-1"), NOW)).isTrue();
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failedTerminally.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Integer attempts = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_attempts WHERE execution_id = 'exec-1'", Integer.class);
        assertThat(attempts).isEqualTo(3);
    }

    /**
     * ADR-0041: shutdown gracioso escreve o último heartbeat como STOPPED —
     * sem ele, stop limpo e crash ficavam indistinguíveis no banco (linha
     * RUNNING para sempre). Poll inalcançável de propósito: o primeiro tick
     * roda imediatamente e é o ÚNICO — determinístico contra a corrida
     * (aceita e documentada) de um tick em voo commitar depois do write
     * final.
     */
    @Test
    void stopWritesAFinalStoppedHeartbeat() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(1));
        Engine engine = newEngine(counting, List.of(), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofMinutes(5), BATCH_SIZE, LEASE_TTL));
        engine.start();
        assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

        engine.stop(Duration.ofSeconds(5));

        assertThat(nodeStore.findAll()).singleElement()
                .extracting(StoredNode::state).isEqualTo(EngineState.STOPPED);
    }

    /** ADR-0041: o heartbeat final é best-effort — banco fora no shutdown vira WARN, nunca falha o stop. */
    @Test
    void stopCompletesEvenWhenTheFinalHeartbeatWriteFails() throws Exception {
        NodeStore blinkingStore = mock(NodeStore.class, delegatesTo(nodeStore));
        // o purge é a ÚLTIMA chamada de NodeStore do tick — esperar por ele
        // garante que a thread do tick não toca mais o mock durante o
        // stubbing abaixo (Mockito detecta a interleaving como UnfinishedStubbing)
        CountDownLatch tickDone = new CountDownLatch(1);
        NodeStore tickCompletionProbe = new NodeStore() {
            @Override
            public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
                blinkingStore.heartbeat(nodeId, state, epoch, at, expiresAt);
            }

            @Override
            public List<StoredNode> findAll() {
                return blinkingStore.findAll();
            }

            @Override
            public int deleteHeartbeatsBefore(Instant cutoff) {
                int purged = blinkingStore.deleteHeartbeatsBefore(cutoff);
                tickDone.countDown();
                return purged;
            }
        };
        Engine engine = newEngine(tickCompletionProbe, List.of(), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofMinutes(5), BATCH_SIZE, LEASE_TTL));
        engine.start();
        assertThat(tickDone.await(5, TimeUnit.SECONDS)).isTrue();
        doThrow(new DataAccessResourceFailureException("database down during shutdown"))
                .when(blinkingStore).heartbeat(any(), any(), anyLong(), any(), any());

        engine.stop(Duration.ofSeconds(5));

        assertThat(engine.state()).isEqualTo(EngineState.STOPPED);
    }

    /** ADR-0041: heartbeat mais velho que 10× lease-ttl é purgado de carona no tick — cada boot gera node_id novo, e sem purge cada instância morta deixava uma linha órfã para sempre. */
    @Test
    void tickPurgesNodeRowsWithStaleHeartbeats() throws Exception {
        Instant staleAt = NOW.minus(LEASE_TTL.multipliedBy(10)).minusSeconds(1);
        nodeStore.heartbeat("dead-node", EngineState.RUNNING, 1, staleAt, staleAt.plus(LEASE_TTL));
        nodeStore.heartbeat("recent-node", EngineState.RUNNING, 1, NOW.minusSeconds(1), NOW.minusSeconds(1).plus(LEASE_TTL));
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::nodeId)
                    .doesNotContain("dead-node")
                    .contains("recent-node");
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /**
     * ADR-0040: com {@code claimRounds > 1}, um MESMO tick encadeia claims
     * enquanto o lote voltar cheio — limitado pelo número de rounds e pela
     * folga de dispatch (ADR-0039), que encolhe a cada round. A trilha
     * tick/claim gravada pelos wrappers prova o formato por dentro:
     * [2,2] no tick saturador, [2] quando só resta folga pra um round,
     * [] com o node cheio.
     */
    @Test
    void aFullBatchChainsAnotherClaimRoundWithinTheSameTick() throws Exception {
        for (int i = 1; i <= 6; i++) {
            seedEnqueuedExecution("exec-" + i, "welcome-email", "hello");
        }
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch allSucceeded = new CountDownLatch(6);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> releaseHandlers.await(10, TimeUnit.SECONDS));
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                allSucceeded.countDown();
            }
        };
        List<String> trace = Collections.synchronizedList(new ArrayList<>());
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(4));
        Engine engine = newEngineWithTickTrace(trace, counting, List.of(listener),
                new EngineSettings(POLL_INTERVAL, 2, 6, 2, LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            List<List<Integer>> ticks = claimsPerTick(trace);

            assertThat(ticks.get(0)).containsExactly(2, 2); // rounds encadeados no mesmo tick, teto claimRounds=2
            assertThat(ticks.get(1)).containsExactly(2);    // folga de dispatch (6−4) limita o round único
            assertThat(ticks.get(2)).isEmpty();             // node cheio: tick sem claim (ADR-0039)

            releaseHandlers.countDown();
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(countByState(ExecutionState.SUCCEEDED)).isEqualTo(6);
    }

    /** ADR-0040: lote que volta menor que o pedido encerra os rounds — o round seguinte seria um SELECT de fila já drenada. */
    @Test
    void aShortBatchEndsTheClaimRoundsEarly() throws Exception {
        for (int i = 1; i <= 3; i++) {
            seedEnqueuedExecution("exec-" + i, "welcome-email", "hello");
        }
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch allSucceeded = new CountDownLatch(3);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> releaseHandlers.await(10, TimeUnit.SECONDS));
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                allSucceeded.countDown();
            }
        };
        List<String> trace = Collections.synchronizedList(new ArrayList<>());
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngineWithTickTrace(trace, counting, List.of(listener),
                new EngineSettings(POLL_INTERVAL, 2, 10, 3, LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            List<List<Integer>> ticks = claimsPerTick(trace);

            assertThat(ticks.get(0)).containsExactly(2, 1); // 3º round não acontece apesar de claimRounds=3

            releaseHandlers.countDown();
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(countByState(ExecutionState.SUCCEEDED)).isEqualTo(3);
    }

    /**
     * ADR-0051: posse perdida (reclaim externo) não tem mais detecção por
     * renovação — o zumbi termina sozinho e o resultado tardio é descartado
     * pelo FENCE do CAS: a linha volta a RUNNING com {@code fired_at} novo
     * (re-claim de outro nó), então SÓ o fence de posse — não o CAS de
     * estado — distingue a encarnação nova da velha. Sem o fence, a
     * conclusão zumbi venceria e mataria a encarnação nova saudável.
     */
    @Test
    void aZombieResultAfterAnExternalReclaimIsDiscardedByTheFencedCompletion() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        Engine engine = newEngine(nodeStore, List.of());
        Instant newIncarnationFiredAt = NOW.plusSeconds(7);

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // re-claim externo simulado: outro nó VIVO (senão o reaper deste
            // engine o declararia morto e reclamaria no meio do teste) já
            // reexecuta a mesma linha — RUNNING, posse de outra encarnação
            nodeStore.heartbeat("other-node", EngineState.RUNNING, 1, NOW, NOW.plusSeconds(3600));
            rawJdbcTemplate.update(
                    "UPDATE mohs_executions SET node_id = 'other-node', fired_at = ? WHERE id = 'exec-1'",
                    JdbcTimestamps.toUtcLocalDateTime(newIncarnationFiredAt));
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5)); // drena o handler — a conclusão tardia roda e perde o FENCE
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT node_id FROM mohs_executions WHERE id = 'exec-1'", String.class)).isEqualTo("other-node");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_attempts WHERE execution_id = 'exec-1'", Integer.class)).isZero();
    }

    /**
     * ADR-0034 fim-a-fim: o timeout do job dispara de carona no tick,
     * interrompe o handler bloqueado, e o desfecho é passivo — segue o
     * orçamento quando o handler responde (aqui retries=0 → FAILED com
     * causa de timeout).
     */
    @Test
    void jobTimeoutInterruptsTheHandlerAndTheOutcomeFollowsTheRetryBudget() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().timeout(Duration.ofMillis(50))));
        executionStore.insert(new Execution(ExecutionId.of("exec-1"), JobKey.of("welcome-email"),
                ExecutionState.ENQUEUED, NOW.minusSeconds(1), null, List.of(), "test"), "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            try {
                never.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
        });
        CountDownLatch failedPublished = new CountDownLatch(1);
        AtomicReference<Failed> failed = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failed.set(f);
                failedPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failedPublished.await(5, TimeUnit.SECONDS)).as("timeout interrupts and the attempt fails").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(interrupted).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(failed.get().error().getMessage()).contains("exceeded job timeout");
    }

    /**
     * API-DESIGN (shutdown gracioso, passo 3) / ADR-0034: grace estourado
     * escala pela maquinaria de cancelamento — flag + interrupt; o attempt
     * falha com causa NodeShutdown e segue o retry normal (aqui retries=0 →
     * FAILED). Durante o grace nada disso acontece: drain ≠ cancel.
     */
    @Test
    void drainGraceOverflowInterruptsInFlightWorkAndItFailsWithNodeShutdown() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            never.await(10, TimeUnit.SECONDS);
        });
        CountDownLatch failedPublished = new CountDownLatch(1);
        AtomicReference<Failed> failed = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failed.set(f);
                failedPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
        engine.stop(Duration.ofMillis(50));

        assertThat(failedPublished.await(5, TimeUnit.SECONDS)).as("escalation interrupts and the attempt fails").isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(failed.get().error().getMessage()).contains("node shutdown");
    }

    /**
     * ADR-0034 fim-a-fim: cancel manual gravado no banco (o que o POST
     * /executions/{id}/cancel faz de outro processo) é observado pelo tick
     * em ≤ 1 poll-interval — flag pura, sem interrupt (cancel é cooperativo
     * por contrato); o handler observa via JobContext, sai, e o desfecho é
     * CANCELLED com evento Cancelled.
     */
    @Test
    void manualCancelRequestedInTheDatabaseCancelsTheRunningExecution() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            while (!ctx.cancellationRequested()) {
                Thread.onSpinWait();
            }
            throw new IllegalStateException("stopping: cancellation observed");
        });
        CountDownLatch cancelledPublished = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Cancelled) {
                cancelledPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(executionStore.requestCancellation(ExecutionId.of("exec-1"))).isTrue();
            assertThat(cancelledPublished.await(5, TimeUnit.SECONDS)).as("tick observes the flag and the handler exits").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.CANCELLED);
    }

    private Instant leaseOf(String id) {
        return JdbcTimestamps.fromUtcLocalDateTime(rawJdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM mohs_executions WHERE id = ?", LocalDateTime.class, id));
    }

    /** Pendência 8 fechada: desfecho do reaper publica os mesmos eventos do dispatch — o alerta de morte de nó (Javadoc de Failed) passa a disparar de verdade. */
    @Test
    void reclaimOfADeadNodesExecutionPublishesRetryEvents() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1)));
        // execução de um nó morto: RUNNING com lease já expirada — o primeiro tick reclama
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, node_id, lease_expires_at, payload, payload_type, created_at)
                VALUES ('exec-1', 'welcome-email', 'RUNNING', ?, 'test', 'dead-node', ?, '"x"', 'java.lang.String', ?)
                """, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)),
                JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)), JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)));
        CountDownLatch retryScheduled = new CountDownLatch(1);
        CountDownLatch attemptFailed = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof RetryScheduled) {
                retryScheduled.countDown();
            }
            if (event instanceof AttemptFailed) {
                attemptFailed.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(attemptFailed.await(5, TimeUnit.SECONDS)).as("AttemptFailed published for the reclaim").isTrue();
            assertThat(retryScheduled.await(5, TimeUnit.SECONDS)).as("RetryScheduled published for the reclaim").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        // o estado final não é assertado: após o reclaim, o retry pode ser re-reivindicado
        // no mesmo teste (jitter pode ser ~0) — o contrato sob teste são os eventos
    }

    /** ADR-0034 no caminho de crash-recovery: reclaim de execução de nó morto com cancel pendente publica Cancelled — nem retry, nem Failed; a ordem do operador sobrevive à morte do nó. */
    @Test
    void reclaimOfACancelRequestedExecutionPublishesCancelled() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1)));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, node_id, lease_expires_at, cancel_requested, payload, payload_type, created_at)
                VALUES ('exec-1', 'welcome-email', 'RUNNING', ?, 'test', 'dead-node', ?, TRUE, '"x"', 'java.lang.String', ?)
                """, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)),
                JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)), JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)));
        CountDownLatch cancelledPublished = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Cancelled) {
                cancelledPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(cancelledPublished.await(5, TimeUnit.SECONDS)).as("Cancelled published for the reclaim").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.CANCELLED);
    }

    /**
     * Review ADR-0034: zumbi (posse perdida por reclaim externo) continua
     * no mapa de in-flight até a conclusão — e por isso a escalada do
     * shutdown ainda o interrompe (pra job sem timeout, a única chance de
     * pará-lo antes de a JVM morrer). O resultado tardio é descartado pelo
     * CAS, como todo zumbi.
     */
    @Test
    void aZombieAfterAnExternalReclaimStillReceivesTheShutdownInterrupt() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            try {
                never.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        });
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // reclaim externo: a posse foi perdida, mas a entrada fica no mapa até a conclusão
            rawJdbcTemplate.update(
                    "UPDATE mohs_executions SET state = 'RETRY_SCHEDULED', node_id = NULL, scheduled_at = ? WHERE id = 'exec-1'",
                    JdbcTimestamps.toUtcLocalDateTime(NOW.plusSeconds(3600)));
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofMillis(50));
        }

        assertThat(interrupted.await(5, TimeUnit.SECONDS)).as("escalation still reaches the dropped zombie").isTrue();
    }

    /** Drain ≠ cancel (ADR-0007): o in-flight continua executando em PAUSED — a promessa de liveness do NÓ (ADR-0051) tem que acompanhar o TRABALHO, não o modo do control loop; sem isto, pause/drain mais longo que a lease do nó vira dupla execução do que o próprio drain espera. */
    @Test
    void theNodeLeaseKeepsBeingPromisedWhilePaused() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            engine.pause();
            Instant initialExpiry = nodeLeaseExpiryOfTheOnlyNode();
            clock.advance(Duration.ofSeconds(5));
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(nodeLeaseExpiryOfTheOnlyNode()).isAfter(initialExpiry);
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
            engine.resume();
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private Instant nodeLeaseExpiryOfTheOnlyNode() {
        List<StoredNode> nodes = nodeStore.findAll();
        assertThat(nodes).hasSize(1);
        Instant expiresAt = nodes.get(0).expiresAt();
        assertThat(expiresAt).isNotNull();
        return expiresAt;
    }

    /** Bound menor/igual à lease do NÓ liberaria posse antes de o node sequer poder ser considerado morto (ADR-0051) — rejeitado na construção, nomeando as duas propriedades. */
    @Test
    void watchdogTimeoutMustExceedNodeLeaseTtl() {
        assertThatThrownBy(() -> new EngineSettings(POLL_INTERVAL, BATCH_SIZE, Duration.ofSeconds(30), Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mohs.engine.watchdog-timeout")
                .hasMessageContaining("mohs.engine.node-lease-ttl");
    }

    /**
     * Watchdog Bound pós-ADR-0051: passado o bound (tempo monotônico real,
     * ~200ms aqui), o node LIBERA a posse explicitamente — attempt
     * sintético consome o orçamento (retries = 0 → FAILED terminal) sem
     * reaper nem avanço de relógio envolvidos; o handler zumbi segue
     * rodando e seu resultado tardio é descartado pelo CAS cercado.
     */
    @Test
    void watchdogBoundReleasesOwnershipAndFailsTheExecution() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        CountDownLatch failedPublished = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Failed) {
                failedPublished.countDown();
            }
        };
        CountDownLatch boundWarned = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> watcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("watchdog-timeout")) {
                    boundWarned.countDown();
                }
            }
        };
        watcher.start();
        engineLogger.addAppender(watcher);
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of(listener), defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, Duration.ofMillis(50), Duration.ofMillis(200)));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(boundWarned.await(5, TimeUnit.SECONDS)).as("watchdog bound warned within timeout").isTrue();
            assertThat(failedPublished.await(5, TimeUnit.SECONDS)).as("ownership released as a terminal failure").isTrue();

            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
            engineLogger.detachAppender(watcher);
        }
        // o zumbi terminou no stop e o resultado tardio perdeu o CAS cercado
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
    }

    @Test
    void drainWaitsForInFlightDispatchWithoutCancellingIt() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(5, TimeUnit.SECONDS);
        });
        Engine engine = newEngine(nodeStore, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);

            CompletableFuture<Void> drainCall = CompletableFuture.runAsync(() -> engine.drain(Duration.ofSeconds(5)));
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING); // drain ≠ cancel — still running
            releaseHandler.countDown();
            drainCall.get(5, TimeUnit.SECONDS);
        } finally {
            engine.stop(Duration.ZERO);
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    @Test
    void unreadablePayloadFailsTheExecutionWithoutHangingTheTick() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_executions (
                    id, job_key, state, scheduled_at, actor, payload, payload_type, created_at)
                VALUES ('exec-1', 'welcome-email', 'ENQUEUED', ?, 'test', '{}', 'com.example.DoesNotExist', ?)
                """, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)), JdbcTimestamps.toUtcLocalDateTime(NOW));
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Failed> failedEvent = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failedEvent.set(f);
                failed.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(handlerCalled.get()).isFalse();
        Execution found = executionStore.find(ExecutionId.of("exec-1")).orElseThrow();
        assertThat(found.attempts().get(0).error()).contains("payload could not be read");
        // RESP-3 (docs/codereview-naming.md): payload ilegível também publica Failed, mesmo caminho de qualquer outra falha terminal.
        assertThat(failedEvent.get()).isNotNull();
        assertThat(failedEvent.get().error()).hasMessageContaining("payload could not be read");
        // terminal por natureza, não por orçamento (ADR-0033): exhausted=false em falha pré-dispatch
        assertThat(failedEvent.get().attemptsExhausted()).isFalse();
    }

    @Test
    void executionDispatchesOnItsJobsNamedRunner() throws Exception {
        RunnerRegistry runnerRegistry = new RunnerRegistry(List.of(
                MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build(),
                MohsRunner.io("s3").maxConcurrent(4).build()));
        seedEnqueuedExecution("exec-1", "s3-upload", "hello", NOW.minusSeconds(1), "s3");
        AtomicReference<String> dispatchThreadName = new AtomicReference<>();
        handlerRegistry.register(JobKey.of("s3-upload"), (payload, ctx) -> dispatchThreadName.set(Thread.currentThread().getName()));
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener), runnerRegistry);

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(dispatchThreadName.get()).startsWith("mohs-runner-s3-");
    }

    /** Mesmo padrão de unreadablePayloadFailsTheExecutionWithoutHangingTheTick — falha só a execução, não o tick nem o node. */
    @Test
    void unknownRunnerFailsTheExecutionWithoutHangingTheTick() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello", NOW.minusSeconds(1), "ghost-runner");
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Failed> failedEvent = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failedEvent.set(f);
                failed.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(handlerCalled.get()).isFalse();
        assertThat(failedEvent.get()).isNotNull();
        assertThat(failedEvent.get().error()).hasMessageContaining("runner could not be resolved");
    }

    /**
     * Erro transitório de infra ao carregar a definição não é veredito sobre
     * a execução: falhar terminalmente violaria at-least-once (o runner pode
     * existir e o handler nunca rodou) — fica RUNNING até o reaper reclamá-la
     * na expiração da lease, mesmo caminho da rejeição do executor.
     */
    @Test
    void transientJobStoreErrorLeavesExecutionRunningForTheReaper() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        JobStore flakyJobStore = new FindOverridingJobStore(jobStore, key -> {
            throw new RuntimeException("simulated transient database error");
        });
        CountDownLatch leftForReaper = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> warnWatcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("could not load the job definition")) {
                    leftForReaper.countDown();
                }
            }
        };
        warnWatcher.start();
        engineLogger.addAppender(warnWatcher);
        Engine engine = newEngineWith(flakyJobStore, executionStore, List.of());

        engine.start();
        try {
            assertThat(leftForReaper.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
            engineLogger.detachAppender(warnWatcher);
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
        assertThat(handlerCalled.get()).isFalse();
    }

    /**
     * {@code Mohs.remove} entre claim e dispatch: a definição sumiu de
     * verdade — falha terminal com diagnóstico próprio, não o "runner could
     * not be resolved" (que apontaria o operador pro problema errado).
     */
    @Test
    void removedDefinitionFailsTheExecutionWithItsOwnError() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        JobStore removedBehindOurBack = new FindOverridingJobStore(jobStore, key -> Optional.empty());
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Failed> failedEvent = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failedEvent.set(f);
                failed.countDown();
            }
        };
        Engine engine = newEngineWith(removedBehindOurBack, executionStore, List.of(listener));

        engine.start();
        try {
            assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(handlerCalled.get()).isFalse();
        assertThat(failedEvent.get().error()).hasMessageContaining("was removed after this execution was claimed");
    }

    /**
     * A gravação da falha terminal (failBeforeDispatch) roda no for de
     * tick(): se ela própria lançar (banco, executor de eventos saturado), o
     * resto do lote ainda precisa ser despachado — sem a guarda, a exceção
     * abortava o for e exec-2, já RUNNING no banco, ficava órfã até a lease
     * expirar.
     */
    @Test
    void tickContinuesWhenRecordingATerminalFailureThrows() throws Exception {
        seedEnqueuedExecution("exec-1", "ghost-job", "hello", NOW.minusSeconds(2));
        seedEnqueuedExecution("exec-2", "welcome-email", "hello", NOW.minusSeconds(1));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> { });
        // exec-1: a definição some entre claim e dispatch, e a gravação da própria falha terminal quebra
        JobStore ghostJobStore = new FindOverridingJobStore(jobStore,
                key -> key.equals(JobKey.of("ghost-job")) ? Optional.empty() : jobStore.find(key));
        ExecutionStore failingCompleteStore = new CompleteThrowingExecutionStore(executionStore, ExecutionId.of("exec-1"));
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = newEngineWith(ghostJobStore, failingCompleteStore, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
    }

    /**
     * Engine.submitDispatch: item novo do achado "A" (docs/codereview-naming.md,
     * addendum 2026-08-14) — um {@code dispatchExecutor} saturado rejeita a
     * 2ª/3ª submissão do mesmo lote de claim; antes da correção, a exceção
     * síncrona de {@code CompletableFuture.runAsync} abortava o {@code for}
     * de {@code tick()} assim que a 1ª rejeição acontecia, deixando as
     * execuções seguintes do lote sem sequer tentar {@code submitDispatch}.
     * `exec-1` prende o único slot do executor (concorrência 1); `exec-2` e
     * `exec-3` são reivindicadas no mesmo lote e têm que ser rejeitadas
     * individualmente — as duas aparecem no log, não só a primeira, o que só
     * é possível se o loop continuou depois da rejeição de `exec-2`.
     */
    @Test
    void submitDispatchContinuesAfterDispatchExecutorRejectsOne() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello", NOW.minusSeconds(3));
        seedEnqueuedExecution("exec-2", "welcome-email", "hello", NOW.minusSeconds(2));
        seedEnqueuedExecution("exec-3", "welcome-email", "hello", NOW.minusSeconds(1));
        CountDownLatch releaseFirstHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> releaseFirstHandler.await(5, TimeUnit.SECONDS));

        // CountDownLatch, não ListAppender.list: o append() de um Appender roda na
        // thread do tick scheduler, esta asserção roda na thread do teste —
        // contar um latch dá a publicação segura (JCIP) que ler uma lista comum
        // concorrentemente não dá (ListAppender.list é um ArrayList cru).
        CountDownLatch exec2Rejected = new CountDownLatch(1);
        CountDownLatch exec3Rejected = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> rejectionWatcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                String message = event.getFormattedMessage();
                if (message.contains("exec-2")) {
                    exec2Rejected.countDown();
                }
                if (message.contains("exec-3")) {
                    exec3Rejected.countDown();
                }
            }
        };
        rejectionWatcher.start();
        engineLogger.addAppender(rejectionWatcher);

        // concorrência 1 de propósito — exec-1 esgota o único slot, forçando exec-2/exec-3 a rejeitar.
        RunnerRegistry oneSlotRunnerRegistry = new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(1).build()));
        Engine engine = newEngine(nodeStore, List.of(), oneSlotRunnerRegistry);

        engine.start();
        try {
            // as duas têm que disparar — só é possível se o loop de tick() continuou
            // depois da rejeição de exec-2 e chegou a tentar exec-3 também.
            assertThat(exec2Rejected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(exec3Rejected.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseFirstHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
            engineLogger.detachAppender(rejectionWatcher);
        }
    }

    /** Decorator que intercepta só {@link JobStore#find} — simula erro de infra ou remoção entre claim e dispatch sem tocar o resto da porta. */
    private static final class FindOverridingJobStore implements JobStore {
        private final JobStore delegate;
        private final Function<JobKey, Optional<StoredJob>> find;

        FindOverridingJobStore(JobStore delegate, Function<JobKey, Optional<StoredJob>> find) {
            this.delegate = delegate;
            this.find = find;
        }

        @Override
        public Optional<StoredJob> find(JobKey key) {
            return find.apply(key);
        }

        @Override
        public JobDefinition upsert(JobDefinition definition) {
            return delegate.upsert(definition);
        }

        @Override
        public Stream<StoredJob> findAll() {
            return delegate.findAll();
        }

        @Override
        public Stream<StoredJob> findAllAnnotationSourced() {
            return delegate.findAllAnnotationSourced();
        }

        @Override
        public List<StoredJob> findDueRecurring(Instant now, int limit) {
            return delegate.findDueRecurring(now, limit);
        }

        @Override
        public void armNextFire(JobKey key, Instant nextFireAt) {
            delegate.armNextFire(key, nextFireAt);
        }

        @Override
        public boolean reschedule(JobKey key, Schedule schedule) {
            return delegate.reschedule(key, schedule);
        }

        @Override
        public void markOrphaned(JobKey key) {
            delegate.markOrphaned(key);
        }

        @Override
        public void pause(JobKey key) {
            delegate.pause(key);
        }

        @Override
        public void resume(JobKey key) {
            delegate.resume(key);
        }

        @Override
        public void remove(JobKey key) {
            delegate.remove(key);
        }

        @Override
        public boolean tryIncrementRunningExecutions(JobKey key) {
            return delegate.tryIncrementRunningExecutions(key);
        }

        @Override
        public void decrementRunningExecutions(JobKey key) {
            delegate.decrementRunningExecutions(key);
        }

        @Override
        public void decrementRunningExecutions(JobKey key, int permits) {
            delegate.decrementRunningExecutions(key, permits);
        }
    }

    /** Decorator que faz {@link ExecutionStore#complete} lançar pra uma execução específica — simula o banco falhando na gravação da falha terminal. */
    private static final class CompleteThrowingExecutionStore implements ExecutionStore {
        private final ExecutionStore delegate;
        private final ExecutionId failingId;

        CompleteThrowingExecutionStore(ExecutionStore delegate, ExecutionId failingId) {
            this.delegate = delegate;
            this.failingId = failingId;
        }

        @Override
        public Completion complete(CompletionRequest request, JobStore jobStore) {
            if (request.id().equals(failingId)) {
                throw new RuntimeException("simulated database error completing " + request.id());
            }
            return delegate.complete(request, jobStore);
        }

        @Override
        public boolean cancelIfPending(ExecutionId id) {
            return delegate.cancelIfPending(id);
        }

        @Override
        public boolean requestCancellation(ExecutionId id) {
            return delegate.requestCancellation(id);
        }

        @Override
        public boolean rearmForManualRetry(ExecutionId id, Instant scheduledAt) {
            return delegate.rearmForManualRetry(id, scheduledAt);
        }

        @Override
        public Set<ExecutionId> findCancelRequested(List<ExecutionId> ids) {
            return delegate.findCancelRequested(ids);
        }

        @Override
        public Execution insert(Execution execution, Object payload) {
            return delegate.insert(execution, payload);
        }

        @Override
        public Optional<Execution> find(ExecutionId id) {
            return delegate.find(id);
        }

        @Override
        public Optional<Execution> findByIdempotencyKey(JobKey jobKey, String idempotencyKey) {
            return delegate.findByIdempotencyKey(jobKey, idempotencyKey);
        }

        @Override
        public Optional<Object> findPayload(ExecutionId id) {
            return delegate.findPayload(id);
        }

        @Override
        public PayloadBatch findPayloads(List<ExecutionId> ids) {
            return delegate.findPayloads(ids);
        }

        @Override
        public Map<ExecutionId, Completion> completeAll(List<CompletionRequest> requests, JobStore jobStore) {
            return delegate.completeAll(requests, jobStore);
        }

        @Override
        public List<Execution> findByIds(List<ExecutionId> ids) {
            return delegate.findByIds(ids);
        }

        @Override
        public Stream<Execution> findByJobKey(JobKey jobKey) {
            return delegate.findByJobKey(jobKey);
        }

        @Override
        public Stream<Execution> findAll() {
            return delegate.findAll();
        }

        @Override
        public List<Execution> findPage(JobKey jobKey, ExecutionState status, Instant from, Instant to, ExecutionId cursor, int limit) {
            return delegate.findPage(jobKey, status, from, to, cursor, limit);
        }

        @Override
        public Map<ExecutionState, Long> countActiveByState() {
            return delegate.countActiveByState();
        }

        @Override
        public Map<ExecutionState, Long> countTerminalOutcomesSince(Instant since) {
            return delegate.countTerminalOutcomesSince(since);
        }
    }

    /** Decorator só pra dar ao teste um jeito determinístico de esperar N ticks reais, sem Thread.sleep. */
    private static final class CountingNodeStore implements NodeStore {
        private final NodeStore delegate;
        private final AtomicReference<CountDownLatch> latch;

        CountingNodeStore(NodeStore delegate, CountDownLatch initial) {
            this.delegate = delegate;
            this.latch = new AtomicReference<>(initial);
        }

        void resetLatch(CountDownLatch newLatch) {
            latch.set(newLatch);
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.get().await(timeout, unit);
        }

        @Override
        public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
            delegate.heartbeat(nodeId, state, epoch, at, expiresAt);
            latch.get().countDown();
        }

        @Override
        public List<StoredNode> findAll() {
            return delegate.findAll();
        }

        @Override
        public int deleteHeartbeatsBefore(Instant cutoff) {
            return delegate.deleteHeartbeatsBefore(cutoff);
        }
    }
}
