package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

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
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcHistoryStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcNodeStore;
import io.mohs.store.jdbc.JdbcRateLimitStore;
import io.mohs.store.jdbc.JdbcTimestamps;
import io.mohs.store.jdbc.JdbcTriggerFirer;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
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
    private JdbcHistoryStore historyStore;
    private JdbcWorkQueue workQueue;
    private JdbcLeaseStore leaseStore;
    private JdbcRateLimitStore rateLimitStore;
    private JdbcNodeStore nodeStore;
    private HandlerRegistry handlerRegistry;

    private final ch.qos.logback.classic.Logger engineLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Engine.class);

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        jobStore = new JdbcJobStore(dataSource, clock);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock);
        historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new H2JdbcDialect());
        workQueue = new JdbcWorkQueue(dataSource, new H2JdbcDialect(), batchStore);
        leaseStore = new JdbcLeaseStore(dataSource, new H2JdbcDialect(), batchStore);
        rateLimitStore = new JdbcRateLimitStore(dataSource, clock);
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

    private Engine newEngine(NodeStore nodeStoreOverride, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(workQueue, leaseStore, historyStore, nodeStoreOverride, listeners, runnerRegistry, settings);
    }

    /**
     * Montagem comum: dispatcher, trigger firer e tick scheduler reais sobre
     * as portas do fixture — os overrides simulam falha num único ponto
     * (fila, posse, história ou node) sem tocar o resto.
     */
    private Engine assembleEngine(WorkQueue workQueueOverride, LeaseStore leaseStoreOverride, HistoryStore historyStoreOverride,
            NodeStore nodeStoreOverride, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(jobStore, workQueueOverride, leaseStoreOverride, historyStoreOverride, nodeStoreOverride,
                listeners, runnerRegistry, settings);
    }

    private Engine assembleEngine(JobStore jobStoreOverride, WorkQueue workQueueOverride, LeaseStore leaseStoreOverride,
            HistoryStore historyStoreOverride, NodeStore nodeStoreOverride, List<ExecutionListener> listeners,
            RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(jobStoreOverride, workQueueOverride, leaseStoreOverride, historyStoreOverride,
                nodeStoreOverride, new ExecutionWindowRegistry(List.of()), listeners, runnerRegistry, settings);
    }

    private Engine assembleEngine(JobStore jobStoreOverride, WorkQueue workQueueOverride, LeaseStore leaseStoreOverride,
            HistoryStore historyStoreOverride, NodeStore nodeStoreOverride, ExecutionWindowRegistry windowRegistry,
            List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings) {
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        EngineMetrics metrics = new EngineMetrics(new SimpleMeterRegistry());
        Dispatcher dispatcher = new Dispatcher(leaseStoreOverride, jobStoreOverride, handlerRegistry, clock, List.of(), listeners, eventExecutor, metrics);
        ThreadPoolTaskScheduler tickScheduler = MohsExecutors.scheduler("mohs-engine-tick-test", 1);
        return new Engine(workQueueOverride, dispatcher, historyStoreOverride, leaseStoreOverride, jobStoreOverride, nodeStoreOverride,
                new JdbcTriggerFirer(dataSource, historyStore, workQueue), windowRegistry,
                rateLimitStore, clock, settings, tickScheduler, runnerRegistry, metrics);
    }

    private static RunnerRegistry defaultRunnerRegistry() {
        return new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build()));
    }

    /**
     * Engine com fila e node store gravando uma trilha única de
     * {@code tick}/{@code claim:N} — como os dois só rodam na thread do
     * tick, a ordem da trilha É a estrutura do tick, e prova quantos
     * rounds de claim aconteceram DENTRO de cada um (ADR-0040).
     */
    private Engine newEngineWithTickTrace(List<String> trace, CountingNodeStore counting, List<ExecutionListener> listeners, EngineSettings settings) {
        WorkQueue tracingQueue = new WorkQueue() {
            @Override
            public List<ClaimedWork> claim(int shard, String nodeId, long epoch, int limit, Collection<JobKey> inadmissible, Instant now) {
                List<ClaimedWork> claimed = workQueue.claim(shard, nodeId, epoch, limit, inadmissible, now);
                trace.add("claim:" + claimed.size());
                return claimed;
            }

            @Override
            public void offer(List<ReadyEntry> entries) {
                workQueue.offer(entries);
            }

            @Override
            public int requeue(List<Requeue> orders) {
                return workQueue.requeue(orders);
            }

            @Override
            public boolean cancelQueued(ExecutionId id, Instant now) {
                return workQueue.cancelQueued(id, now);
            }

            @Override
            public boolean rearmForManualRetry(ExecutionId id, Instant now) {
                return workQueue.rearmForManualRetry(id, now);
            }
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
        return assembleEngine(tracingQueue, leaseStore, historyStore, tracingNodeStore, listeners, defaultRunnerRegistry(), settings);
    }

    /**
     * Janelas completas da trilha: claims agrupados por tick, descartando a
     * janela ainda aberta no fim. Desde o lap da Phase 6 (§5.4) cada
     * statement sonda UM shard e o fixture semeia tudo no shard 0 — as
     * sondas vazias ({@code claim:0}) são só o lap circulando shards
     * possuídos-mas-vazios; a estrutura que o ADR-0040 afirma (quantos
     * lotes por tick, encadeamento, parada antecipada) é a dos claims que
     * acharam trabalho, então o zero sai da janela.
     */
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
                int claimed = Integer.parseInt(entry.substring("claim:".length()));
                if (claimed > 0) {
                    current.add(claimed);
                }
            }
        }
        if (!ticks.isEmpty()) {
            ticks.removeLast();
        }
        return ticks;
    }

    /** Sondas BRUTAS (vazias inclusive) do tick — pina a economia de SELECTs do ADR-0040, que o filtro de zeros acima esconderia. */
    private static long rawClaimStatementsInTick(List<String> trace, int tickIndex) {
        List<String> snapshot;
        synchronized (trace) {
            snapshot = List.copyOf(trace);
        }
        int tick = -1;
        long statements = 0;
        for (String entry : snapshot) {
            if (entry.equals("tick")) {
                tick++;
            } else if (tick == tickIndex) {
                statements++;
            }
        }
        return statements;
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
        recordAndOffer(id, jobKey, payload, scheduledAt);
    }

    /** A unidade de enqueue do §7.5-1 sem o upsert — pra teste que registra a definição por conta própria. */
    private void recordAndOffer(String id, String jobKey, Object payload, Instant scheduledAt) {
        recordAndOffer(id, jobKey, payload, scheduledAt, 0);
    }

    private void recordAndOffer(String id, String jobKey, Object payload, Instant scheduledAt, int shard) {
        historyStore.record(List.of(new HistoryStore.NewExecution(ExecutionId.of(id), JobKey.of(jobKey), shard, 20,
                scheduledAt, NOW, "test", null, null, payload)));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), shard, 20, 1, scheduledAt)));
    }

    /** Estado do read model derivado (§4.3): advisory + lease + fila — o que a API pública vê. */
    private ExecutionState stateOf(String id) {
        return historyStore.find(ExecutionId.of(id), clock.instant()).orElseThrow().state();
    }

    /** RUNNING derivado = posse viva ({@code mohs_lease}); ENQUEUED = entrada na fila. */
    private int leaseCount() {
        Integer count = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_lease", Integer.class);
        return count == null ? 0 : count;
    }

    private int readyCount() {
        Integer count = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_ready", Integer.class);
        return count == null ? 0 : count;
    }

    private int terminalCount(ExecutionState state) {
        Integer count = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_execution WHERE state = ?", Integer.class, state.name());
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

    /** ADR-0035, ponta a ponta: trigger devido materializa a ocorrência no tick (história + fila), o claim do MESMO tick reivindica e o dispatch executa — e o trigger avança na série. */
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
                "SELECT actor FROM mohs_execution WHERE job_key = 'poll'", String.class);
        assertThat(actor).isEqualTo(Execution.SCHEDULER_ACTOR);
        Instant scheduledAt = JdbcTimestamps.fromUtcLocalDateTime(rawJdbcTemplate.queryForObject(
                "SELECT scheduled_at FROM mohs_execution WHERE job_key = 'poll'", LocalDateTime.class));
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
     * ADR-0051: a liveness mora no NÓ — handler mais lento que qualquer TTL
     * sobrevive enquanto o node ticka, sem renovação por execução (a lease
     * da Phase 5 nem carrega expiração própria). O avanço do relógio
     * ultrapassa {@code lease-ttl} de propósito: nada reclama a posse,
     * porque o heartbeat roda ANTES do reaper no mesmo tick (a ordem que
     * mata o self-reap do S8) e a promessa do nó segue fresca.
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
            clock.advance(LEASE_TTL.plusSeconds(5));
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            // a posse continua deste node e nenhum attempt sintético foi gravado
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
            assertThat(rawJdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class)).isZero();
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * ADR-0039: o claim de cada tick é limitado pela folga de dispatch
     * ({@code dispatchConcurrency − in-flight}) — node saturado para de
     * reivindicar em vez de estourar o teto do runner. O excedente fica na
     * fila ({@code mohs_ready}), reivindicável por qualquer node com folga.
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

            assertThat(leaseCount()).isEqualTo(dispatchConcurrency);
            assertThat(readyCount()).isEqualTo(3);

            releaseHandlers.countDown();
            // com a folga de volta, os ticks seguintes drenam o excedente
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(5);
    }

    /**
     * A corrida do define+schedule no mesmo tick (bug real do flip — ~50%
     * de falha no E2E do starter que define e agenda em sequência): o
     * snapshot de definições do tick PRECEDE o claim, então um job
     * recém-nascido pode estar fora do snapshot com a entrada já na fila —
     * e ele NÃO é um job removido. O miss do snapshot cura com consulta
     * fresca ({@code jobStore.find}); sem ela, a execução morria FAILED
     * terminal com a mensagem de "removed". O jobStore cego de findAll
     * torna o miss determinístico.
     */
    @Test
    void aJobBornAfterTheTickSnapshotStillDispatches() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        JobStore snapshotBlind = mock(JobStore.class, delegatesTo(jobStore));
        doAnswer(_ -> java.util.stream.Stream.<StoredJob>empty()).when(snapshotBlind).findAll();
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = assembleEngine(snapshotBlind, workQueue, leaseStore, historyStore, nodeStore, List.of(listener),
                defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("a just-born job dispatches despite the stale snapshot").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * O complemento do heal (review S5.4): o achado da consulta fresca é
     * MEMOIZADO no snapshot — sem isso, a Admission das rodadas seguintes
     * do mesmo tick não veria as leases do recém-nascido ({@code leaseCount}
     * = 0) e o mutex de {@code maxConcurrentExecutions} viraria no-op, até
     * {@code claimRounds × cap} execuções concorrentes num só nó.
     * {@code batchSize=1 + claimRounds=2} força exatamente a rodada 2
     * dentro do tick de nascimento (o findAll cego só na primeira chamada).
     */
    @Test
    void aJobBornAfterTheSnapshotStillHonoursItsConcurrencyCapAcrossClaimRounds() throws Exception {
        jobStore.upsert(JobDefinition.of("single-file", Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(1)));
        recordAndOffer("exec-1", "single-file", "hello", NOW.minusSeconds(2));
        recordAndOffer("exec-2", "single-file", "hello", NOW.minusSeconds(1));
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch bothSucceeded = new CountDownLatch(2);
        handlerRegistry.register(JobKey.of("single-file"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandlers.await(10, TimeUnit.SECONDS);
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                bothSucceeded.countDown();
            }
        };
        JobStore blindOnce = mock(JobStore.class, delegatesTo(jobStore));
        AtomicBoolean firstSnapshot = new AtomicBoolean(true);
        doAnswer(_ -> firstSnapshot.getAndSet(false) ? java.util.stream.Stream.<StoredJob>empty() : jobStore.findAll())
                .when(blindOnce).findAll();
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = assembleEngine(blindOnce, workQueue, leaseStore, historyStore, counting, List.of(listener),
                defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, 1, 10, 2, LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            // a rodada 2 do tick cego NÃO furou o mutex: uma posse, um na fila
            assertThat(leaseCount()).isEqualTo(1);
            assertThat(readyCount()).isEqualTo(1);

            releaseHandlers.countDown();
            assertThat(bothSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt", Integer.class)).containsExactly(1, 1);
    }

    /**
     * Janela como segunda linha de defesa (review S5.4): job recém-nascido
     * (fora do snapshot) com janela FECHADA não roda — o {@code admitFor}
     * barra e devolve pra fila com o MESMO attempt. É o único guard entre a
     * fila e a janela quando o filtro pré-claim não conhece o job (ou foi
     * descartado no modo degradado do {@code MAX_INADMISSIBLE_FILTER}).
     */
    @Test
    void aJobBornAfterTheSnapshotInsideAClosedWindowIsRequeuedNotDispatched() throws Exception {
        jobStore.upsert(JobDefinition.of("night-batch", Handler.class, spec -> spec.onDemand().window("night")));
        recordAndOffer("exec-1", "night-batch", "hello", NOW.minusSeconds(1));
        AtomicBoolean handlerRan = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("night-batch"), (payload, ctx) -> handlerRan.set(true));
        JobStore alwaysBlind = mock(JobStore.class, delegatesTo(jobStore));
        // cego SEMPRE: toda rodada exercita o caminho do heal — o pior caso do guard
        doAnswer(_ -> java.util.stream.Stream.<StoredJob>empty()).when(alwaysBlind).findAll();
        ExecutionWindowRegistry closedWindow = new ExecutionWindowRegistry(
                List.of(new ExecutionWindow("night", List.of(_ -> true))));
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(3));
        Engine engine = assembleEngine(alwaysBlind, workQueue, leaseStore, historyStore, counting, closedWindow,
                List.of(), defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handlerRan.get()).isFalse();
            assertThat(leaseCount()).isZero();
            assertThat(readyCount()).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT attempt FROM mohs_ready", Integer.class)).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_attempt", Integer.class)).isZero();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /**
     * §5.4 — a corrida de admissão resolvida pós-claim: cap parcial (folga
     * 1, a rodada trouxe 2) admite um e devolve o outro pra fila com o
     * MESMO attempt e sem attempt sintético — perda de admissão nunca
     * consome orçamento. Enquanto a posse viva satura o cap, o devolvido
     * espera na fila (o guard o torna inadmissível nas rodadas seguintes);
     * liberado o cap, ele roda normalmente.
     */
    @Test
    void admissionCapOverflowRequeuesTheLoserWithoutBurningBudget() throws Exception {
        jobStore.upsert(JobDefinition.of("single-file", Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(1)));
        recordAndOffer("exec-1", "single-file", "hello", NOW.minusSeconds(2));
        recordAndOffer("exec-2", "single-file", "hello", NOW.minusSeconds(1));
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch bothSucceeded = new CountDownLatch(2);
        handlerRegistry.register(JobKey.of("single-file"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandlers.await(10, TimeUnit.SECONDS);
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                bothSucceeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of(listener));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            // um rodando, um de volta na fila — attempt intacto, nenhum attempt gravado
            assertThat(leaseCount()).isEqualTo(1);
            assertThat(readyCount()).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT attempt FROM mohs_ready", Integer.class)).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_attempt", Integer.class)).isZero();

            releaseHandlers.countDown();
            assertThat(bothSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        // cada um rodou exatamente uma vez — a perda de admissão não virou attempt
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt", Integer.class)).containsExactly(1, 1);
    }

    /**
     * ADR-0033/M3, o caminho de falha do caminho de falha, de ponta a ponta:
     * execução FAILED com attempts 1..2 já gravados é rearmada manualmente
     * ({@code WorkQueue.rearmForManualRetry}: advisory volta a PENDING e a
     * fila ganha a entrada do attempt 3 = COUNT(attempts)+1), o claim a
     * reivindica, o attempt 3 grava SEM colisão de PK e, com o orçamento já
     * exaurido, a nova falha termina FAILED terminal — o retry manual
     * compra exatamente uma tentativa, nunca um loop.
     */
    @Test
    void aManuallyRearmedExecutionRunsOnceMoreAndFailsTerminally() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = 'exec-1'");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            throw new RuntimeException("still broken");
        });
        rawJdbcTemplate.update("UPDATE mohs_execution SET state = 'FAILED' WHERE execution_id = 'exec-1'");
        for (int number = 1; number <= 2; number++) {
            rawJdbcTemplate.update(
                    "INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error) VALUES (?, ?, 'node-a', ?, ?, 'FAILED', 'java.lang.RuntimeException', 'boom')",
                    "exec-1", number, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        }
        CountDownLatch failedTerminally = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Failed failed && failed.attemptsExhausted()) {
                failedTerminally.countDown();
            }
        };
        assertThat(workQueue.rearmForManualRetry(ExecutionId.of("exec-1"), NOW)).isTrue();
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failedTerminally.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Integer attempts = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class);
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
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(6);
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
            // economia bruta: lap 1 para na 1ª sonda (orçamento cheio), lap 2 dá a
            // volta (64) e não enche → SEM lap 3 — um lap extra de sondas vazias
            // estouraria este teto
            assertThat(rawClaimStatementsInTick(trace, 0)).isLessThanOrEqualTo(1 + Shards.SHARD_COUNT);

            releaseHandlers.countDown();
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(3);
    }

    /**
     * §8.3/S6.1: com um par RUNNING vivo, a partição derivada dos node ids
     * ordenados dá a cada node metade dos shards — este node NÃO reivindica
     * dos shards do par, mesmo com a execução devida e folga de sobra.
     * "zzz-peer" ordena depois do node_id UUID deste engine, então este
     * node é o índice 0 e possui os shards PARES.
     */
    @Test
    void shardOwnershipLeavesThePeersShardsUnclaimed() throws Exception {
        nodeStore.heartbeat("zzz-peer", EngineState.RUNNING, 1, NOW, NOW.plusSeconds(3600));
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        recordAndOffer("exec-mine", "welcome-email", "hello", NOW.minusSeconds(1), 0);
        recordAndOffer("exec-peers", "welcome-email", "hello", NOW.minusSeconds(1), 1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(1));
        Engine engine = newEngine(counting, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
            // mais dois ticks completos DEPOIS do sucesso: a chance que o node
            // teria de reivindicar o shard alheio, se fosse reivindicar
            CountDownLatch twoMoreTicks = new CountDownLatch(2);
            counting.resetLatch(twoMoreTicks);
            assertThat(twoMoreTicks.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(stateOf("exec-mine")).isEqualTo(ExecutionState.SUCCEEDED);
            assertThat(stateOf("exec-peers")).isEqualTo(ExecutionState.ENQUEUED);
            assertThat(readyCount()).isEqualTo(1);
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /** §11.2: par não-RUNNING (DRAINING/PAUSED) não reivindica — mantê-lo na atribuição deixaria 1/n da fila parada; excluídos os dois, os shards deles voltam pra cá e a execução roda. */
    @Test
    void aNonRunningPeerIsExcludedFromShardAssignment() throws Exception {
        nodeStore.heartbeat("zzz-peer", EngineState.DRAINING, 1, NOW, NOW.plusSeconds(3600));
        nodeStore.heartbeat("zzz-peer2", EngineState.PAUSED, 1, NOW, NOW.plusSeconds(3600));
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        recordAndOffer("exec-even", "welcome-email", "hello", NOW.minusSeconds(1), 0);
        recordAndOffer("exec-odd", "welcome-email", "hello", NOW.minusSeconds(1), 1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CountDownLatch bothSucceeded = new CountDownLatch(2);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                bothSucceeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(bothSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(2);
    }

    /**
     * §6.3: posse perdida (reclaim externo) não tem detecção ativa — o
     * zumbi termina sozinho e o resultado tardio é descartado pelo fence
     * {@code (node_id, epoch)}: a lease agora pertence à encarnação nova, e
     * a conclusão do zumbi não deleta a posse alheia, não grava attempt e
     * não toca o advisory. Sem o fence, a conclusão zumbi mataria a
     * encarnação nova saudável.
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

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // re-claim externo simulado: outro nó VIVO (senão o reaper deste
            // engine o declararia morto e reclamaria no meio do teste) já
            // reexecuta a mesma linha — a posse é de outra encarnação
            nodeStore.heartbeat("other-node", EngineState.RUNNING, 1, NOW, NOW.plusSeconds(3600));
            rawJdbcTemplate.update(
                    "UPDATE mohs_lease SET node_id = 'other-node', epoch = 9 WHERE execution_id = 'exec-1'");
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5)); // drena o handler — a conclusão tardia roda e perde o FENCE
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT node_id FROM mohs_lease WHERE execution_id = 'exec-1'", String.class)).isEqualTo("other-node");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class)).isZero();
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
        recordAndOffer("exec-1", "welcome-email", "hello", NOW.minusSeconds(1));
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
     * /executions/{id}/cancel faz de outro processo — agora a flag mora na
     * POSSE, {@code mohs_lease.cancel_requested}) é observado pelo tick em
     * ≤ 1 poll-interval — flag pura, sem interrupt (cancel é cooperativo
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
            assertThat(leaseStore.requestCancellation(ExecutionId.of("exec-1"))).isTrue();
            assertThat(cancelledPublished.await(5, TimeUnit.SECONDS)).as("tick observes the flag and the handler exits").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.CANCELLED);
    }

    /**
     * O sucessor da expiração por execução que o split aposentou: uma lease
     * DESTE node sem encarnação em memória (trabalho perdido entre claim e
     * dispatch — payload query que falhou, executor que rejeitou) seria
     * invisível ao reaper (o nó está vivo) e ao watchdog (nunca entrou no
     * mapa). O passe de reconciliação a devolve pra fila em duas rodadas,
     * com o MESMO attempt — e ela conclui sem queimar orçamento.
     */
    @Test
    void aStrayLeaseOnAHealthyNodeIsRequeuedAndCompletes() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = 'exec-1'");
        CountDownLatch succeeded = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));
        // a posse é DESTE engine (nodeId/epoch reais), mas nenhum dispatch a conhece
        rawJdbcTemplate.update("""
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested)
                VALUES ('exec-1', 'welcome-email', ?, 1, 1, 20, ?, FALSE)
                """, engine.nodeId(), JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        // rodou UMA vez, attempt 1 — a reconciliação devolveu, não puniu
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class))
                .containsExactly(1);
    }

    /**
     * O grace do S5.5: lease sem encarnação mas RECÉM-claimada não é
     * candidata do reconcile — a alta vazão ela é quase sempre uma
     * conclusão em trânsito no batcher, e requeueá-la é o bug medido no
     * bench (requeues fantasma, deadlocks com o flush). Só depois de
     * {@code max(2s, 4×poll)} + duas rodadas ela vira órfã de verdade e
     * volta pra fila.
     */
    @Test
    void aFreshStrayLeaseWaitsTheClaimedAtGraceBeforeRequeue() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = 'exec-1'");
        CountDownLatch succeeded = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(3));
        Engine engine = newEngine(counting, List.of(listener));
        // posse deste engine SEM encarnação, claimed_at = agora — dentro do grace
        rawJdbcTemplate.update("""
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested)
                VALUES ('exec-1', 'welcome-email', ?, 1, 1, 20, ?, FALSE)
                """, engine.nodeId(), JdbcTimestamps.toUtcLocalDateTime(NOW));

        engine.start();
        try {
            // vários ticks com o relógio parado: dentro do grace, NADA é requeueado
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(readyCount()).isZero();
            assertThat(leaseCount()).isEqualTo(1);

            // além do grace (2s no poll de teste): duas rodadas depois, requeue → claim → conclui
            clock.advance(Duration.ofSeconds(3));
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("stray requeued after the grace and completed").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class))
                .containsExactly(1);
    }

    /** Uma lease possuída por um nó AUSENTE de mohs_nodes (morto por definição, ADR-0051) — a matéria-prima dos testes de reaper. */
    private void seedOrphanedLease(String id, String jobKey, boolean cancelRequested) {
        recordAndOffer(id, jobKey, "hello", NOW.minusSeconds(60));
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = ?", id);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested)
                VALUES (?, ?, 'dead-node', 1, 1, 20, ?, ?)
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)), cancelRequested);
    }

    /** Pendência 8 fechada: desfecho do reaper publica os mesmos eventos do dispatch — o alerta de morte de nó (Javadoc de Failed) passa a disparar de verdade. */
    @Test
    void reclaimOfADeadNodesExecutionPublishesRetryEvents() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1)));
        seedOrphanedLease("exec-1", "welcome-email", false);
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
        seedOrphanedLease("exec-1", "welcome-email", true);
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
     * fence, como todo zumbi.
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
            // reclaim externo: a posse caiu, mas a entrada fica no mapa até a conclusão
            rawJdbcTemplate.update("DELETE FROM mohs_lease WHERE execution_id = 'exec-1'");
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
     * rodando e seu resultado tardio é descartado pelo fence.
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
        // o zumbi terminou no stop e o resultado tardio perdeu o fence
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
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-1', 'welcome-email', 'PENDING', ?, ?, 'test', '{}', 'com.example.DoesNotExist')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)), JdbcTimestamps.toUtcLocalDateTime(NOW));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of("exec-1"), JobKey.of("welcome-email"), 0, 20, 1, NOW.minusSeconds(1))));
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
        Execution found = historyStore.find(ExecutionId.of("exec-1"), NOW).orElseThrow();
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
     * ADR-0047 sobre a porta nova: falha TRANSIENTE da consulta de payloads
     * em lote é infra, nunca veredito sobre as execuções — o lote já
     * reivindicado fica com a posse de pé até um reaper o devolver se este
     * node morrer; o soluço nunca vira falha TERMINAL imediata (o achado
     * do S8).
     */
    @Test
    void transientPayloadQueryErrorLeavesTheBatchLeasedForTheReaper() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        HistoryStore flakyHistoryStore = mock(HistoryStore.class, delegatesTo(historyStore));
        doThrow(new RuntimeException("simulated transient database error"))
                .when(flakyHistoryStore).findPayloads(any());
        CountDownLatch leftForReaper = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> warnWatcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("could not load the payloads")) {
                    leftForReaper.countDown();
                }
            }
        };
        warnWatcher.start();
        engineLogger.addAppender(warnWatcher);
        Engine engine = assembleEngine(workQueue, leaseStore, flakyHistoryStore, nodeStore, List.of(),
                defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

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
     * verdade (fora do snapshot de definições do tick) — falha terminal com
     * diagnóstico próprio, não o "runner could not be resolved" (que
     * apontaria o operador pro problema errado).
     */
    @Test
    void removedDefinitionFailsTheExecutionWithItsOwnError() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_job_definitions WHERE job_key = 'welcome-email'");
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
        assertThat(failedEvent.get().error()).hasMessageContaining("was removed after this execution was claimed");
    }

    /**
     * A gravação da falha terminal (failBeforeDispatch) roda no for de
     * tick(): se ela própria lançar (banco, executor de eventos saturado), o
     * resto do lote ainda precisa ser despachado — sem a guarda, a exceção
     * abortava o for e exec-2, já possuída no banco, ficava órfã até um
     * reaper.
     */
    @Test
    void tickContinuesWhenRecordingATerminalFailureThrows() throws Exception {
        // exec-1: job sem definição registrada — o caminho de failBeforeDispatch
        recordAndOffer("exec-1", "ghost-job", "hello", NOW.minusSeconds(2));
        seedEnqueuedExecution("exec-2", "welcome-email", "hello", NOW.minusSeconds(1));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> { });
        LeaseStore failingForExec1 = mock(LeaseStore.class, delegatesTo(leaseStore));
        doAnswer(invocation -> {
            List<LeaseStore.CompletionResult> results = invocation.getArgument(0);
            if (results.stream().anyMatch(result -> result.executionId().equals(ExecutionId.of("exec-1")))) {
                throw new RuntimeException("simulated database error completing exec-1");
            }
            return leaseStore.complete(results, invocation.getArgument(1));
        }).when(failingForExec1).complete(any(), any());
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = assembleEngine(workQueue, failingForExec1, historyStore, nodeStore, List.of(listener),
                defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

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
