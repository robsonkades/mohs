package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
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

import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.MohsRunner;
import io.mohs.jdbc.JdbcClaimer;
import io.mohs.jdbc.JdbcExecutionStore;
import io.mohs.jdbc.JdbcJobStore;
import io.mohs.jdbc.JdbcNodeStore;
import io.mohs.jdbc.JdbcReaper;
import io.mohs.jdbc.JdbcTimestamps;
import io.mohs.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

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
        JdbcClaimer claimer = new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()));
        JdbcReaper reaper = new JdbcReaper(dataSource, clock, executionStore, jobStore);
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        Dispatcher dispatcher = new Dispatcher(executionStore, jobStore, handlerRegistry, clock, List.of(), listeners, eventExecutor);
        ThreadPoolTaskScheduler tickScheduler = MohsExecutors.scheduler("mohs-engine-tick-test", 1);
        return new Engine(claimer, dispatcher, executionStore, jobStore, nodeStoreOverride, reaper, clock, POLL_INTERVAL, BATCH_SIZE,
                tickScheduler, runnerRegistry);
    }

    private static RunnerRegistry defaultRunnerRegistry() {
        return new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build()));
    }

    /** Claim/reclaim usam as portas reais (o claim precisa funcionar); os overrides simulam falha só no caminho do dispatch. */
    private Engine newEngineWith(JobStore jobStoreOverride, ExecutionStore executionStoreOverride, List<ExecutionListener> listeners) {
        JdbcClaimer claimer = new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()));
        JdbcReaper reaper = new JdbcReaper(dataSource, clock, executionStore, jobStore);
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        Dispatcher dispatcher = new Dispatcher(executionStoreOverride, jobStoreOverride, handlerRegistry, clock, List.of(), listeners, eventExecutor);
        ThreadPoolTaskScheduler tickScheduler = MohsExecutors.scheduler("mohs-engine-tick-test", 1);
        return new Engine(claimer, dispatcher, executionStoreOverride, jobStoreOverride, nodeStore, reaper, clock, POLL_INTERVAL, BATCH_SIZE,
                tickScheduler, defaultRunnerRegistry());
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

            counting.resetLatch(new CountDownLatch(3));
            seedEnqueuedExecution("exec-1", "welcome-email", "hello");
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
                """, JdbcTimestamps.toUtcTimestamp(NOW.minusSeconds(1)), JdbcTimestamps.toUtcTimestamp(NOW));
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
        assertThat(failedEvent.get().attemptsExhausted()).isTrue();
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

        JdbcClaimer claimer = new JdbcClaimer(dataSource, new H2JdbcDialect(), clock, executionStore, jobStore, LEASE_TTL, new ExecutionWindowRegistry(List.of()));
        JdbcReaper reaper = new JdbcReaper(dataSource, clock, executionStore, jobStore);
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        Dispatcher dispatcher = new Dispatcher(executionStore, jobStore, handlerRegistry, clock, List.of(), List.of(), eventExecutor);
        ThreadPoolTaskScheduler tickScheduler = MohsExecutors.scheduler("mohs-engine-tick-test", 1);
        // concorrência 1 de propósito — exec-1 esgota o único slot, forçando exec-2/exec-3 a rejeitar.
        RunnerRegistry oneSlotRunnerRegistry = new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(1).build()));
        Engine engine = new Engine(claimer, dispatcher, executionStore, jobStore, nodeStore, reaper, clock, POLL_INTERVAL, BATCH_SIZE,
                tickScheduler, oneSlotRunnerRegistry);

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
        public boolean complete(CompletionRequest request, JobStore jobStore) {
            if (request.id().equals(failingId)) {
                throw new RuntimeException("simulated database error completing " + request.id());
            }
            return delegate.complete(request, jobStore);
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
        public void markFired(ExecutionId id, Instant firedAt) {
            delegate.markFired(id, firedAt);
        }

        @Override
        public Set<ExecutionId> completeAll(List<CompletionRequest> requests, JobStore jobStore) {
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
        public void heartbeat(String nodeId, EngineState state, Instant at) {
            delegate.heartbeat(nodeId, state, at);
            latch.get().countDown();
        }

        @Override
        public List<StoredNode> findAll() {
            return delegate.findAll();
        }
    }
}
