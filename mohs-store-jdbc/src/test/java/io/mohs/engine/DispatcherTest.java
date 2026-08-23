package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcHistoryStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcTimestamps;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.dialect.H2JdbcDialect;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * {@link Dispatcher} contra as portas JDBC reais (H2, Phase 5): cada
 * desfecho vira um {@link LeaseStore.CompletionResult} cercado pela posse
 * do {@link Dispatcher.Grant} — a posse dos seeds nasce pelo caminho real
 * (história → fila → claim), então o fence dos testes é o de verdade.
 */
class DispatcherTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final String NODE = "node-a";
    private static final long EPOCH = 1;

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcJobStore jobStore;
    private JdbcHistoryStore historyStore;
    private JdbcWorkQueue workQueue;
    private JdbcLeaseStore leaseStore;
    private HandlerRegistry handlerRegistry;
    private RecordingListener listener;
    private AsyncTaskExecutor eventExecutor;

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
        handlerRegistry = new HandlerRegistry();
        listener = new RecordingListener();
        eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:dispatcher-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private Dispatcher newDispatcher(List<ExecutionInterceptor> interceptors) {
        return new Dispatcher(leaseStore, jobStore, handlerRegistry, clock, interceptors, List.of(listener), eventExecutor,
                new EngineMetrics(new SimpleMeterRegistry()));
    }

    /** A posse que o claim dos seeds entregou — o mesmo fence (NODE, EPOCH) da lease gravada. */
    private static Dispatcher.Grant grant(int attemptNumber) {
        return new Dispatcher.Grant(NODE, EPOCH, attemptNumber, NOW, NOW);
    }

    /**
     * ADR-0047: com o {@link CompletionBatcher}, a conclusão vira group
     * commit e o evento sai DEPOIS do flush — a garantia "publica só o que
     * ficou durável" atravessa o batcher. A mecânica do batcher em si mora
     * em {@link CompletionBatcherTest}; aqui é só o wiring do dispatcher.
     */
    @Test
    void dispatchThroughTheBatcherCompletesAndPublishesAfterTheFlush() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CompletionBatcher batcher = new CompletionBatcher(leaseStore, jobStore, 1, Duration.ofMillis(5));
        batcher.start();
        try {
            new Dispatcher(leaseStore, jobStore, handlerRegistry, clock, List.of(), List.of(listener), eventExecutor,
                    new EngineMetrics(new SimpleMeterRegistry()), batcher)
                    .dispatch(execution, onDemand("welcome-email"), "hello", grant(1));

            assertThat(listener.awaitEvent(Succeeded.class)).isNotNull();
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        } finally {
            batcher.close();
        }
    }

    /** História + fila + claim — a posse nasce pelo caminho real, com o fence (NODE, EPOCH). */
    private Execution seedRunningExecution(String id, String jobKey) {
        return seedRunningExecution(id, jobKey, 1, "test");
    }

    private Execution seedRunningExecution(String id, String jobKey, int attempt, String actor) {
        if (jobStore.find(JobKey.of(jobKey)).isEmpty()) {
            jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
        }
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES (?, ?, 'PENDING', ?, ?, ?, '{}', 'java.lang.Object')
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW), actor);
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, 20, attempt, NOW.minusSeconds(1))));
        workQueue.claim(0, NODE, EPOCH, 10, List.of(), NOW);
        return historyStore.find(ExecutionId.of(id), NOW).orElseThrow();
    }

    /** Estado do read model derivado (§4.3): advisory + lease + fila, exatamente o que a API pública vê. */
    private ExecutionState stateOf(String id) {
        return historyStore.find(ExecutionId.of(id), clock.instant()).orElseThrow().state();
    }

    /** retries default 0 — primeira falha esgota o orçamento, o comportamento terminal de sempre. */
    private static JobDefinition onDemand(String jobKey) {
        return JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand());
    }

    /** ADR-0033: falha com orçamento renasce na fila com backoff dentro do bound (1s pra 1ª falha) — RETRY_WAITING derivado — e publica AttemptFailed + RetryScheduled, nunca Failed. */
    @Test
    void failureWithRemainingBudgetSchedulesARetryWithBackoff() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(2));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            throw new IllegalStateException("boom");
        });

        newDispatcher(List.of()).dispatch(execution, definition, "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RETRY_WAITING);
        Execution found = historyStore.find(ExecutionId.of("exec-1"), NOW).orElseThrow();
        assertThat(found.attempts()).hasSize(1);
        assertThat(found.attempts().get(0).outcome()).isEqualTo(ExecutionState.FAILED);
        // o renascimento aterrissou na fila: attempt 2, visível no backoff
        Instant retryVisibleAt = JdbcTimestamps.fromUtcLocalDateTime(rawJdbcTemplate.queryForObject(
                "SELECT visible_at FROM mohs_ready WHERE execution_id = 'exec-1'", LocalDateTime.class));
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_ready WHERE execution_id = 'exec-1'", Integer.class)).isEqualTo(2);
        assertThat(retryVisibleAt).isBetween(NOW, NOW.plusSeconds(1));

        RetryScheduled retryScheduled = listener.awaitEvent(RetryScheduled.class);
        assertThat(retryScheduled.nextAttempt()).isEqualTo(2);
        assertThat(retryScheduled.retryAt()).isEqualTo(retryVisibleAt);
        assertThat(listener.events().stream().filter(AttemptFailed.class::isInstance)).hasSize(1);
        assertThat(listener.events().stream().filter(Failed.class::isInstance)).isEmpty();
    }

    /** A última tentativa do orçamento falhando é FAILED terminal com Failed(exhausted=true) — o attemptNumber veio da lease (§5.3), não de contagem. */
    @Test
    void failureOnTheLastBudgetedAttemptFailsTerminally() throws Exception {
        Execution execution = seedRunningExecutionWithPriorFailedAttempt("exec-1", "welcome-email");
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            throw new IllegalStateException("boom again");
        });

        newDispatcher(List.of()).dispatch(execution, definition, "hello", grant(2));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Failed event = listener.awaitEvent(Failed.class);
        assertThat(event.attempt()).isEqualTo(2);
        assertThat(event.attemptsExhausted()).isTrue();
    }

    /** Terminal por natureza, não por orçamento: falha pré-dispatch publica Failed(exhausted=false) — o campo responde a pergunta do Javadoc de Failed, não herda o valor do caminho vizinho. */
    @Test
    void failBeforeDispatchPublishesFailedWithoutTheExhaustedFlag() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");

        newDispatcher(List.of()).failBeforeDispatch(execution, null, new IllegalStateException("payload could not be read"), grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Failed event = listener.awaitEvent(Failed.class);
        assertThat(event.attemptsExhausted()).isFalse();
    }

    /** ADR-0035: conclusão terminal de ocorrência do scheduler em job fixed-delay rearma a corrente — fim da execução + interval. */
    @Test
    void terminalCompletionOfASchedulerOccurrenceRearmsTheAfterFinishChain() {
        JobDefinition definition = JobDefinition.of("poll", Handler.class, spec -> spec.everyAfterFinish(Duration.ofMinutes(5)));
        jobStore.upsert(definition);
        Execution execution = seedRunningExecution("exec-1", "poll", 1, Execution.SCHEDULER_ACTOR);
        disarmTrigger("poll"); // NULL-until-finish: o disparo desarmou o trigger (ADR-0035)
        handlerRegistry.register(JobKey.of("poll"), (payload, ctx) -> { });

        newDispatcher(List.of()).dispatch(execution, definition, "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(nextFireAtOf("poll")).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    /** Execução manual de job fixed-delay não é a corrente — concluir não rearma. */
    @Test
    void terminalCompletionOfAManualExecutionDoesNotRearmTheChain() {
        JobDefinition definition = JobDefinition.of("poll", Handler.class, spec -> spec.everyAfterFinish(Duration.ofMinutes(5)));
        jobStore.upsert(definition);
        Execution execution = seedRunningExecution("exec-1", "poll", 1, "api:user");
        disarmTrigger("poll");
        handlerRegistry.register(JobKey.of("poll"), (payload, ctx) -> { });

        newDispatcher(List.of()).dispatch(execution, definition, "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(nextFireAtOf("poll")).isNull();
    }

    /** Falha com orçamento não rearma — a corrente continua viva pelo retry; o rearme vem da conclusão terminal do retry. */
    @Test
    void aBudgetedFailureOfASchedulerOccurrenceDoesNotRearm() {
        JobDefinition definition = JobDefinition.of("poll", Handler.class,
                spec -> spec.everyAfterFinish(Duration.ofMinutes(5)).retries(2));
        jobStore.upsert(definition);
        Execution execution = seedRunningExecution("exec-1", "poll", 1, Execution.SCHEDULER_ACTOR);
        disarmTrigger("poll");
        handlerRegistry.register(JobKey.of("poll"), (payload, ctx) -> {
            throw new IllegalStateException("boom");
        });

        newDispatcher(List.of()).dispatch(execution, definition, "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RETRY_WAITING);
        assertThat(nextFireAtOf("poll")).isNull();
    }

    private void disarmTrigger(String jobKey) {
        rawJdbcTemplate.update("UPDATE mohs_job_definitions SET next_fire_at = NULL WHERE job_key = ?", jobKey);
    }

    private @Nullable Instant nextFireAtOf(String jobKey) {
        LocalDateTime stored = rawJdbcTemplate.queryForObject(
                "SELECT next_fire_at FROM mohs_job_definitions WHERE job_key = ?", LocalDateTime.class, jobKey);
        return stored == null ? null : JdbcTimestamps.fromUtcLocalDateTime(stored);
    }

    /** Segunda tentativa: um attempt FAILED já gravado e a entrada de fila do attempt 2 — o claim entrega a lease com attempt_number 2. */
    private Execution seedRunningExecutionWithPriorFailedAttempt(String id, String jobKey) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> spec.onDemand()));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES (?, ?, 'PENDING', ?, ?, 'test', '{}', 'java.lang.Object')
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
                VALUES (?, 1, 'node-a', ?, ?, 'FAILED', 'java.lang.IllegalStateException', 'boom')
                """, id, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(10)), JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(9)));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, 20, 2, NOW.minusSeconds(1))));
        workQueue.claim(0, NODE, EPOCH, 10, List.of(), NOW);
        return historyStore.find(ExecutionId.of(id), NOW).orElseThrow();
    }

    @Test
    void dispatchInvokesTheHandlerAndRecordsSuccess() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        List<Object> received = new CopyOnWriteArrayList<>();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> received.add(payload));

        newDispatcher(List.of()).dispatch(execution, onDemand("welcome-email"), "hello", grant(1));

        assertThat(received).containsExactly("hello");
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        Execution found = historyStore.find(ExecutionId.of("exec-1"), NOW).orElseThrow();
        assertThat(found.attempts()).hasSize(1);
        assertThat(found.attempts().get(0).outcome()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(listener.awaitEvent(Succeeded.class)).isNotNull();
    }

    @Test
    void dispatchRecordsTerminalFailureWhenHandlerThrows() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            throw new IllegalStateException("boom");
        });

        newDispatcher(List.of()).dispatch(execution, onDemand("welcome-email"), "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Execution found = historyStore.find(ExecutionId.of("exec-1"), NOW).orElseThrow();
        assertThat(found.attempts().get(0).outcome()).isEqualTo(ExecutionState.FAILED);
        assertThat(found.attempts().get(0).error()).isEqualTo("boom");
        Failed event = listener.awaitEvent(Failed.class);
        assertThat(event.attemptsExhausted()).isTrue();
        assertThat(event.error()).hasMessage("boom");
    }

    @Test
    void dispatchFailsTerminallyWhenNoHandlerIsRegistered() {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");

        newDispatcher(List.of()).dispatch(execution, onDemand("welcome-email"), "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Execution found = historyStore.find(ExecutionId.of("exec-1"), NOW).orElseThrow();
        assertThat(found.attempts().get(0).error()).contains("no handler registered").contains("welcome-email");
    }

    /** ExecutionInterceptor's own Javadoc: sua exceção É falha de attempt, mesmo tratamento que exceção do handler. */
    @Test
    void interceptorExceptionCountsAsAttemptFailure() {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionInterceptor failingInterceptor = (ctx, chain) -> {
            throw new RuntimeException("interceptor blew up");
        };

        newDispatcher(List.of(failingInterceptor)).dispatch(execution, onDemand("welcome-email"), "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Execution found = historyStore.find(ExecutionId.of("exec-1"), NOW).orElseThrow();
        assertThat(found.attempts().get(0).error()).isEqualTo("interceptor blew up");
    }

    @Test
    void interceptorsRunInOrderAroundTheHandler() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        List<String> order = new ArrayList<>();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> order.add("handler"));
        ExecutionInterceptor first = (ctx, chain) -> {
            order.add("first-before");
            chain.proceed();
            order.add("first-after");
        };
        ExecutionInterceptor second = (ctx, chain) -> {
            order.add("second-before");
            chain.proceed();
            order.add("second-after");
        };

        newDispatcher(List.of(first, second)).dispatch(execution, onDemand("welcome-email"), "hello", grant(1));

        assertThat(order).containsExactly("first-before", "second-before", "handler", "second-after", "first-after");
    }

    @Test
    void listenerExceptionNeverAffectsTheOutcome() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionListener throwingListener = event -> {
            throw new RuntimeException("listener blew up");
        };
        Dispatcher dispatcher = new Dispatcher(leaseStore, jobStore, handlerRegistry, clock, List.of(),
                List.of(throwingListener, listener), eventExecutor, new EngineMetrics(new SimpleMeterRegistry()));

        dispatcher.dispatch(execution, onDemand("welcome-email"), "hello", grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(listener.awaitEvent(Succeeded.class)).isNotNull();
    }

    /** ADR-0034: sinal MANUAL + saída anormal = CANCELLED terminal com evento Cancelled — cancel vence orçamento (reagendar o que o operador mandou parar contradiz a ordem). */
    @Test
    void manualCancellationMapsAnAbnormalExitToCancelledWithoutRetry() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(5));
        CancellationSignal signal = new CancellationSignal();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
            assertThat(ctx.cancellationRequested()).isTrue();
            throw new IllegalStateException("stopping at the operator's request");
        });

        newDispatcher(List.of()).dispatch(execution, definition, "hello", signal, grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.CANCELLED);
        Cancelled cancelled = listener.awaitEvent(Cancelled.class);
        assertThat(cancelled.attempt()).isEqualTo(1);
    }

    /** ADR-0034: sinal TIMEOUT reclassifica a falha com causa de timeout e mantém o orçamento — e a thread sai limpa (interrupt pendente nunca sobrevive ao dispatch). */
    @Test
    void timeoutSignalMapsTheFailureToTimeoutCauseAndKeepsTheRetryBudget() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class,
                spec -> spec.onDemand().retries(1).timeout(Duration.ofMinutes(5)));
        CancellationSignal signal = new CancellationSignal();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, true);
            throw new InterruptedException("interrupted mid-await");
        });

        newDispatcher(List.of()).dispatch(execution, definition, "hello", signal, grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RETRY_WAITING);
        AttemptFailed failed = listener.awaitEvent(AttemptFailed.class);
        assertThat(failed.error().getMessage()).contains("exceeded job timeout PT5M");
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /** ADR-0034/API-DESIGN passo 3 do shutdown: sinal SHUTDOWN reclassifica com causa NodeShutdown e segue o retry normal — at-least-once honesto até no desligamento. */
    @Test
    void shutdownSignalMapsTheFailureToNodeShutdownCause() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        CancellationSignal signal = new CancellationSignal();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
            throw new InterruptedException("interrupted by drain escalation");
        });

        newDispatcher(List.of()).dispatch(execution, onDemand("welcome-email"), "hello", signal, grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Failed failed = listener.awaitEvent(Failed.class);
        assertThat(failed.error().getMessage()).contains("node shutdown");
    }

    /** ADR-0034: retorno normal com sinal disparado é SUCCEEDED — o trabalho terminou; registrar outra coisa mentiria e agendaria uma duplicata. */
    @Test
    void aHandlerThatFinishesDespiteTheSignalSucceeds() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        CancellationSignal signal = new CancellationSignal();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) ->
                signal.requestCancellation(CancellationSignal.Reason.MANUAL, false));

        newDispatcher(List.of()).dispatch(execution, onDemand("welcome-email"), "hello", signal, grant(1));

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        listener.awaitEvent(Succeeded.class);
    }

    /**
     * Aprovado no review do ciclo ADR-0034: falha da ESCRITA de sucesso não
     * é falha do handler — propaga (o Engine loga e a lease fica de pé pro
     * reaper, indistinguível de crash pré-conclusão) em vez de ser
     * reclassificada pelo sinal (CANCELLED de trabalho concluído) ou pelo
     * orçamento (FAILED→retry fabricando duplicata).
     */
    @Test
    void aFailingSuccessWriteIsNeverReclassifiedBySignalOrBudget() {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        CancellationSignal signal = new CancellationSignal();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) ->
                signal.requestCancellation(CancellationSignal.Reason.MANUAL, false));
        LeaseStore blinkingStore = mock(LeaseStore.class, delegatesTo(leaseStore));
        doThrow(new RuntimeException("simulated database error on the success write")).when(blinkingStore).complete(any(), any());
        Dispatcher dispatcher = new Dispatcher(blinkingStore, jobStore, handlerRegistry, clock, List.of(), List.of(listener),
                eventExecutor, new EngineMetrics(new SimpleMeterRegistry()));

        assertThatThrownBy(() -> dispatcher.dispatch(execution, onDemand("welcome-email"), "hello", signal, grant(1)))
                .hasMessageContaining("simulated database error");

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
    }

    /** Checagem pré-start (review ADR-0034): MANUAL já em pé quando a task sai da fila — o handler nem roda, a ordem do operador é honrada sem gastar CPU. */
    @Test
    void aManualSignalRaisedBeforeTheHandlerStartsSkipsTheHandlerAndCancels() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        CancellationSignal signal = new CancellationSignal();
        signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
        AtomicBoolean handlerRan = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerRan.set(true));

        newDispatcher(List.of()).dispatch(execution, onDemand("welcome-email"), "hello", signal, grant(1));

        assertThat(handlerRan).isFalse();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.CANCELLED);
        listener.awaitEvent(Cancelled.class);
    }

    /** Checagem pré-start (review ADR-0034): SHUTDOWN em task enfileirada — trabalho não feito falha NodeShutdown sem rodar e vira retry limpo em outro node (não iniciar trabalho novo é o primeiro passo de graceful shutdown). */
    @Test
    void aShutdownSignalRaisedBeforeTheHandlerStartsSkipsTheHandlerAndFollowsTheBudget() throws Exception {
        Execution execution = seedRunningExecution("exec-1", "welcome-email");
        JobDefinition definition = JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1));
        CancellationSignal signal = new CancellationSignal();
        signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
        AtomicBoolean handlerRan = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerRan.set(true));

        newDispatcher(List.of()).dispatch(execution, definition, "hello", signal, grant(1));

        assertThat(handlerRan).isFalse();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RETRY_WAITING);
        AttemptFailed failed = listener.awaitEvent(AttemptFailed.class);
        assertThat(failed.error().getMessage()).contains("before attempt 1 started");
    }

    private static final class RecordingListener implements ExecutionListener {
        private final List<ExecutionEvent> events = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void on(ExecutionEvent event) {
            events.add(event);
            if (!(event instanceof Started)) {
                latch.countDown();
            }
        }

        <T extends ExecutionEvent> T awaitEvent(Class<T> type) throws InterruptedException {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("terminal event within timeout").isTrue();
            return events.stream().filter(type::isInstance).map(type::cast).findFirst()
                    .orElseThrow(() -> new AssertionError("no event of type " + type + " among " + events));
        }

        List<ExecutionEvent> events() {
            return events;
        }
    }
}
