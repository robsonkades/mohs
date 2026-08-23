package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.JobContext;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;

/**
 * Invoca o handler de uma execução reivindicada — a metade "dispatch" do
 * fluxo de job, agora sobre as portas da Phase 5: a conclusão é um
 * {@link LeaseStore.CompletionResult} cercado pela posse
 * {@code (node_id, epoch)} do {@link Grant} (§6.3 — o fencing token que
 * sucedeu o {@code (node_id, fired_at)} da ADR-0051), entregue direto ou
 * via {@link CompletionBatcher} (group commit, ADR-0047/§7.6).
 *
 * <p>Falha de attempt com orçamento ({@code JobDefinition.retries})
 * renasce na fila com backoff ({@link RetrySchedule}, ADR-0033) — a
 * entrada de retry viaja DENTRO do resultado e aterrissa na mesma
 * transação da conclusão (ver {@code CompletionResult.retry}); orçamento
 * esgotado é {@code FAILED} terminal. {@code retryPolicy} (bean
 * customizado) segue sem efeito — SPI futura.
 *
 * <p>Síncrono e sem pool próprio: quem decide quantos {@code dispatch}
 * ficam em voo, e em que tipo de thread, é o poll loop do {@link Engine}.
 */
public final class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private static final String NO_HANDLER_ERROR = "no handler registered for job ";

    private final LeaseStore leaseStore;
    private final JobStore jobStore;
    private final HandlerRegistry handlerRegistry;
    private final Clock clock;
    private final List<ExecutionInterceptor> interceptors;
    private final ExecutionEventPublisher events;
    private final EngineMetrics metrics;
    private final @Nullable CompletionBatcher completionBatcher;

    /**
     * A posse que o claim entregou (§6.2): {@code (nodeId, epoch)} é o
     * fence de toda conclusão; {@code attemptNumber} veio da entrada de
     * fila (§5.3 — nada conta attempts no hot path); {@code claimedAt}
     * ancora o attempt sintético do watchdog; {@code executionCreatedAt}
     * poda a partição do UPDATE terminal ({@code null} = sem poda, o
     * caminho degradado de linha ilegível).
     */
    public record Grant(String nodeId, long epoch, int attemptNumber, Instant claimedAt,
            @Nullable Instant executionCreatedAt) {
        public Grant {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(claimedAt, "claimedAt");
            if (attemptNumber < 1) {
                throw new IllegalArgumentException("attemptNumber must be >= 1");
            }
        }
    }

    /** Conclusão síncrona por resultado (pré-ADR-0047) — a forma dos testes e de {@code completion-flush-on-every-result}. */
    public Dispatcher(LeaseStore leaseStore, JobStore jobStore, HandlerRegistry handlerRegistry, Clock clock,
            List<ExecutionInterceptor> interceptors, List<ExecutionListener> listeners, AsyncTaskExecutor eventExecutor,
            EngineMetrics metrics) {
        this(leaseStore, jobStore, handlerRegistry, clock, interceptors, listeners, eventExecutor, metrics, null);
    }

    public Dispatcher(LeaseStore leaseStore, JobStore jobStore, HandlerRegistry handlerRegistry, Clock clock,
            List<ExecutionInterceptor> interceptors, List<ExecutionListener> listeners, AsyncTaskExecutor eventExecutor,
            EngineMetrics metrics, @Nullable CompletionBatcher completionBatcher) {
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.interceptors = List.copyOf(Objects.requireNonNull(interceptors, "interceptors"));
        this.events = new ExecutionEventPublisher(Objects.requireNonNull(listeners, "listeners"), Objects.requireNonNull(eventExecutor, "eventExecutor"));
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.completionBatcher = completionBatcher;
    }

    /** Forma sem fonte externa de cancelamento — sinal próprio, nada o levanta. Conveniência de teste e de chamador avulso. */
    public void dispatch(Execution execution, JobDefinition definition, Object payload, Grant grant) {
        dispatch(execution, definition, payload, new CancellationSignal(), grant);
    }

    public void dispatch(Execution execution, JobDefinition definition, Object payload, CancellationSignal signal,
            Grant grant) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(signal, "signal");
        Objects.requireNonNull(grant, "grant");

        Instant firedAt = clock.instant();
        metrics.dispatchLatency(execution.jobKey(), Duration.between(execution.scheduledAt(), firedAt));

        int attemptNumber = grant.attemptNumber();
        JobContext ctx = new DefaultJobContext(execution.jobKey(), execution.id(), attemptNumber, execution.scheduledAt(), firedAt, signal);
        events.publish(new Started(execution.id(), execution.jobKey(), attemptNumber, firedAt));

        Optional<JobHandler> handler = handlerRegistry.find(execution.jobKey());
        if (handler.isEmpty()) {
            // passa pelo orçamento de retry de propósito: em rolling update, outro
            // nó (com a versão que ainda registra o handler) pode reivindicar o retry
            fail(execution, definition, firedAt, new IllegalStateException(NO_HANDLER_ERROR + execution.jobKey().value()), grant);
            return;
        }

        CancellationSignal.Reason preStart = signal.reason();
        if (preStart != null) {
            failBeforeStart(execution, definition, firedAt, preStart, grant);
            return;
        }

        // succeed() fora do try de propósito (review ADR-0034): o catch mapeia
        // por razão do sinal, e a precondição dele é "o HANDLER saiu anormal" —
        // falha da escrita de sucesso não é falha do handler; propaga (o Engine
        // loga, a lease fica de pé e o reaper decide — indistinguível de
        // crash pré-conclusão, que é o caminho honesto)
        try {
            invokeWithinInterruptWindow(handler.orElseThrow(), payload, ctx, signal);
        } catch (Exception e) {
            failSignalAware(execution, definition, firedAt, e, signal, grant);
            return;
        }
        succeed(execution, definition, firedAt, grant);
    }

    /**
     * Checagem pré-start (review ADR-0034): sinal levantado com a task ainda
     * na fila do runner é honrado ANTES de invocar o handler — não iniciar
     * trabalho novo é o primeiro passo de shutdown gracioso (Burns).
     * {@code MANUAL} → {@code CANCELLED} sem rodar; {@code SHUTDOWN} →
     * falha NodeShutdown sem rodar (trabalho não feito → retry limpo em
     * outro node). {@code TIMEOUT} pré-start é inalcançável — mapeado pelo
     * mesmo caminho por exaustividade, nunca ignorado.
     */
    private void failBeforeStart(Execution execution, JobDefinition definition, Instant firedAt,
            CancellationSignal.Reason reason, Grant grant) {
        switch (reason) {
            case MANUAL -> cancelled(execution, definition, firedAt,
                    new IllegalStateException("cancel requested before the handler started"), grant);
            case SHUTDOWN -> fail(execution, definition, firedAt, new IllegalStateException(
                    "node shutdown: drain grace elapsed before attempt " + grant.attemptNumber() + " started"), grant);
            case TIMEOUT -> fail(execution, definition, firedAt, timeoutError(definition, grant.attemptNumber(),
                    new IllegalStateException("timeout signalled before the handler started — should be unreachable")), grant);
            // statement switch sobre enum não impõe exaustividade — razão nova
            // sem case cai aqui, nunca no silêncio (a promessa do Javadoc)
            default -> throw new IllegalStateException("unmapped cancellation reason: " + reason);
        }
    }

    /**
     * A janela de interrupt (ADR-0034): abre imediatamente antes da cadeia e
     * fecha em {@code finally}, ANTES de qualquer escrita de conclusão —
     * JDBC nunca roda interrompido e a thread de um runner CPU volta limpa
     * ao pool.
     */
    private void invokeWithinInterruptWindow(JobHandler handler, Object payload, JobContext ctx, CancellationSignal signal) throws Exception {
        signal.registerHandlerThread();
        try {
            runInterceptorChain(handler, payload, ctx);
        } finally {
            signal.unregisterHandlerThreadAndClearInterrupt();
        }
    }

    /**
     * ADR-0034: sinal disparado reclassifica SÓ a saída anormal — retorno
     * normal é {@code SUCCEEDED} mesmo com sinal (o trabalho terminou;
     * registrar outra coisa mentiria e agendaria uma duplicata).
     * {@code TIMEOUT}/{@code SHUTDOWN} seguem o orçamento de retry como
     * qualquer falha; {@code MANUAL} é {@code CANCELLED} terminal — cancel
     * vence orçamento.
     */
    private void failSignalAware(Execution execution, JobDefinition definition, Instant firedAt,
            Exception error, CancellationSignal signal, Grant grant) {
        CancellationSignal.Reason reason = signal.reason();
        if (reason == null) {
            fail(execution, definition, firedAt, error, grant);
            return;
        }
        switch (reason) {
            case TIMEOUT -> fail(execution, definition, firedAt, timeoutError(definition, grant.attemptNumber(), error), grant);
            case SHUTDOWN -> fail(execution, definition, firedAt, new IllegalStateException(
                    "node shutdown: drain grace elapsed before attempt " + grant.attemptNumber() + " finished", error), grant);
            case MANUAL -> cancelled(execution, definition, firedAt, error, grant);
        }
    }

    private static TimeoutException timeoutError(JobDefinition definition, int attemptNumber, Exception cause) {
        TimeoutException error = new TimeoutException("attempt " + attemptNumber + " exceeded job timeout " + definition.timeout());
        error.initCause(cause);
        return error;
    }

    /**
     * INFO, não WARN, e sem stack trace: cancelamento honrado é o sistema
     * fazendo o que o operador pediu, não uma falha. O attempt
     * {@code CANCELLED} carrega {@code error} nulo (invariante de
     * {@code Attempt}) — a exceção com que o handler saiu vai no log.
     */
    private void cancelled(Execution execution, JobDefinition definition, Instant firedAt, Exception error, Grant grant) {
        log.info("execution {} of job '{}' cancelled on attempt {} — cooperative cancellation honoured (handler exited with: {})",
                execution.id().value(), execution.jobKey().value(), grant.attemptNumber(), error.toString());
        Instant finishedAt = clock.instant();
        completeOrDiscard(new LeaseStore.CompletionResult(execution.id(), execution.jobKey(), grant.nodeId(), grant.epoch(),
                grant.attemptNumber(), firedAt, finishedAt, ExecutionState.CANCELLED, null, null,
                ExecutionState.CANCELLED, grant.executionCreatedAt(), null, execution.batchId(),
                rearmNextFireAt(execution, definition, finishedAt)),
                () -> events.publish(new Cancelled(execution.id(), execution.jobKey(), grant.attemptNumber())));
    }

    /**
     * O pipeline de eventos, para o {@link Engine} publicar os desfechos do
     * reaper pelo mesmo caminho do dispatch — package-private: o publisher
     * continua interno ao pacote, só muda quem pode falar por ele.
     */
    ExecutionEventPublisher events() {
        return events;
    }

    /**
     * O guard por estado do reconcile de stray leases (review S5.5): a
     * conclusão deste id está em trânsito no {@link CompletionBatcher}?
     * Sem batcher (conclusão síncrona) não existe trânsito — {@code false}.
     */
    boolean completionInTransit(ExecutionId executionId) {
        return completionBatcher != null && completionBatcher.completionInTransit(executionId);
    }

    /**
     * Chain of Responsibility clássica (GoF): cada interceptor embrulha o
     * próximo, o mais interno chama o handler de verdade. Roda na própria
     * thread do dispatch; exceção de interceptor É falha de attempt.
     */
    private void runInterceptorChain(JobHandler handler, Object payload, JobContext ctx) throws Exception {
        ExecutionInterceptor.Chain chain = () -> handler.invoke(payload, ctx);
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            ExecutionInterceptor interceptor = interceptors.get(i);
            ExecutionInterceptor.Chain next = chain;
            chain = () -> interceptor.intercept(ctx, next);
        }
        chain.proceed();
    }

    private void succeed(Execution execution, JobDefinition definition, Instant firedAt, Grant grant) {
        Instant finishedAt = clock.instant();
        completeOrDiscard(new LeaseStore.CompletionResult(execution.id(), execution.jobKey(), grant.nodeId(), grant.epoch(),
                grant.attemptNumber(), firedAt, finishedAt, ExecutionState.SUCCEEDED, null, null,
                ExecutionState.SUCCEEDED, grant.executionCreatedAt(), null, execution.batchId(),
                rearmNextFireAt(execution, definition, finishedAt)),
                () -> events.publish(new Succeeded(execution.id(), execution.jobKey(), grant.attemptNumber())));
    }

    /**
     * ADR-0035: conclusão terminal de ocorrência do scheduler em agenda
     * fixed-delay rearma a corrente — {@code fim + interval}, "ancorado no
     * fim da execução anterior" ao pé da letra. {@code null} nas demais
     * agendas, em execução manual e sem definição em mãos
     * ({@link #failBeforeDispatch} com definição removida — job retired
     * não dispara, a ressurreição via upsert cura).
     */
    private static @Nullable Instant rearmNextFireAt(Execution execution, @Nullable JobDefinition definition, Instant finishedAt) {
        return Execution.SCHEDULER_ACTOR.equals(execution.actor())
                && definition != null && definition.schedule() instanceof IntervalSpec interval && interval.afterFinish()
                ? finishedAt.plus(interval.interval())
                : null;
    }

    /** Orçamento restante ({@link RetrySchedule}) decide: renasce na fila com backoff (na MESMA transação da conclusão) ou falha terminal ({@link #failTerminally}). */
    private void fail(Execution execution, JobDefinition definition, Instant firedAt, Exception error, Grant grant) {
        fail(execution, definition, firedAt, error, grant, true);
    }

    private void fail(Execution execution, JobDefinition definition, Instant firedAt, Exception error, Grant grant,
            boolean throughBatcher) {
        int attemptNumber = grant.attemptNumber();
        Optional<Instant> nextRetry = RetrySchedule.nextRetryAt(attemptNumber, definition.retries(), clock.instant());
        if (nextRetry.isEmpty()) {
            failTerminally(execution, definition, firedAt, error, true, grant, throughBatcher);
            return;
        }
        Instant retryAt = nextRetry.orElseThrow();
        log.warn("execution {} of job '{}' failed on attempt {} — retry {} scheduled for {}", execution.id().value(),
                execution.jobKey().value(), attemptNumber, attemptNumber + 1, retryAt, error);
        completeOrDiscard(new LeaseStore.CompletionResult(execution.id(), execution.jobKey(), grant.nodeId(), grant.epoch(),
                attemptNumber, firedAt, clock.instant(), ExecutionState.FAILED, error.getClass().getName(), errorMessage(error),
                null, grant.executionCreatedAt(),
                new WorkQueue.ReadyEntry(execution.id(), execution.jobKey(), Shards.of(execution.id()),
                        execution.priority().value(), attemptNumber + 1, retryAt),
                execution.batchId(), null),
                () -> {
                    events.publish(new AttemptFailed(execution.id(), execution.jobKey(), attemptNumber, error));
                    events.publish(new RetryScheduled(execution.id(), execution.jobKey(), attemptNumber + 1, retryAt));
                },
                throughBatcher);
    }

    /**
     * O WARN com a exceção completa é o único lugar onde o stack trace da
     * falha aparece por padrão — {@code Attempt.error} guarda só a mensagem
     * e o evento {@code Failed} depende de um {@code ExecutionListener}
     * registrado; sem este log, a causa de um job quebrado às 3h da manhã
     * não estaria em lugar nenhum.
     *
     * <p>{@code attemptsExhausted} responde a pergunta que o Javadoc de
     * {@link Failed} faz: {@code true} só quando {@link RetrySchedule}
     * disse "sem saldo"; falha terminal por natureza (pré-dispatch) publica
     * {@code false} — orçamento intacto não é orçamento esgotado.
     */
    private void failTerminally(Execution execution, @Nullable JobDefinition definition, Instant firedAt,
            Exception error, boolean attemptsExhausted, Grant grant) {
        failTerminally(execution, definition, firedAt, error, attemptsExhausted, grant, true);
    }

    private void failTerminally(Execution execution, @Nullable JobDefinition definition, Instant firedAt,
            Exception error, boolean attemptsExhausted, Grant grant, boolean throughBatcher) {
        log.warn("execution {} of job '{}' failed on attempt {}", execution.id().value(),
                execution.jobKey().value(), grant.attemptNumber(), error);
        Instant finishedAt = clock.instant();
        completeOrDiscard(new LeaseStore.CompletionResult(execution.id(), execution.jobKey(), grant.nodeId(), grant.epoch(),
                grant.attemptNumber(), firedAt, finishedAt, ExecutionState.FAILED, error.getClass().getName(), errorMessage(error),
                ExecutionState.FAILED, grant.executionCreatedAt(), null, execution.batchId(),
                rearmNextFireAt(execution, definition, finishedAt)),
                () -> events.publish(new Failed(execution.id(), execution.jobKey(), grant.attemptNumber(), error, attemptsExhausted)),
                throughBatcher);
    }

    /**
     * Publica os eventos só se o fence de conclusão passou — uma encarnação
     * perdida (reaper/requeue passou antes) descarta o resultado com WARN,
     * nunca publica evento de uma transição que não ocorreu. Com o
     * {@link CompletionBatcher} (ADR-0047) a escrita vira group commit e o
     * desfecho (métrica/eventos) roda na thread do flusher, DEPOIS do
     * commit do lote — mesma garantia, janela de durabilidade ≤ o
     * intervalo de flush.
     */
    private void completeOrDiscard(LeaseStore.CompletionResult result, Runnable publishEvents) {
        completeOrDiscard(result, publishEvents, true);
    }

    /**
     * {@code throughBatcher = false} força a conclusão síncrona mesmo com o
     * batcher ligado — os caminhos frios ({@link #failBeforeDispatch},
     * {@link #abandonOwnership}) dependem do contrato "lançou = não
     * aconteceu" dos chamadores (o retry do watchdog no tick seguinte, a
     * guarda do Engine): roteá-los pela fila trocaria a exceção síncrona
     * por um log no flusher que ninguém re-tenta (JCIP §6.3.2 — assíncrono
     * muda o contrato de erro do chamador junto).
     */
    private void completeOrDiscard(LeaseStore.CompletionResult result, Runnable publishEvents, boolean throughBatcher) {
        if (completionBatcher == null || !throughBatcher) {
            handleOutcome(result, leaseStore.complete(List.of(result), jobStore)
                    .getOrDefault(result.executionId(), LeaseStore.Completion.FENCED_OUT), publishEvents);
            return;
        }
        completionBatcher.submit(result, completion -> handleOutcome(result, completion, publishEvents));
    }

    private void handleOutcome(LeaseStore.CompletionResult result, LeaseStore.Completion completion, Runnable publishEvents) {
        if (completion.owned()) {
            metrics.attemptFinished(result.jobKey(),
                    new Attempt(result.attemptNumber(), result.startedAt(), result.finishedAt(), result.outcome(), result.error()),
                    result.terminalState() != null ? result.terminalState() : ExecutionState.RETRY_WAITING);
            publishEvents.run();
            publishBatchCompletedIfClosed(completion);
        } else {
            log.warn("attempt {} of execution {} finished {} but the incarnation was no longer ours (reaper/requeue passed first) — result discarded",
                    result.attemptNumber(), result.executionId().value(), result.outcome());
        }
    }

    /**
     * ADR-0043: quem fechou o lote foi eleito pelo banco, dentro da
     * transação de conclusão; a publicação acontece DEPOIS dela, junto dos
     * demais eventos da conclusão, porque evento não volta atrás se a
     * transação abortar.
     */
    private void publishBatchCompletedIfClosed(LeaseStore.Completion completion) {
        BatchCounters closed = completion.closedBatch();
        if (closed != null) {
            events.publish(new BatchCompleted(closed.batchId(), closed.total(), closed.succeeded(), closed.failed()));
        }
    }

    private static String errorMessage(Exception error) {
        return Objects.requireNonNullElse(error.getMessage(), error.toString());
    }

    /**
     * Falha uma execução terminalmente sem ter passado por {@link #dispatch}
     * — pra quando o chamador ({@link Engine}) já sabe que a execução
     * falhou antes do handler sequer poder rodar (payload ilegível,
     * definição removida entre claim e dispatch). Sempre terminal, sem
     * consultar orçamento: payload ilegível não sara repetindo a leitura, e
     * definição removida não tem {@code retries} confiável.
     * {@code Failed.attemptsExhausted} sai {@code false} — terminal por
     * natureza, não por orçamento.
     */
    void failBeforeDispatch(Execution execution, @Nullable JobDefinition definition, Exception cause, Grant grant) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(cause, "cause");
        // síncrono de propósito: a guarda do Engine trata a exceção desta
        // gravação ("loga e deixa a posse pro reconcile") — pela fila do
        // batcher, a falha do flush nunca chegaria a ela
        failTerminally(execution, definition, clock.instant(), cause, false, grant, false);
    }

    /**
     * O Watchdog Bound (ADR-0051 sobre a posse nova): o node LIBERA a posse
     * de uma encarnação cujo runtime estourou o bound — attempt sintético
     * FAILED pelo orçamento de retry, cercado pela posse liberada. O zumbi
     * local continua rodando até terminar sozinho; a conclusão dele carrega
     * esta MESMA posse, mas a lease já caiu (e um re-claim grava dono/epoch
     * próprios), então o fence dele perde por construção.
     */
    void abandonOwnership(Execution execution, JobDefinition definition, Grant grant, String reason) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(grant, "grant");
        // síncrono de propósito: o watchdog só marca a posse como liberada se
        // esta chamada retornar — "falha na liberação não marca, o próximo
        // tick re-tenta" exige exceção síncrona, não um enfileirar que
        // sempre retorna (JCIP §6.3.2)
        fail(execution, definition, grant.claimedAt(), new IllegalStateException(reason), grant, false);
    }

    /**
     * {@link #cancellationRequested()} lê o {@link CancellationSignal} da
     * encarnação (ADR-0034) — as fontes são o timeout do job, o estouro do
     * grace de drain e o {@code POST /executions/{id}/cancel} observado
     * pelo tick. {@link #progress} é no-op — já é o contrato documentado
     * quando nada observa.
     */
    private record DefaultJobContext(JobKey jobKey, ExecutionId executionId, int attempt, Instant scheduledAt, Instant firedAt,
            CancellationSignal signal) implements JobContext {

        @Override
        public boolean cancellationRequested() {
            return signal.cancellationRequested();
        }

        @Override
        public void progress(int done, int total) {
        }
    }
}
