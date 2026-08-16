package io.mohs.engine;

import java.time.Clock;
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
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.event.AttemptFailed;
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

/**
 * Invoca o handler de uma {@code Execution RUNNING} já reivindicada por
 * {@link Claimer} — a metade "dispatch" de M3 que o próprio Javadoc de
 * {@link Claimer} deixa em aberto. Classe concreta, não porta: ao contrário
 * de {@link Claimer}/{@link ExecutionStore}, não tem SQL próprio nenhum —
 * só orquestra portas que já existem.
 *
 * <p>Falha de attempt com orçamento ({@code JobDefinition.retries})
 * transiciona pra {@code RETRY_SCHEDULED} com backoff ({@link RetrySchedule},
 * ADR-0033 — que destravou a restrição da ADR-0026); orçamento esgotado é
 * {@code FAILED} terminal. {@code retryPolicy} (bean customizado) segue
 * sem efeito — SPI futura.
 *
 * <p>Síncrono e sem pool próprio: não gerencia {@code MohsRunner} nenhum —
 * quem decide quantos {@code dispatch} ficam em voo, e em que tipo de
 * thread, é o poll loop (M3, ainda não construído), mesma separação que
 * {@link Claimer} já tem hoje (reivindica, não executa).
 */
public final class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private static final String NO_HANDLER_ERROR = "no handler registered for job ";

    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    private final HandlerRegistry handlerRegistry;
    private final Clock clock;
    private final List<ExecutionInterceptor> interceptors;
    private final ExecutionEventPublisher events;

    public Dispatcher(ExecutionStore executionStore, JobStore jobStore, HandlerRegistry handlerRegistry, Clock clock,
            List<ExecutionInterceptor> interceptors, List<ExecutionListener> listeners, AsyncTaskExecutor eventExecutor) {
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.interceptors = List.copyOf(Objects.requireNonNull(interceptors, "interceptors"));
        this.events = new ExecutionEventPublisher(Objects.requireNonNull(listeners, "listeners"), Objects.requireNonNull(eventExecutor, "eventExecutor"));
    }

    /** Forma sem fonte externa de cancelamento — sinal próprio, nada o levanta. Conveniência de teste e de chamador avulso. */
    public void dispatch(Execution execution, JobDefinition definition, Object payload) {
        dispatch(execution, definition, payload, new CancellationSignal());
    }

    public void dispatch(Execution execution, JobDefinition definition, Object payload, CancellationSignal signal) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(signal, "signal");

        Instant firedAt = clock.instant();
        executionStore.markFired(execution.id(), firedAt);

        int attemptNumber = execution.attempts().size() + 1;
        JobContext ctx = new DefaultJobContext(execution.jobKey(), execution.id(), attemptNumber, execution.scheduledAt(), firedAt, signal);
        events.publish(new Started(execution.id(), execution.jobKey(), attemptNumber, firedAt));

        Optional<JobHandler> handler = handlerRegistry.find(execution.jobKey());
        if (handler.isEmpty()) {
            // passa pelo orçamento de retry de propósito: em rolling update, outro
            // nó (com a versão que ainda registra o handler) pode reivindicar o retry
            fail(execution, definition, attemptNumber, firedAt, new IllegalStateException(NO_HANDLER_ERROR + execution.jobKey().value()));
            return;
        }

        CancellationSignal.Reason preStart = signal.reason();
        if (preStart != null) {
            failBeforeStart(execution, definition, attemptNumber, firedAt, preStart);
            return;
        }

        // succeed() fora do try de propósito (review ADR-0034): o catch mapeia
        // por razão do sinal, e a precondição dele é "o HANDLER saiu anormal" —
        // falha da escrita de sucesso não é falha do handler; propaga (o Engine
        // loga, a execução fica RUNNING e o reaper decide — indistinguível de
        // crash pré-conclusão, que é o caminho honesto)
        try {
            invokeWithinInterruptWindow(handler.orElseThrow(), payload, ctx, signal);
        } catch (Exception e) {
            failSignalAware(execution, definition, attemptNumber, firedAt, e, signal);
            return;
        }
        succeed(execution, definition, attemptNumber, firedAt);
    }

    /**
     * Checagem pré-start (review ADR-0034): sinal levantado com a task ainda
     * na fila do runner é honrado ANTES de invocar o handler — não iniciar
     * trabalho novo é o primeiro passo de shutdown gracioso (Burns), e rodar
     * o que o operador mandou parar desperdiçaria a chance barata de honrar
     * a ordem. {@code MANUAL} → {@code CANCELLED} sem rodar; {@code SHUTDOWN}
     * → falha NodeShutdown sem rodar (trabalho não feito → retry limpo em
     * outro node). {@code TIMEOUT} pré-start é inalcançável (o relógio só
     * corre a partir do registro) — mapeado pelo mesmo caminho por
     * exaustividade, nunca ignorado.
     */
    private void failBeforeStart(Execution execution, JobDefinition definition, int attemptNumber, Instant firedAt,
            CancellationSignal.Reason reason) {
        switch (reason) {
            case MANUAL -> cancelled(execution, definition, attemptNumber, firedAt,
                    new IllegalStateException("cancel requested before the handler started"));
            case SHUTDOWN -> fail(execution, definition, attemptNumber, firedAt, new IllegalStateException(
                    "node shutdown: drain grace elapsed before attempt " + attemptNumber + " started"));
            case TIMEOUT -> fail(execution, definition, attemptNumber, firedAt, timeoutError(definition, attemptNumber,
                    new IllegalStateException("timeout signalled before the handler started — should be unreachable")));
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
    private void failSignalAware(Execution execution, JobDefinition definition, int attemptNumber, Instant firedAt,
            Exception error, CancellationSignal signal) {
        CancellationSignal.Reason reason = signal.reason();
        if (reason == null) {
            fail(execution, definition, attemptNumber, firedAt, error);
            return;
        }
        switch (reason) {
            case TIMEOUT -> fail(execution, definition, attemptNumber, firedAt, timeoutError(definition, attemptNumber, error));
            case SHUTDOWN -> fail(execution, definition, attemptNumber, firedAt, new IllegalStateException(
                    "node shutdown: drain grace elapsed before attempt " + attemptNumber + " finished", error));
            case MANUAL -> cancelled(execution, definition, attemptNumber, firedAt, error);
        }
    }

    private static TimeoutException timeoutError(JobDefinition definition, int attemptNumber, Exception cause) {
        TimeoutException error = new TimeoutException("attempt " + attemptNumber + " exceeded job timeout " + definition.timeout());
        error.initCause(cause);
        return error;
    }

    /**
     * INFO, não WARN, e sem stack trace: cancelamento honrado é o sistema
     * fazendo o que o operador pediu, não uma falha. O {@code Attempt}
     * {@code CANCELLED} carrega {@code error} nulo (invariante de
     * {@code Attempt}) — a exceção com que o handler saiu vai no log.
     */
    private void cancelled(Execution execution, JobDefinition definition, int attemptNumber, Instant firedAt, Exception error) {
        log.info("execution {} of job '{}' cancelled on attempt {} — cooperative cancellation honoured (handler exited with: {})",
                execution.id().value(), execution.jobKey().value(), attemptNumber, error.toString());
        Instant finishedAt = clock.instant();
        Attempt attempt = new Attempt(attemptNumber, firedAt, finishedAt, ExecutionState.CANCELLED, null);
        completeOrDiscard(
                new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(), attempt, ExecutionState.CANCELLED,
                        null, null, rearmNextFireAt(execution, definition, finishedAt)),
                () -> events.publish(new Cancelled(execution.id(), execution.jobKey(), attemptNumber)));
    }

    /**
     * O pipeline de eventos, para o {@link Engine} publicar os desfechos do
     * reclaim pelo mesmo caminho do dispatch — package-private: o publisher
     * continua interno ao pacote, só muda quem pode falar por ele.
     */
    ExecutionEventPublisher events() {
        return events;
    }

    /**
     * Chain of Responsibility clássica (GoF): cada interceptor embrulha o
     * próximo, o mais interno chama o handler de verdade. Roda na própria
     * thread do dispatch — Javadoc de {@link ExecutionInterceptor} já exige
     * isso (lugar de MDC/tracing/{@code ScopedValue}); exceção de
     * interceptor É falha de attempt, mesmo tratamento que exceção do
     * handler.
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

    private void succeed(Execution execution, JobDefinition definition, int attemptNumber, Instant firedAt) {
        Instant finishedAt = clock.instant();
        Attempt attempt = new Attempt(attemptNumber, firedAt, finishedAt, ExecutionState.SUCCEEDED, null);
        completeOrDiscard(
                new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(), attempt, ExecutionState.SUCCEEDED,
                        null, null, rearmNextFireAt(execution, definition, finishedAt)),
                () -> events.publish(new Succeeded(execution.id(), execution.jobKey(), attemptNumber)));
    }

    /**
     * ADR-0035: conclusão terminal de ocorrência do scheduler em agenda
     * fixed-delay rearma a corrente — {@code fim + interval}, "ancorado no
     * fim da execução anterior" ao pé da letra. {@code null} nas demais
     * agendas, em execução manual (não é a corrente — o guard
     * {@code IS NULL} do rearme já protege, mas rearmar por ela anteciparia
     * a série com a corrente ainda em voo) e sem definição em mãos
     * ({@link #failBeforeDispatch} com definição removida — job retired
     * não dispara, a ressurreição via upsert cura).
     */
    private static @Nullable Instant rearmNextFireAt(Execution execution, @Nullable JobDefinition definition, Instant finishedAt) {
        return Execution.SCHEDULER_ACTOR.equals(execution.actor())
                && definition != null && definition.schedule() instanceof IntervalSpec interval && interval.afterFinish()
                ? finishedAt.plus(interval.interval())
                : null;
    }

    /** Orçamento restante ({@link RetrySchedule}) decide: reagenda com backoff ou falha terminal ({@link #failTerminally}). */
    private void fail(Execution execution, JobDefinition definition, int attemptNumber, Instant firedAt, Exception error) {
        Optional<Instant> nextRetry = RetrySchedule.nextRetryAt(attemptNumber, definition.retries(), clock.instant());
        if (nextRetry.isEmpty()) {
            failTerminally(execution, definition, attemptNumber, firedAt, error, true);
            return;
        }
        Instant retryAt = nextRetry.orElseThrow();
        log.warn("execution {} of job '{}' failed on attempt {} — retry {} scheduled for {}", execution.id().value(),
                execution.jobKey().value(), attemptNumber, attemptNumber + 1, retryAt, error);
        Attempt attempt = failedAttempt(attemptNumber, firedAt, error);
        completeOrDiscard(
                new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(), attempt, ExecutionState.RETRY_SCHEDULED, retryAt),
                () -> {
                    events.publish(new AttemptFailed(execution.id(), execution.jobKey(), attemptNumber, error));
                    events.publish(new RetryScheduled(execution.id(), execution.jobKey(), attemptNumber + 1, retryAt));
                });
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
    private void failTerminally(Execution execution, @Nullable JobDefinition definition, int attemptNumber, Instant firedAt,
            Exception error, boolean attemptsExhausted) {
        log.warn("execution {} of job '{}' failed on attempt {}", execution.id().value(),
                execution.jobKey().value(), attemptNumber, error);
        Attempt attempt = failedAttempt(attemptNumber, firedAt, error);
        // finishedAt do attempt é o "fim" que ancora o rearme fixed-delay (ADR-0035)
        completeOrDiscard(
                new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(), attempt, ExecutionState.FAILED,
                        null, null, rearmNextFireAt(execution, definition, Objects.requireNonNull(attempt.finishedAt()))),
                () -> events.publish(new Failed(execution.id(), execution.jobKey(), attemptNumber, error, attemptsExhausted)));
    }

    /**
     * Publica os eventos só se o CAS de conclusão passou — uma conclusão
     * concorrente (reaper/outro caminho) descarta o resultado com WARN,
     * nunca publica evento de uma transição que não ocorreu.
     */
    private void completeOrDiscard(ExecutionStore.CompletionRequest request, Runnable publishEvents) {
        if (executionStore.complete(request, jobStore)) {
            publishEvents.run();
        } else {
            log.warn("attempt {} of execution {} finished {} but the state had already moved on (reaper/concurrent completion) — result discarded",
                    request.attempt().number(), request.id().value(), request.attempt().outcome());
        }
    }

    private Attempt failedAttempt(int attemptNumber, Instant firedAt, Exception error) {
        return new Attempt(attemptNumber, firedAt, clock.instant(), ExecutionState.FAILED, errorMessage(error));
    }

    private static String errorMessage(Exception error) {
        return Objects.requireNonNullElse(error.getMessage(), error.toString());
    }

    /**
     * Falha uma execução terminalmente sem ter passado por {@link #dispatch}
     * — mesma síntese de {@link Attempt} e publicação de {@link Failed} que
     * {@link #failTerminally} já usa, pra quando o chamador (ex.:
     * {@link Engine}, quando o payload não pôde ser lido de volta) já sabe
     * que a execução falhou antes do handler sequer poder rodar. Sempre
     * terminal, sem consultar orçamento: definição removida não tem
     * {@code retries} confiável pra ler, e payload ilegível não sara
     * repetindo a leitura do mesmo dado. {@code Failed.attemptsExhausted}
     * sai {@code false} — terminal por natureza, não por orçamento.
     * {@code definition} nula quando o chamador não a tem (removida entre
     * claim e dispatch) — sem rearme fixed-delay nesse caso (ADR-0035).
     */
    void failBeforeDispatch(Execution execution, @Nullable JobDefinition definition, Exception cause) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(cause, "cause");
        int attemptNumber = execution.attempts().size() + 1;
        failTerminally(execution, definition, attemptNumber, clock.instant(), cause, false);
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
