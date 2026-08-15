package io.mohs.engine;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;

import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
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
 * <p>Sempre termina em {@code SUCCEEDED} ou {@code FAILED}, nunca {@code
 * RETRY_SCHEDULED} — mesma restrição da ADR-0026 (a claim query não
 * reconhece esse estado como candidato em nenhum dos 4 dialetos ainda;
 * agendar um retry hoje prenderia a execução pra sempre). {@code retries}/
 * {@code retryPolicy} continuam sem efeito, consistente com o reaper.
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

    public void dispatch(Execution execution, Object payload) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(payload, "payload");

        Instant firedAt = clock.instant();
        executionStore.markFired(execution.id(), firedAt);

        int attemptNumber = execution.attempts().size() + 1;
        JobContext ctx = new DefaultJobContext(execution.jobKey(), execution.id(), attemptNumber, execution.scheduledAt(), firedAt);
        events.publish(new Started(execution.id(), execution.jobKey(), attemptNumber, firedAt));

        Optional<JobHandler> invocation = handlerRegistry.find(execution.jobKey());
        if (invocation.isEmpty()) {
            fail(execution, attemptNumber, firedAt, new IllegalStateException(NO_HANDLER_ERROR + execution.jobKey().value()));
            return;
        }

        try {
            runInterceptorChain(invocation.get(), payload, ctx);
            succeed(execution, attemptNumber, firedAt);
        } catch (Exception e) {
            fail(execution, attemptNumber, firedAt, e);
        }
    }

    /**
     * Chain of Responsibility clássica (GoF): cada interceptor embrulha o
     * próximo, o mais interno chama o handler de verdade. Roda na própria
     * thread do dispatch — Javadoc de {@link ExecutionInterceptor} já exige
     * isso (lugar de MDC/tracing/{@code ScopedValue}); exceção de
     * interceptor É falha de attempt, mesmo tratamento que exceção do
     * handler.
     */
    private void runInterceptorChain(JobHandler invocation, Object payload, JobContext ctx) throws Exception {
        ExecutionInterceptor.Chain chain = () -> invocation.invoke(payload, ctx);
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            ExecutionInterceptor interceptor = interceptors.get(i);
            ExecutionInterceptor.Chain next = chain;
            chain = () -> interceptor.intercept(ctx, next);
        }
        chain.proceed();
    }

    private void succeed(Execution execution, int attemptNumber, Instant firedAt) {
        Attempt attempt = new Attempt(attemptNumber, firedAt, clock.instant(), ExecutionState.SUCCEEDED, null);
        boolean completed = executionStore.complete(
                new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(), attempt, ExecutionState.SUCCEEDED), jobStore);
        if (completed) {
            events.publish(new Succeeded(execution.id(), execution.jobKey(), attemptNumber));
        } else {
            log.warn("attempt {} of execution {} finished SUCCEEDED but the state had already moved on (reaper/concurrent completion) — result discarded",
                    attemptNumber, execution.id().value());
        }
    }

    /**
     * O WARN com a exceção completa é o único lugar onde o stack trace da
     * falha aparece por padrão — {@code Attempt.error} guarda só a mensagem
     * e o evento {@code Failed} depende de um {@code ExecutionListener}
     * registrado; sem este log, a causa de um job quebrado às 3h da manhã
     * não estaria em lugar nenhum.
     */
    private void fail(Execution execution, int attemptNumber, Instant firedAt, Exception error) {
        String message = error.getMessage() != null ? error.getMessage() : error.toString();
        log.warn("execution {} of job '{}' failed on attempt {}", execution.id().value(),
                execution.jobKey().value(), attemptNumber, error);
        Attempt attempt = new Attempt(attemptNumber, firedAt, clock.instant(), ExecutionState.FAILED, message);
        boolean completed = executionStore.complete(
                new ExecutionStore.CompletionRequest(execution.id(), execution.jobKey(), attempt, ExecutionState.FAILED), jobStore);
        if (completed) {
            // sempre esgotado nesta rodada — nunca agenda RETRY_SCHEDULED (ADR-0026)
            events.publish(new Failed(execution.id(), execution.jobKey(), attemptNumber, error, true));
        } else {
            log.warn("attempt {} of execution {} finished FAILED but the state had already moved on (reaper/concurrent completion) — result discarded",
                    attemptNumber, execution.id().value());
        }
    }

    /**
     * Falha uma execução terminalmente sem ter passado por {@link #dispatch}
     * — mesma síntese de {@link Attempt} e publicação de {@link Failed} que
     * {@link #fail} já usa, pra quando o chamador (ex.: {@link Engine},
     * quando o payload não pôde ser lido de volta) já sabe que a execução
     * falhou antes do handler sequer poder rodar.
     */
    void failBeforeDispatch(Execution execution, Exception cause) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(cause, "cause");
        int attemptNumber = execution.attempts().size() + 1;
        fail(execution, attemptNumber, clock.instant(), cause);
    }

    /**
     * {@link #cancellationRequested()} sempre {@code false} nesta rodada —
     * não existe fonte nenhuma pra "cancelamento pedido" ainda ({@code POST
     * /executions/{id}/cancel} não existe, sem coluna/mecanismo). Honesto: não
     * finge um cache que não tem o que observar. {@link #progress} é no-op —
     * já é o contrato documentado quando nada observa.
     */
    private record DefaultJobContext(JobKey jobKey, ExecutionId executionId, int attempt, Instant scheduledAt, Instant firedAt) implements JobContext {

        @Override
        public boolean cancellationRequested() {
            return false;
        }

        @Override
        public void progress(int done, int total) {
        }
    }
}
