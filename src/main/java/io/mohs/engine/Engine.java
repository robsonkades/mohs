package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;

/**
 * O motor: liga {@link Claimer}, {@link Dispatcher}, {@link NodeStore} e
 * {@link Reaper} num ciclo que roda de fato — nenhum dos quatro tinha
 * chamador em produção antes desta classe (todos testáveis isoladamente,
 * mesma situação que {@code Claimer}/{@code Reaper} já tinham). Implementa
 * {@link MohsLifecycle} diretamente: "exposta via {@code mohs.lifecycle()}"
 * (ADR-0007) já é a mesma máquina de estados que o poll loop precisa —
 * ligar a fachada {@code Mohs} a este motor vira delegação simples, tarefa
 * separada.
 *
 * <p>Poll de intervalo fixo, não wakeup event-driven (LISTEN/NOTIFY) — essa
 * é uma aposta ainda não paga (`MOHS-DOCUMENTO-MESTRE.md`, "riscos e
 * lacunas"), não uma decisão fechada; intervalo fixo é a linha de base
 * correta agora. {@code nodeId} é um UUID gerado por instância — identidade
 * estável entre reinícios é decisão de {@code io.mohs.autoconfigure}
 * (ainda não construído), não desta classe.
 *
 * <p><b>Limitações conhecidas desta rodada, documentadas, não escondidas:</b>
 * sem Watchdog Bound (lease de execuções já {@code RUNNING} de ticks
 * anteriores não é renovada — só quem acaba de ser reivindicado ganha
 * lease nova; handlers mais lentos que {@code leaseTtl} podem ser
 * reclamados pelo {@link Reaper} prematuramente); sem mecanismo de
 * interrupt real — no estouro do grace de {@link #drain}, o trabalho em
 * voo continua rodando em segundo plano, só loga um aviso.
 */
public final class Engine implements MohsLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    private final Claimer claimer;
    private final Dispatcher dispatcher;
    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    private final NodeStore nodeStore;
    private final Reaper reaper;
    private final Clock clock;
    private final Duration pollInterval;
    private final int batchSize;
    private final String nodeId;

    private final AtomicReference<EngineState> state = new AtomicReference<>(EngineState.CREATED);
    private final Set<CompletableFuture<Void>> inFlight = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService tickScheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("mohs-engine-tick").factory());
    private final ExecutorService dispatchExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("mohs-dispatch-", 0).factory());

    private @Nullable ScheduledFuture<?> tickHandle;

    public Engine(Claimer claimer, Dispatcher dispatcher, ExecutionStore executionStore, JobStore jobStore,
            NodeStore nodeStore, Reaper reaper, Clock clock, Duration pollInterval, int batchSize) {
        this.claimer = Objects.requireNonNull(claimer, "claimer");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
        this.reaper = Objects.requireNonNull(reaper, "reaper");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
        this.nodeId = UUID.randomUUID().toString();
    }

    @Override
    public EngineState state() {
        return state.get();
    }

    /** CAS guardado (mesma disciplina do resto do projeto — ADR-0018), não lock: transição só vale se o estado de origem ainda for o esperado. */
    @Override
    public void start() {
        if (!state.compareAndSet(EngineState.CREATED, EngineState.RUNNING)) {
            throw new IllegalStateException("start() only valid from CREATED, was " + state.get());
        }
        long intervalMillis = pollInterval.toMillis();
        tickHandle = tickScheduler.scheduleWithFixedDelay(this::tick, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void pause() {
        if (!state.compareAndSet(EngineState.RUNNING, EngineState.PAUSED)) {
            throw new IllegalStateException("pause() only valid from RUNNING, was " + state.get());
        }
    }

    @Override
    public void resume() {
        if (!state.compareAndSet(EngineState.PAUSED, EngineState.RUNNING)) {
            throw new IllegalStateException("resume() only valid from PAUSED, was " + state.get());
        }
    }

    /** Não agenda claim/reclaim novo; espera o in-flight até {@code grace}. Drain ≠ cancel (ADR-0007). */
    @Override
    public void drain(Duration grace) {
        Objects.requireNonNull(grace, "grace");
        EngineState current = state.get();
        if (current != EngineState.RUNNING && current != EngineState.PAUSED) {
            throw new IllegalStateException("drain() only valid from RUNNING/PAUSED, was " + current);
        }
        if (!state.compareAndSet(current, EngineState.DRAINING)) {
            throw new IllegalStateException("concurrent state change during drain(), was " + state.get());
        }
        awaitInFlight(grace);
    }

    /** {@link #drain} seguido do desligamento dos executors. */
    @Override
    public void stop(Duration grace) {
        Objects.requireNonNull(grace, "grace");
        EngineState current = state.get();
        if (current == EngineState.STOPPED) {
            throw new IllegalStateException("already STOPPED");
        }
        if (current != EngineState.DRAINING) {
            drain(grace);
        }
        if (tickHandle != null) {
            tickHandle.cancel(false);
        }
        tickScheduler.shutdown();
        dispatchExecutor.shutdown();
        state.set(EngineState.STOPPED);
    }

    private void awaitInFlight(Duration grace) {
        CompletableFuture<?>[] snapshot = inFlight.toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(snapshot).get(grace.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("drain grace period ({}) elapsed with {} execution(s) still in flight — Mohs has no interrupt "
                    + "mechanism yet (per-job timeout isn't enforced), they keep running in the background", grace, inFlight.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("unexpected exception waiting for in-flight dispatch during drain", e);
        }
    }

    /**
     * Publica heartbeat sempre, refletindo o estado atual — mesmo fora de
     * {@code RUNNING} (ADR-0012: heartbeat é só informativo, útil pra
     * {@code GET /nodes} mostrar "pausado"/"drenando", não "morto"). Claim
     * e reclaim só rodam em {@code RUNNING}.
     */
    private void tick() {
        try {
            EngineState current = state.get();
            nodeStore.heartbeat(nodeId, current, clock.instant());
            if (current != EngineState.RUNNING) {
                return;
            }
            reaper.reclaimExpired();
            List<Execution> claimed = claimer.claim(nodeId, batchSize);
            for (Execution execution : claimed) {
                submitDispatch(execution);
            }
        } catch (RuntimeException e) {
            log.error("engine tick failed — will retry next tick", e);
        }
    }

    /** Fire-and-forget em relação ao próximo tick: um handler lento não trava a próxima rodada de claim. */
    private void submitDispatch(Execution execution) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> dispatchOne(execution), dispatchExecutor);
        inFlight.add(future);
        future.whenComplete((result, error) -> inFlight.remove(future));
    }

    private void dispatchOne(Execution execution) {
        Object payload;
        try {
            payload = executionStore.findPayload(execution.id()).orElseThrow(
                    () -> new IllegalStateException("execution " + execution.id() + " vanished before payload could be read — should be unreachable"));
        } catch (RuntimeException e) {
            failUnreadablePayload(execution, e);
            return;
        }
        dispatcher.dispatch(execution, payload);
    }

    /** Payload corrompido/classe sumida do classpath não trava o ciclo — falha só esta execução, direto, sem passar pelo handler. */
    private void failUnreadablePayload(Execution execution, RuntimeException cause) {
        Instant now = clock.instant();
        int attemptNumber = execution.attempts().size() + 1;
        String message = "payload could not be read: " + (cause.getMessage() != null ? cause.getMessage() : cause.toString());
        Attempt attempt = new Attempt(attemptNumber, now, now, ExecutionState.FAILED, message);
        executionStore.complete(execution.id(), execution.jobKey(), attempt, ExecutionState.FAILED, jobStore);
    }

    /** Package-private — só {@code EngineTest} usa isto pra confirmar a identidade do node por trás de heartbeat/claim. */
    String nodeId() {
        return nodeId;
    }
}
