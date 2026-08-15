package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import io.github.robsonkades.uuidv7.UUIDv7;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.execution.Execution;

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
 * <p>{@code tickScheduler}/{@code runnerRegistry} são injetados —
 * construídos por {@link MohsExecutors}/quem monta o {@link RunnerRegistry},
 * não por esta classe. {@link #stop} nunca desliga o que recebeu: ciclo de
 * vida deles é de quem os construiu (mesma disciplina documentada em
 * {@link MohsExecutors}), só {@link #drain} espera o que já está em voo
 * terminar.
 *
 * <p><b>Limitações conhecidas desta rodada, documentadas, não escondidas:</b>
 * sem Watchdog Bound (lease de execuções já {@code RUNNING} de ticks
 * anteriores não é renovada — só quem acaba de ser reivindicado ganha
 * lease nova; handlers mais lentos que {@code leaseTtl} podem ser
 * reclamados pelo {@link Reaper} prematuramente); sem mecanismo de
 * interrupt real — no estouro do grace de {@link #drain}, o trabalho em
 * voo continua rodando em segundo plano, só loga um aviso. Como não há
 * retry (ADR-0026), a garantia efetiva sob falha de nó hoje é
 * <b>at-most-once</b>: dimensione {@code mohs.engine.lease-ttl} acima do
 * pior handler esperado (o boot avisa por job — {@code MohsEngineLifecycle}).
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
    private final TaskScheduler tickScheduler;
    private final RunnerRegistry runnerRegistry;

    /** {@code volatile}: escrito por {@link #start} e lido por {@link #stop}, que podem vir de threads distintas ({@code MohsLifecycle} é API pública) — publicação segura, JCIP 3.1. */
    private volatile @Nullable ScheduledFuture<?> tickHandle;

    public Engine(
            Claimer claimer,
            Dispatcher dispatcher,
            ExecutionStore executionStore,
            JobStore jobStore,
            NodeStore nodeStore,
            Reaper reaper,
            Clock clock,
            Duration pollInterval,
            int batchSize,
            TaskScheduler tickScheduler,
            RunnerRegistry runnerRegistry
    ) {
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
        this.tickScheduler = Objects.requireNonNull(tickScheduler, "tickScheduler");
        this.runnerRegistry = Objects.requireNonNull(runnerRegistry, "runnerRegistry");
        this.nodeId = UUIDv7.randomUUID().toString();
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
        ScheduledFuture<?> handle = tickScheduler.scheduleWithFixedDelay(this::tick, pollInterval);
        tickHandle = handle;
        if (state.get() == EngineState.STOPPED) { // stop() venceu a corrida durante o agendamento — cancela o tick que ele não viu
            handle.cancel(false);
        }
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

    /**
     * {@link #drain} seguido de cancelar o agendamento do tick.
     * {@code tickScheduler}/{@code runnerRegistry} continuam vivos —
     * não são desta classe pra desligar (ver Javadoc da classe).
     */
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
        ScheduledFuture<?> handle = tickHandle;
        if (handle != null) {
            handle.cancel(false);
        }
        state.set(EngineState.STOPPED);
    }

    /**
     * Espera {@code inFlight} esvaziar em loop, não num snapshot só — um
     * tick que já tinha passado de {@code claimer.claim(...)} no instante
     * exato do CAS pra {@code DRAINING} ainda vai submeter as execuções que
     * acabou de reivindicar, e essas entram em {@code inFlight} depois de
     * qualquer snapshot único já tirado. Um segundo snapshot pega o que o
     * primeiro perdeu; o loop para quando {@code inFlight} esvazia de
     * verdade ou o {@code grace} acaba, o que vier primeiro.
     */
    private void awaitInFlight(Duration grace) {
        // System.nanoTime, não clock.instant(): duração se mede com tempo
        // monotônico — o Clock injetado pode ser o DatabaseClock, cujo offset
        // salta a cada resync e encurtaria/esticaria o grace de shutdown.
        long deadlineNanos = System.nanoTime() + grace.toNanos();
        while (!inFlight.isEmpty()) {
            long remainingMillis = (deadlineNanos - System.nanoTime()) / 1_000_000;
            if (remainingMillis <= 0) {
                log.warn("drain grace period ({}) elapsed with {} execution(s) still in flight — Mohs has no interrupt mechanism yet (per-job timeout isn't enforced), they keep running in the background", grace, inFlight.size());
                return;
            }
            CompletableFuture<?>[] snapshot = inFlight.toArray(CompletableFuture[]::new);
            try {
                CompletableFuture.allOf(snapshot).get(remainingMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                log.warn("drain grace period ({}) elapsed with {} execution(s) still in flight — Mohs has no interrupt mechanism yet (per-job timeout isn't enforced), they keep running in the background", grace, inFlight.size());
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                log.warn("unexpected exception waiting for in-flight dispatch during drain", e);
            }
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

    /**
     * Fire-and-forget em relação ao próximo tick: um handler lento não trava
     * a próxima rodada de claim. O executor do runner pode rejeitar
     * ({@link MohsExecutors#ioBoundExecutor} rejeita acima do teto de
     * concorrência, de propósito — backpressure real) — {@code
     * CompletableFuture.runAsync} propaga essa rejeição de forma síncrona,
     * antes mesmo de existir um {@code future}; sem capturar aqui, uma única
     * rejeição no meio do lote abortaria o {@code for} de {@link #tick} e
     * deixaria as execuções seguintes já reivindicadas (RUNNING no banco)
     * órfãs até a lease expirar e o {@link Reaper} reclamá-las — caminho de
     * recuperação bem mais lento que necessário.
     */
    private void submitDispatch(Execution execution) {
        // Consulta síncrona, na própria thread do tick — mesmo padrão de
        // claimer.claim()/reaper.reclaimExpired() em tick(). Deliberadamente uma
        // consulta por execução, não em lote pro batch inteiro: candidato a
        // próximo DBTUNE se LivenessLoadHarness mostrar que importa, não
        // otimizado especulativamente agora (sem número, não é otimização).
        Optional<StoredJob> storedJob;
        try {
            storedJob = jobStore.find(execution.jobKey());
        } catch (RuntimeException e) {
            // Erro de infra (banco fora, pool esgotado) não é veredito sobre a
            // execução — falhar terminalmente aqui violaria at-least-once, o
            // runner pode existir e o handler nunca teria rodado. Mesmo caminho
            // de recuperação da rejeição do executor logo abaixo.
            log.warn("could not load the job definition for execution {} — already claimed, will sit RUNNING until the reaper reclaims it on lease expiry",
                    execution.id(), e);
            return;
        }
        if (storedJob.isEmpty()) {
            failBeforeDispatchGuarded(execution, new IllegalStateException(
                    "job definition for " + execution.jobKey() + " was removed after this execution was claimed (e.g. Mohs.remove between claim and dispatch)"));
            return;
        }

        AsyncTaskExecutor executor;
        try {
            executor = runnerRegistry.resolve(storedJob.get().definition().runner());
        } catch (NoSuchElementException e) {
            failBeforeDispatchGuarded(execution, new IllegalStateException(
                    "runner could not be resolved: " + (e.getMessage() != null ? e.getMessage() : e.toString()), e));
            return;
        }

        CompletableFuture<Void> future;
        try {
            future = CompletableFuture.runAsync(() -> resolvePayloadAndDispatch(execution), executor);
        } catch (RuntimeException e) {
            log.warn("runner executor rejected execution {} — already claimed, will sit RUNNING until the reaper reclaims it on lease expiry",
                    execution.id(), e);
            return;
        }
        inFlight.add(future);
        future.whenComplete((_, _) -> inFlight.remove(future));
    }

    /**
     * Falha terminal decidida ainda na thread do tick — definição removida ou
     * nome de runner sem correspondente no {@link RunnerRegistry} (config
     * removida depois que o job foi definido, ou erro de digitação que a
     * validação de boot — fora de escopo ainda — não pega) falha só esta
     * execução, mesmo racional de {@link #failUnreadablePayload}: nunca
     * derruba o tick nem o node por causa de uma definição só. Guardada
     * porque, ao contrário de {@code failUnreadablePayload} (que roda dentro
     * da task assíncrona), isto roda no {@code for} de {@link #tick}: se a
     * própria gravação da falha lançar (banco, executor de eventos saturado),
     * o resto do lote ainda precisa ser despachado — loga e deixa esta
     * execução RUNNING pro {@link Reaper} reclamá-la na expiração da lease.
     */
    private void failBeforeDispatchGuarded(Execution execution, IllegalStateException error) {
        try {
            dispatcher.failBeforeDispatch(execution, error);
        } catch (RuntimeException e) {
            log.warn("could not record the terminal failure of execution {} ({}) — will sit RUNNING until the reaper reclaims it on lease expiry",
                    execution.id(), error.getMessage(), e);
        }
    }

    /**
     * Resolve o payload gravado por {@code ExecutionStore#insert} — a única
     * coisa que só {@link Engine} sabe fazer aqui, já que {@link Dispatcher}
     * é deliberadamente agnóstico a como o payload chega (testável com
     * {@code Object} puro) — e só então delega pra
     * {@link Dispatcher#dispatch}. Payload ilegível vira falha terminal via
     * {@link #failUnreadablePayload}, nunca propaga.
     */
    private void resolvePayloadAndDispatch(Execution execution) {
        Object payload;
        try {
            payload = executionStore
                    .findPayload(execution.id())
                    .orElseThrow(() -> new IllegalStateException("execution " + execution.id() + " vanished before payload could be read — should be unreachable"));
        } catch (RuntimeException e) {
            failUnreadablePayload(execution, e);
            return;
        }
        dispatcher.dispatch(execution, payload);
    }

    /**
     * Payload corrompido/classe sumida do classpath não trava o ciclo —
     * falha só esta execução, direto, sem passar pelo handler.
     * {@link Dispatcher#failBeforeDispatch} sintetiza o {@code Attempt} e
     * publica {@code Failed} — mesmo caminho que qualquer outra falha
     * terminal do sistema usa, então um {@code ExecutionListener} é
     * notificado aqui também. A causa original preservada em
     * {@code getCause()}; a mensagem prefixada é o que fica gravado no
     * {@code Attempt}/publicado no evento, contexto que só {@link Engine}
     * tem (não é falha de handler nem de interceptor).
     */
    private void failUnreadablePayload(Execution execution, RuntimeException cause) {
        String message = "payload could not be read: " + (cause.getMessage() != null ? cause.getMessage() : cause.toString());
        dispatcher.failBeforeDispatch(execution, new IllegalStateException(message, cause));
    }

    /** Package-private — só {@code EngineTest} usa isto pra confirmar a identidade do node por trás de heartbeat/claim. */
    String nodeId() {
        return nodeId;
    }
}
