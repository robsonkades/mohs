package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import io.github.robsonkades.uuidv7.UUIDv7;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;

/**
 * O motor sobre as portas da Phase 5 (ADR-A): liga {@link WorkQueue},
 * {@link Dispatcher}, {@link HistoryStore}, {@link LeaseStore} e
 * {@link NodeStore} num ciclo que roda de fato. Implementa
 * {@link MohsLifecycle} diretamente: "exposta via {@code mohs.lifecycle()}"
 * (ADR-0007) já é a mesma máquina de estados que o poll loop precisa.
 *
 * <p>Poll ADAPTATIVO com hand-off local (Phase 6, §5.5/ADR-G): o loop
 * dorme entre o piso e o teto conforme acha trabalho, e um enqueue DESTA
 * JVM já devido o acorda ({@link #signalWorkScheduled}). Wakeup
 * event-driven cross-nó (LISTEN/NOTIFY) foi implementado, MEDIDO e
 * retirado — a transação notificante não participa de group commit e
 * serializou o ingest (ADR-0054, com os números); o poll adaptativo é o
 * único backstop cross-nó, latência limitada por
 * {@code max-poll-interval}. {@code nodeId} é um UUID gerado por
 * instância.
 *
 * <p><b>Guards de admissão (§5.4):</b> os predicados que a era da tabela
 * única pagava POR CANDIDATO no SQL do claim (janela, rate limit, cap de
 * concorrência) viraram a lista de inadmissíveis POR JOB, computada em
 * memória antes de cada rodada — e a sobra é resolvida DEPOIS do claim
 * pelo caminho de perda de admissão ({@link WorkQueue#requeue}, sem
 * consumir orçamento). O cap é DERIVADO de {@code mohs_lease} (§5.7,
 * ADR-D): contar a posse viva substitui o contador quente
 * {@code running_execution_count}; sobre-admissão entre nós é limitada a
 * 1 rodada × nós e corrige na rodada seguinte.
 *
 * <p><b>Liveness (ADR-0051 sobre a mesa nova):</b> por NÓ — heartbeat com
 * promessa {@code now + node-lease-ttl}; o reaper deste motor reclama
 * leases de nós mortos ({@link LeaseStore#findOrphaned}) decidindo pelo
 * orçamento de retry, e TODA conclusão é cercada por
 * {@code (node_id, epoch)} (§6.3). Sob falha de nó a garantia é
 * <b>at-least-once</b> quando {@code retries > 0}.
 *
 * <p><b>Timeout e cancelamento (ADR-0034):</b> verificados de carona no
 * tick — flag + interrupt via {@link CancellationSignal}, desfecho passivo
 * no {@link Dispatcher}. Handler que ignora o interrupt continua zumbi até
 * terminar sozinho e tem o resultado descartado pelo fence — é o caso do
 * Watchdog Bound, o degrau seguinte da escada.
 */
public final class Engine implements MohsLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    /**
     * Teto de triggers disparados por tick — um boot depois de downtime
     * longo não vira uma varredura sem limite; o excedente continua devido
     * e drena nos ticks seguintes, mais antigo primeiro.
     */
    static final int FIRE_LIMIT = 500;

    /**
     * Teto de reclaims por tick — morte de nó em massa não vira uma
     * transação sem limite de locks; o excedente drena nos ticks
     * seguintes, mais antigo primeiro ({@code claimed_at}).
     */
    static final int RECLAIM_LIMIT = 500;

    /**
     * Retention da linha de heartbeat (ADR-0041), em múltiplos de
     * {@code lease-ttl} — derivada de propósito, nenhum knob novo. 10
     * leases (5 min no default) mantém o node morto VISÍVEL como stale por
     * tempo de sobra para {@code GET /nodes} e alertas.
     */
    static final int STALE_NODE_RETENTION_LEASES = 10;

    private static final String NODE_DEAD_ERROR = "node lease expired — node presumed dead";

    /**
     * Teto da lista de inadmissíveis que vai ao {@code NOT IN} do claim —
     * folga confortável sob o limite de ~2100 parâmetros do driver do SQL
     * Server (o menor dos 4 dialetos), já descontado o restante do bind.
     */
    static final int MAX_INADMISSIBLE_FILTER = 1000;

    private final WorkQueue workQueue;
    private final Dispatcher dispatcher;
    private final HistoryStore historyStore;
    private final LeaseStore leaseStore;
    private final JobStore jobStore;
    private final NodeStore nodeStore;
    private final TriggerFirer triggerFirer;
    private final FiringPlanner firingPlanner;
    private final ExecutionWindowRegistry windowRegistry;
    private final RateLimitStore rateLimitStore;
    private final Clock clock;
    private final EngineSettings settings;
    private final String nodeId;
    private final EngineMetrics metrics;

    private final AtomicReference<EngineState> state = new AtomicReference<>(EngineState.CREATED);
    private final Set<CompletableFuture<Void>> inFlight = ConcurrentHashMap.newKeySet();
    /**
     * As execuções em voo NESTE node — a base do watchdog (ADR-0051) e da
     * escada de timeout/cancelamento (ADR-0034) a cada tick. Entrada nasce
     * no submit e SÓ morre na conclusão ({@code whenComplete}): a
     * liberação de posse pelo watchdog MARCA a encarnação
     * ({@code ownershipReleased}) em vez de removê-la — o zumbi continua
     * alcançável pela escalada de drain e pelo poll de cancel (review
     * ADR-0034). Exceção única: um re-claim DESTE node pro mesmo id
     * substitui a entrada marcada no {@code put} do submit (o mapa
     * comporta uma encarnação por id de propósito).
     */
    private final ConcurrentHashMap<ExecutionId, InFlightAttempt> inFlightAttempts = new ConcurrentHashMap<>();
    /** Candidatas do passe de reconciliação ({@link #reconcileOwnStrayLeases}) — só a thread do tick toca (JCIP 3.3). */
    private final Set<ExecutionId> strayLeaseCandidates = new HashSet<>();
    /**
     * Cursor de rotação do lap de claim (§5.4) — avança a cada shard
     * sondado e PERSISTE entre ticks, então o primeiro shard de cada lap
     * gira em vez de sempre recomeçar no mesmo: sem ele, um tick que esgota
     * o orçamento no shard 0 deixaria os últimos da lista famintos. Só a
     * thread do tick toca (JCIP 3.3); overflow é benigno via
     * {@code Math.floorMod}.
     */
    private int shardCursor;
    /**
     * A última volta COMPLETA pelos shards próprios voltou vazia? É o que
     * arma o gate ocioso do S6.5 (ver {@link #claimAndDispatch}); "completa"
     * é a distinção que o {@link LapOutcome} existe pra fazer — zero
     * reivindicado por saturação de dispatch ou orçamento estourado não é
     * fila vazia. Só a thread do tick toca (JCIP 3.3). Nasce {@code false}:
     * o primeiro tick de um engine que acabou de subir não tem direito a
     * supor fila vazia.
     */
    private boolean queueLooksEmpty;
    private final RunnerRegistry runnerRegistry;

    /**
     * O despertador do loop (§5.5): {@code ReentrantLock}+{@code Condition}
     * no lugar de {@code synchronized}/{@code wait} (JCIP 13 — timeout no
     * await é o que o backoff precisa) e a flag {@code wakeRequested}
     * absorve o sinal que chega ANTES do await (um {@code signal} sem
     * ninguém esperando se perde; a flag não — o padrão condition-predicate
     * de JCIP 14.2).
     */
    private final ReentrantLock wakeLock = new ReentrantLock();
    private final Condition wakeCondition = wakeLock.newCondition();
    /** Guardado por {@code wakeLock}. */
    private boolean wakeRequested;

    /** {@code volatile}: escrito por {@link #start} e lido por {@link #stop}, que podem vir de threads distintas ({@code MohsLifecycle} é API pública) — publicação segura, JCIP 3.1. */
    private volatile @Nullable Thread loopThread;

    /** O WARN de escalada de grace é único por shutdown — o SINAL não é guardado por ele (ver {@link #escalateAfterDrainGrace}). */
    private final AtomicBoolean drainEscalated = new AtomicBoolean();

    /**
     * A encarnação e o lease do NÓ (ADR-0051) — escritos só pela thread do
     * tick ({@link #renewNodeLease}). Epoch começa em 1 e só sobe quando o
     * próprio node percebe o lease expirado (§11.2 do redesign); desde a
     * Phase 5 ele também é o fence de toda conclusão (§6.3).
     * {@code volatile} no epoch só pelo leitor de fora:
     * {@link #writeFinalStoppedHeartbeat} lê de outra thread no stop
     * (JCIP 3.1; JLS 17.7 pra {@code long}) — mesma razão de {@code tickHandle}.
     */
    private volatile long nodeEpoch = 1;
    private @Nullable Instant nodeLeaseExpiresAt;

    public Engine(
            WorkQueue workQueue,
            Dispatcher dispatcher,
            HistoryStore historyStore,
            LeaseStore leaseStore,
            JobStore jobStore,
            NodeStore nodeStore,
            TriggerFirer triggerFirer,
            ExecutionWindowRegistry windowRegistry,
            RateLimitStore rateLimitStore,
            Clock clock,
            EngineSettings settings,
            RunnerRegistry runnerRegistry,
            EngineMetrics metrics
    ) {
        this.workQueue = Objects.requireNonNull(workQueue, "workQueue");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.historyStore = Objects.requireNonNull(historyStore, "historyStore");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
        this.triggerFirer = Objects.requireNonNull(triggerFirer, "triggerFirer");
        this.windowRegistry = Objects.requireNonNull(windowRegistry, "windowRegistry");
        this.rateLimitStore = Objects.requireNonNull(rateLimitStore, "rateLimitStore");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.firingPlanner = new FiringPlanner(new NextFireCalculator(), settings.misfireThreshold());
        this.runnerRegistry = Objects.requireNonNull(runnerRegistry, "runnerRegistry");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.nodeId = UUIDv7.randomUUID().toString();
        metrics.bindNodeGauges(inFlightAttempts::size, settings.dispatchConcurrency());
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
        // UMA platform thread, nunca scheduler/virtual (§12.1): latência-crítica,
        // imune a starvation de carrier, e aparece com nome próprio em qualquer
        // profiler/thread dump — o que importa às 3h da manhã. Daemon: um engine
        // vazado nunca segura o exit da JVM (crash já é semântica coberta —
        // lease do nó expira, reaper reclama).
        Duration heartbeatCadence = settings.nodeLeaseTtl().dividedBy(3);
        if (settings.pollInterval().compareTo(heartbeatCadence) > 0) {
            // o cap de liveness do awaitWork também engole o PISO — o operador
            // pediu ticks mais espaçados do que a promessa de vida permite;
            // liveness vence, mas em silêncio seria um mistério de tuning
            log.warn("effective tick cadence is capped at node-lease-ttl/3 ({}) — mohs.engine.poll-interval ({}) "
                    + "exceeds it; the heartbeat each tick carries is what keeps this node alive to its peers",
                    heartbeatCadence, settings.pollInterval());
        }
        Thread thread = Thread.ofPlatform().name("mohs-engine-loop").daemon(true).unstarted(this::runLoop);
        loopThread = thread;
        thread.start();
        if (state.get() == EngineState.STOPPED) { // stop() venceu a corrida durante a largada — acorda o loop que ele não viu
            wake();
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
        // pausa longa deixa o backoff no teto — o operador que resumiu não
        // deve esperar o teto vencer pra ver o primeiro claim
        wake();
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
        awaitInFlight(DrainDeadline.startingNow(grace));
    }

    /**
     * {@link #drain} seguido de encerrar o loop, nesta ordem: drain →
     * {@code STOPPED} → join do loop → espera do in-flight → heartbeat
     * final. {@code runnerRegistry} continua vivo — não é desta classe pra
     * desligar (ver Javadoc da classe).
     *
     * <p>O join espera o tick em curso até {@code nodeLeaseTtl/4} (o
     * próprio orçamento de lap do tick) — encolhe a janela da corrida
     * documentada em {@link #writeFinalStoppedHeartbeat} sem acoplar o
     * shutdown a um tick pendurado.
     *
     * <p>Esperar in-flight de novo depois do join não é redundância com o
     * drain: a espera do drain observa {@code inFlight} com o loop ainda
     * rodando, e um tick que já reivindicou um lote registra as futures
     * DEPOIS do {@code runAsync} — achar o conjunto vazio ali prova que o
     * trabalho ainda não foi registrado, não que ele acabou. Com o loop já
     * parado nenhum submit novo entra e vazio é vazio (join estourado
     * encolhe essa janela em vez de fechá-la). Sem a segunda espera o
     * heartbeat final declararia a lease vencida (ADR-0051) com execuções
     * rodando, e o reaper de um par as reclamaria: o resultado bem-sucedido
     * é descartado pelo fence e vira re-entrega — ou perda terminal, quando
     * não há orçamento de retry.
     *
     * <p>Os dois prazos de UM {@code stop} nascem no mesmo instante — este
     * abre o seu {@link DrainDeadline} e o {@link #drain} abre o dele em
     * seguida —, então o shutdown continua limitado ao {@code grace}
     * pedido: o daqui é o mais antigo dos dois e é ele que a espera
     * pós-loop cobra. Já {@code drain(g)} seguido de {@code stop(g)} são
     * dois prazos SEPARADOS no tempo — a sequência pode custar até
     * {@code 2×g} quando o drain escala e o handler ignora o interrupt.
     *
     * <p>Duas coisas que a espera pós-loop NÃO cobre, e que decidem o
     * comportamento com os defaults:
     * <ul>
     *   <li><b>Ninguém renova a lease do nó durante ela</b> —
     *       {@code renewNodeLease} só roda no tick, que já parou. A proteção
     *       acaba no máximo {@code node-lease-ttl} depois do último tick —
     *       menos, na prática, porque o resíduo já vinha correndo: com TTL
     *       de 15s e {@code grace} de 30s, mais da metade da espera está
     *       descoberta e um par pode reclamar o que ainda roda aqui.</li>
     *   <li><b>A escada de sinalização fica suspensa</b> — sem tick não há
     *       {@code signalJobTimeouts}, {@code pollCancelRequests} nem
     *       Watchdog Bound (ADR-0034). Um job com {@code timeout} menor que
     *       o {@code grace} deixa de ser interrompido no prazo dele:
     *       durante esta janela quem decide é só o {@code grace}.</li>
     * </ul>
     */
    @Override
    public void stop(Duration grace) {
        Objects.requireNonNull(grace, "grace");
        EngineState current = state.get();
        if (current == EngineState.STOPPED) {
            throw new IllegalStateException("already STOPPED");
        }
        DrainDeadline deadline = DrainDeadline.startingNow(grace);
        if (current != EngineState.DRAINING) {
            drain(grace);
        }
        state.set(EngineState.STOPPED);
        wake();
        joinLoopThread();
        awaitInFlight(deadline);
        writeFinalStoppedHeartbeat();
    }

    /**
     * Espera o tick em curso terminar até {@code nodeLeaseTtl/4} — o
     * próprio orçamento de lap do tick. Join completo é o que autoriza o
     * {@link #stop} a ler {@code inFlight} vazio como trabalho terminado:
     * sem loop, não há submit novo. Join estourado devolve o controle
     * assim mesmo — shutdown não fica pendurado em tick travado.
     */
    private void joinLoopThread() {
        Thread thread = loopThread;
        // NEW: start() publica a thread ANTES de iniciá-la, e stop() pode ter
        // vencido a largada (a corrida que o próprio start() trata). Nesse
        // estado join(Duration) lança IllegalThreadStateException — e não há
        // tick em curso pra esperar de qualquer forma: o runLoop testa
        // STOPPED na entrada do while e não chega a ticar.
        if (thread == null || thread.getState() == Thread.State.NEW) {
            return;
        }
        Duration budget = settings.nodeLeaseTtl().dividedBy(4);
        try {
            // join(Duration) em vez de join(millis): não tem a armadilha do
            // join(0) esperar para sempre (o que obrigava um max(1,...) com
            // TTL < 4ms) e devolve SE o loop parou — e é justamente o falso
            // que marca o modo degradado, onde a espera seguinte volta a
            // poder perder um dispatch ainda não registrado
            if (!thread.join(budget)) {
                log.warn("engine loop did not stop within {} — the in-flight wait below can still miss a dispatch "
                        + "the tick in progress had not registered yet; work may be re-delivered", budget);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * ADR-0041: o último heartbeat do shutdown gracioso grava STOPPED — sem
     * ele, stop limpo e crash ficam indistinguíveis no banco. Best-effort
     * de propósito: shutdown nunca falha por banco fora — quem morre sem
     * conseguir escrever fica coberto pela staleness (ADR-0012) e pelo
     * purge (ver {@link #purgeStaleNodeRows}).
     *
     * <p>Corrida aceita de propósito (review ADR-0041, custo revisto pela
     * ADR-0051): um tick que leu o estado antes do {@code state.set(STOPPED)}
     * pode commitar seu heartbeat DEPOIS deste — a linha fica
     * RUNNING/DRAINING com uma promessa fresca de +{@code node-lease-ttl},
     * e órfãos deste node esperam esse TTL extra pelo reaper (o mesmo custo
     * de um crash). Fechar isso exigiria o stop esperar o tick em curso,
     * acoplando a latência de shutdown a uma janela que staleness + purge
     * já cobrem.
     */
    private void writeFinalStoppedHeartbeat() {
        try {
            // ADR-0051: node parado não promete liveness — expires_at = now
            // (lease já vencida) diz aos pares "o que era meu está livre"
            Instant now = clock.instant();
            nodeStore.heartbeat(nodeId, EngineState.STOPPED, nodeEpoch, now, now);
        } catch (RuntimeException e) {
            log.warn("could not write the final STOPPED heartbeat for node {} — the row will read as its last "
                    + "ticked state until the stale purge collects it", nodeId, e);
        }
    }

    /**
     * ADR-0041: recolhe heartbeats mais velhos que
     * {@code lease-ttl × }{@link #STALE_NODE_RETENTION_LEASES} — de carona
     * no tick. Não é detecção de morte (essa é derivada na LEITURA): é só
     * recolher linhas que nenhum leitor tem mais uso, porque cada boot gera
     * {@code node_id} novo e a tabela cresceria para sempre.
     */
    private void purgeStaleNodeRows() {
        Instant cutoff = clock.instant().minus(settings.leaseTtl().multipliedBy(STALE_NODE_RETENTION_LEASES));
        int purged = nodeStore.deleteHeartbeatsBefore(cutoff);
        if (purged > 0) {
            log.info("purged {} stale node heartbeat row(s) — instances dead or restarted more than {} lease TTLs ago",
                    purged, STALE_NODE_RETENTION_LEASES);
        }
    }

    /**
     * O prazo de uma espera por in-flight: o instante monotônico em que ela
     * acaba e o {@code grace} que o operador pediu (só a mensagem de
     * {@link Engine#escalateAfterDrainGrace} usa o segundo). É um objeto
     * porque o {@link Engine#stop} espera duas vezes e as duas medem contra
     * o MESMO prazo — esperar duas vezes não pode cobrar duas vezes o
     * grace.
     */
    private record DrainDeadline(Duration requestedGrace, long deadlineNanos) {

        /**
         * {@code System.nanoTime}, não {@code clock.instant()}: duração se
         * mede com tempo monotônico — o Clock injetado pode ser o
         * DatabaseClock, cujo offset salta a cada resync e encurtaria ou
         * esticaria o grace de shutdown.
         */
        static DrainDeadline startingNow(Duration grace) {
            return new DrainDeadline(grace, System.nanoTime() + grace.toNanos());
        }

        long remainingMillis() {
            return (deadlineNanos - System.nanoTime()) / 1_000_000;
        }
    }

    /**
     * Espera {@code inFlight} esvaziar em laço, não num snapshot só — um
     * tick que já tinha passado do claim no instante exato do CAS pra
     * {@code DRAINING} ainda vai submeter as execuções que acabou de
     * reivindicar, e a volta do laço pega o que o snapshot anterior perdeu.
     * Para quando {@code inFlight} esvazia de verdade ou o prazo acaba, o
     * que vier primeiro.
     *
     * <p>O laço só enxerga o que JÁ foi registrado: com o conjunto vazio no
     * teste de entrada ele volta na hora, mesmo que um tick esteja
     * exatamente entre o {@code runAsync} e o {@code inFlight.add} do
     * {@link #submitDispatch}. Essa janela só fecha com o loop parado — é o
     * que o {@link #stop} faz, esperando de novo depois do join.
     */
    private void awaitInFlight(DrainDeadline deadline) {
        while (!inFlight.isEmpty()) {
            long remainingMillis = deadline.remainingMillis();
            if (remainingMillis <= 0) {
                escalateAfterDrainGrace(deadline);
                return;
            }
            CompletableFuture<?>[] snapshot = inFlight.toArray(CompletableFuture[]::new);
            try {
                CompletableFuture.allOf(snapshot).get(remainingMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                escalateAfterDrainGrace(deadline);
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
     * API-DESIGN (shutdown gracioso, passo 3) / ADR-0034: grace estourado
     * escala pela maquinaria de cancelamento — flag + interrupt em tudo que
     * sobrou; os attempts falham assincronamente com causa NodeShutdown e
     * seguem o retry normal pelo caminho de conclusão de sempre. Sem
     * segunda janela de espera configurável (YAGNI, ADR-0034); handler
     * surdo ao interrupt fica órfão com o fim dos ticks — o heartbeat para,
     * a lease do nó expira e o reaper de outro node reclama (ADR-0051).
     * Durante o grace nada disso acontece: drain ≠ cancel (ADR-0007).
     */
    private void escalateAfterDrainGrace(DrainDeadline deadline) {
        // o guard cerca só o LOG, nunca o laço: o sinal é first-wins por
        // attempt (CancellationSignal), mas um dispatch registrado DEPOIS da
        // primeira escalada — exatamente a janela runAsync/inFlight.add que a
        // segunda espera existe pra cobrir — nunca teria sido sinalizado se o
        // guard pulasse o laço junto
        if (drainEscalated.compareAndSet(false, true)) {
            // as duas contagens de propósito: inFlight (futures) é quem segura
            // o grace; o mapa é quem a escalada alcança — com a marca de posse
            // liberada (zumbi fica no mapa), divergem só na janela de conclusão
            // do whenComplete
            log.warn("drain grace period ({}) elapsed with {} dispatch(es) still in flight ({} still signallable) — "
                    + "signalling cancellation and interrupting them; their attempts will fail with a node-shutdown cause "
                    + "and follow the retry policy (ADR-0034)",
                    deadline.requestedGrace(), inFlight.size(), inFlightAttempts.size());
        } else {
            log.debug("drain grace already escalated — re-signalling {} in-flight attempt(s)", inFlightAttempts.size());
        }
        for (InFlightAttempt attempt : inFlightAttempts.values()) {
            attempt.signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
        }
    }

    /**
     * O loop do engine (§5.5/§12.1): tick, depois dormir — o intervalo
     * começa no piso ({@code pollInterval}), dobra a cada tick que não
     * achou trabalho até {@code maxPollInterval} e volta ao piso no
     * primeiro que achou. O sono é interrompível por {@link #wake} (tier 1
     * — enqueue local; e stop/resume) e SEMPRE limitado por
     * {@code nodeLeaseTtl/3}: o heartbeat roda uma vez por tick, então a
     * cadência do tick é a cadência da promessa de liveness — um teto de
     * backoff mal configurado não pode fazer o nó idle ser declarado morto
     * pelo reaper dos pares (risco nº 1 da Phase 6).
     */
    private void runLoop() {
        Duration delay = settings.pollInterval();
        while (state.get() != EngineState.STOPPED) {
            boolean workFound = tick();
            delay = workFound ? settings.pollInterval() : nextBackoff(delay);
            awaitWork(delay);
        }
    }

    private Duration nextBackoff(Duration current) {
        Duration doubled = current.multipliedBy(2);
        return doubled.compareTo(settings.maxPollInterval()) > 0 ? settings.maxPollInterval() : doubled;
    }

    private void awaitWork(Duration delay) {
        Duration heartbeatCadence = settings.nodeLeaseTtl().dividedBy(3);
        Duration bounded = delay.compareTo(heartbeatCadence) > 0 ? heartbeatCadence : delay;
        wakeLock.lock();
        try {
            if (!wakeRequested) {
                // um único await, não loop: acordar sem sinal (spurious/timeout)
                // só antecipa um tick — o predicado de verdade é a fila no banco
                wakeCondition.awaitNanos(bounded.toNanos());
            }
            wakeRequested = false;
        } catch (InterruptedException e) {
            // a Engine é DONA desta thread (JCIP 7.1.3): o protocolo de parada
            // é state + wake — interrupt não significa nada aqui. Re-armar a
            // flag viraria busy-spin (todo awaitNanos seguinte lançaria na
            // entrada). Engolir É a política de interrupção da dona, não um
            // catch vazio.
        } finally {
            wakeLock.unlock();
        }
    }

    /** Acorda o loop agora — de stop/resume e do tier 1 do wake-up (§5.5). Seguro de qualquer thread. */
    void wake() {
        wakeLock.lock();
        try {
            wakeRequested = true;
            wakeCondition.signal();
        } finally {
            wakeLock.unlock();
        }
    }

    /**
     * Tier 1 do wake-up (§5.5): um enqueue DESTA JVM já devido acorda o
     * loop sem esperar o poll — pós-commit quando o chamador está numa
     * transação (acordar antes do commit seria um lap que ainda não vê a
     * linha), imediatamente quando não está (a transação da unidade de
     * enqueue já commitou). Best-effort por contrato (ADR-G): sinal
     * perdido é coberto pelo tier 3, nunca por correção daqui.
     */
    public void signalWorkScheduled() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    wake();
                }
            });
        } else {
            wake();
        }
    }

    /**
     * Publica heartbeat sempre, refletindo o estado atual — mesmo fora de
     * {@code RUNNING} (ADR-0012: útil pra {@code GET /nodes} mostrar
     * "pausado"/"drenando"). Claim e reclaim só rodam em {@code RUNNING}.
     *
     * @return se este tick achou trabalho no banco (claim ou trigger) — o
     *         que zera o backoff do loop; sinais/manutenção não contam
     */
    private boolean tick() {
        try {
            EngineState current = state.get();
            Instant now = clock.instant();
            renewNodeLease(current, now);
            signalJobTimeouts();
            pollCancelRequests();
            signalWatchdogOverruns();
            if (current != EngineState.RUNNING) {
                return false;
            }
            Map<JobKey, StoredJob> definitions = loadDefinitions();
            // UMA leitura de mohs_nodes por tick serve o reaper (vivos) e a
            // atribuição de shards (elegíveis) — depois do heartbeat, então a
            // própria linha recém-renovada está na foto
            List<StoredNode> nodes = nodeStore.findAll();
            reapOrphanedLeases(definitions, aliveNodeIds(nodes, now), now);
            reconcileOwnStrayLeases(now);
            purgeStaleNodeRows();
            boolean fired = fireDueTriggers();
            int claimed = claimAndDispatch(definitions, Shards.ownedBy(nodeId, shardEligibleNodeIds(nodes, now)));
            return fired || claimed > 0;
        } catch (RuntimeException e) {
            log.error("engine tick failed — will retry next tick", e);
            // falha ≠ fila vazia, mas backoff é a resposta certa pras duas:
            // banco fora não melhora sendo martelado no piso de 25ms
            return false;
        }
    }

    /**
     * ADR-0051: perceber o PRÓPRIO lease de nó expirado ANTES de renová-lo
     * — este node esteve "morto" para o cluster (pausa/stall >
     * node-lease-ttl) e pares podem ter reclamado o que estava em voo. O
     * bump de epoch registra a reencarnação (§11.2 do redesign) e, desde a
     * Phase 5, derruba as escritas dos zumbis por si só: o fence é
     * {@code (node_id, epoch)}. O heartbeat sai em qualquer estado —
     * PAUSED/DRAINING incluídos (ADR-0007: drain ≠ cancel).
     */
    private void renewNodeLease(EngineState current, Instant now) {
        // !isBefore, não isAfter: o predicado do PAR (aliveNodeIds) considera o nó
        // morto já na igualdade — o auto-diagnóstico tem que ser simétrico, senão
        // no instante exato da expiração um par reaparia sem este nó bumpar o epoch
        if (nodeLeaseExpiresAt != null && !now.isBefore(nodeLeaseExpiresAt)) {
            nodeEpoch++;
            log.warn("node lease expired at {} while this node was stalled — epoch bumped to {}; peers may have "
                    + "reclaimed in-flight work (their re-runs stand; this node's fenced completions will be discarded)",
                    nodeLeaseExpiresAt, nodeEpoch);
        }
        Instant promised = now.plus(settings.nodeLeaseTtl());
        nodeStore.heartbeat(nodeId, current, nodeEpoch, now, promised);
        // só a promessa que o cluster REALMENTE viu conta pra detectar a
        // própria morte: se o heartbeat falha (catch do tick), o campo retém
        // a última promessa persistida e o bump dispara na primeira renovação
        // bem-sucedida depois de > TTL
        nodeLeaseExpiresAt = promised;
    }

    /**
     * O snapshot de definições do tick (DBTUNE-18 evoluída): uma varredura
     * da tabela fria de definições serve admissão (janela/cap/rate por
     * job), dispatch e reaper — no lugar dos N finds por job_key. Vira o
     * cache em memória com invalidação por versão quando o plano §7.2
     * chegar (registrado no PLAN.md); a staleness de um tick é a mesma da
     * memoização anterior.
     */
    private Map<JobKey, StoredJob> loadDefinitions() {
        Map<JobKey, StoredJob> byKey = new HashMap<>();
        try (var all = jobStore.findAll()) {
            all.forEach(stored -> byKey.put(stored.definition().key(), stored));
        }
        return byKey;
    }

    /**
     * O {@link StoredJob} do snapshot do tick, com consulta fresca no miss:
     * o snapshot pode PRECEDER um define+schedule do mesmo instante (o
     * claim vê a fila fresca, o snapshot não) — sem a consulta fresca, um
     * job recém-definido seria tratado como removido (bug real: ~50% de
     * falha no E2E do starter que define e agenda em sequência). Job
     * removido de verdade continua {@code null} — cada chamador decide o
     * desfecho. Custo só no miss: recém-nascido ou removido, nunca o hot
     * path.
     *
     * <p>O achado curado entra no snapshot (mapa mutável confinado à thread
     * do tick, JCIP 3.3): sem a memoização, a {@code Admission} das rodadas
     * seguintes do MESMO tick não incluiria o recém-nascido em
     * {@code capped} — {@code leaseCount} devolveria 0 e o mutex de
     * {@code maxConcurrentExecutions} viraria no-op até
     * {@code claimRounds × cap} execuções concorrentes num só nó (review
     * S5.4). Aposentado não é memoizado: {@code null} não é achado.
     */
    private @Nullable StoredJob storedJobFor(JobKey jobKey, Map<JobKey, StoredJob> definitions) {
        StoredJob stored = definitions.get(jobKey);
        if (stored != null) {
            return stored;
        }
        StoredJob fresh = jobStore.find(jobKey).orElse(null);
        if (fresh != null) {
            definitions.put(jobKey, fresh);
        }
        return fresh;
    }

    // ─── reaper (§4.3: recuperação e retry são o MESMO caminho) ─────────────

    /**
     * Reclama as leases de nós mortos — vivo é promessa não vencida
     * ({@code expires_at > now}) ou, em linha de jar antigo sem
     * {@code expires_at}, heartbeat mais novo que {@code lease-ttl}
     * (tolerância de versão mista, ADR-0051); ausente de {@code mohs_nodes}
     * é morto por definição. Cada lease órfã vira um
     * {@link LeaseStore.CompletionResult} cercado pela posse DO MORTO:
     * com orçamento → attempt sintético FAILED + renascimento na fila na
     * mesma transação; sem orçamento/retired → FAILED terminal; com
     * {@code cancel_requested} → CANCELLED (a ordem do operador vence o
     * orçamento, ADR-0034). O attempt sintético consome orçamento como
     * qualquer falha (ADR-0033) e o fence garante que uma encarnação nova
     * saudável nunca é morta (§6.3).
     */
    private void reapOrphanedLeases(Map<JobKey, StoredJob> definitions, List<String> aliveNodeIds, Instant now) {
        List<LeaseStore.Lease> orphans = leaseStore.findOrphaned(aliveNodeIds, RECLAIM_LIMIT);
        if (orphans.isEmpty()) {
            return;
        }
        Map<ExecutionId, HistoryStore.ExecutionHead> heads = new HashMap<>();
        historyStore.findHeads(orphans.stream().map(LeaseStore.Lease::executionId).toList())
                .forEach(head -> heads.put(head.executionId(), head));

        List<ReclaimDecision> decisions = orphans.stream()
                // miss do snapshot → consulta fresca (storedJobFor): job
                // recém-definido não é job aposentado
                .map(orphan -> decideReclaim(orphan, storedJobFor(orphan.jobKey(), definitions),
                        heads.get(orphan.executionId()), now))
                .toList();
        Map<ExecutionId, LeaseStore.Completion> verdicts =
                leaseStore.complete(decisions.stream().map(ReclaimDecision::result).toList(), jobStore);
        for (ReclaimDecision decision : decisions) {
            LeaseStore.Completion verdict = verdicts.get(decision.result().executionId());
            if (verdict != null && verdict.owned()) {
                metrics.leaseReclaimed(decision.reclaimedState(), decision.attemptsExhausted());
                publishReclaimOutcome(decision);
            }
        }
    }

    /**
     * O sucessor da expiração por execução que o split aposentou: uma lease
     * DESTE node sem encarnação em {@code inFlightAttempts} é trabalho
     * perdido entre claim e dispatch (falha da consulta de payloads,
     * rejeição do executor, gravação de falha terminal que lançou) — e o
     * reaper nunca a alcançaria, porque este node está vivo. Sem este
     * passe, o único remédio seria reiniciar o nó (violação de liveness —
     * DDIA cap. 8: toda posse precisa de um caminho de expiração
     * alcançável). Devolve pra fila com o MESMO attempt (nada rodou,
     * orçamento intacto), pelo requeue cercado de sempre.
     *
     * <p>Duas guardas contra falso positivo, porque o normal a alta vazão é
     * a lease de uma conclusão EM TRÂNSITO no {@link CompletionBatcher} (a
     * entrada sai do mapa quando o dispatch retorna; a lease só cai no
     * commit do flush): (1) grace por {@code claimed_at} —
     * {@link #strayLeaseGrace()} — porque a 10k+/s SEMPRE há dezenas de
     * leases em trânsito e o esquema de rodadas sozinho as pegava aos
     * milhares (medido no bench do S5.5: 199k WARNs, requeues fantasma
     * disputando lock com o flush até deadlock); (2) duas rodadas
     * consecutivas ausente. Órfã legítima tem {@code claimed_at} parado —
     * a recuperação custa o grace + 2 rodadas: ~2s no ponto de operação do
     * bench (poll 20ms), ~30s no poll default de 5s — a era por-execução
     * também recuperava em lease-ttl de 30s, então nunca pior, e ordens de
     * magnitude melhor com poll curto. Se mesmo assim uma conclusão perder
     * a corrida, o fence do requeue decide e o pior caso é uma re-execução
     * (at-least-once), nunca corrupção. Estado confinado à thread do tick
     * (JCIP 3.3).
     */
    private void reconcileOwnStrayLeases(Instant now) {
        Instant strayBefore = now.minus(strayLeaseGrace());
        Set<ExecutionId> absentNow = new HashSet<>();
        List<WorkQueue.Requeue> strays = new ArrayList<>();
        for (LeaseStore.Lease lease : leaseStore.findByNodes(List.of(nodeId))) {
            // três guardas, na ordem estado > estado > tempo: encarnação em
            // voo; conclusão em trânsito no batcher (cobre job que roda MAIS
            // que o grace — review S5.5); e o grace por claimed_at, que fica
            // pro único caso sem estado consultável (perda entre claim e
            // dispatch)
            if (inFlightAttempts.containsKey(lease.executionId())
                    || dispatcher.completionInTransit(lease.executionId())
                    || lease.claimedAt().isAfter(strayBefore)) {
                continue;
            }
            absentNow.add(lease.executionId());
            if (strayLeaseCandidates.contains(lease.executionId())) {
                strays.add(new WorkQueue.Requeue(lease.executionId(), nodeId, lease.epoch(),
                        new WorkQueue.ReadyEntry(lease.executionId(), lease.jobKey(), Shards.of(lease.executionId()),
                                lease.priority(), lease.attemptNumber(), now)));
            }
        }
        strayLeaseCandidates.clear();
        strayLeaseCandidates.addAll(absentNow);
        if (!strays.isEmpty()) {
            int requeued = workQueue.requeue(strays);
            if (requeued > 0) {
                metrics.claimRequeued("stray-lease", requeued);
                log.warn("requeued {} lease(s) this node was holding with no in-flight incarnation — "
                        + "work lost between claim and dispatch (same attempt, retry budget untouched)", requeued);
            } else {
                // fence perdeu pra todas: as conclusões commitaram entre o
                // findByNodes e o requeue — rotina a alta vazão, não achado
                log.debug("stray-lease candidates all vanished before the requeue — completions won the race");
            }
        }
    }

    /**
     * Idade mínima pra uma lease sem encarnação virar candidata:
     * {@code max(2s, 4×poll)} — acima do pior trânsito plausível pelo
     * batcher. O piso de 2s não é teórico: no cold start do bench do S5.5
     * (JIT/warmup), o flusher atrasou além de 500ms e o reconcile requeueou
     * LOTES inteiros de conclusões em trânsito (blocos de 256/512, 10,7k
     * fences perdidos num round frio); com 2s o regime frio fica limpo. O
     * custo (recuperação de órfã legítima esperar o grace) depende do poll:
     * ~2s a 20ms, ~20s no default de 5s — nunca pior que o lease-ttl de 30s
     * da era por-execução. Deriva do poll-interval — nenhum knob novo.
     */
    private Duration strayLeaseGrace() {
        Duration fourPolls = settings.pollInterval().multipliedBy(4);
        Duration floor = Duration.ofSeconds(2);
        return fourPolls.compareTo(floor) > 0 ? fourPolls : floor;
    }

    /** Vivos deste instante — inclui SEMPRE este node (a promessa dele acabou de ser renovada neste mesmo tick, antes do reaper: a ordem que mata o self-reap do S8). */
    private List<String> aliveNodeIds(List<StoredNode> nodes, Instant now) {
        return nodeIdsMatching(nodes, node -> isAlive(node, now));
    }

    private boolean isAlive(StoredNode node, Instant now) {
        Instant expiresAt = node.expiresAt();
        // linha de jar antigo sem expires_at: staleness de lease-ttl (tolerância de versão mista, ADR-0051)
        return expiresAt != null ? expiresAt.isAfter(now) : node.lastHeartbeatAt().isAfter(now.minus(settings.leaseTtl()));
    }

    /**
     * Quem participa da atribuição de shards (§8.3/§11.2): vivo E
     * {@code RUNNING} — {@code PAUSED}/{@code DRAINING}/{@code STOPPED}
     * não reivindicam, então dar-lhes shards deixaria 1/n da fila parada
     * enquanto os pares têm folga (o plano só nomeia DRAINING; PAUSED sai
     * pela mesma razão — decisão registrada no PLAN.md da fase). Este node
     * sempre entra quando chega ao claim: o heartbeat do próprio tick
     * acabou de gravar RUNNING. Se a lista sair vazia ou sem este node,
     * {@link Shards#ownedBy} degrada pra "possui todos" — sobreposição é o
     * comportamento pré-shard, fila parada não é opção.
     */
    private List<String> shardEligibleNodeIds(List<StoredNode> nodes, Instant now) {
        return nodeIdsMatching(nodes, node -> node.state() == EngineState.RUNNING && isAlive(node, now));
    }

    /** Os ids que passam no predicado, SEMPRE incluindo este node — o invariante comum de reaper e atribuição de shards (ver os chamadores). */
    private List<String> nodeIdsMatching(List<StoredNode> nodes, Predicate<StoredNode> included) {
        List<String> ids = new ArrayList<>();
        for (StoredNode node : nodes) {
            if (included.test(node)) {
                ids.add(node.nodeId());
            }
        }
        if (!ids.contains(nodeId)) {
            ids.add(nodeId);
        }
        return ids;
    }

    /** Um desfecho de reclaim decidido — o resultado cercado + o que publicar/medir se o fence vencer. */
    private record ReclaimDecision(LeaseStore.CompletionResult result, ExecutionState reclaimedState,
            boolean attemptsExhausted, @Nullable Instant retryAt) {
    }

    private ReclaimDecision decideReclaim(LeaseStore.Lease orphan, @Nullable StoredJob storedJob,
            HistoryStore.@Nullable ExecutionHead head, Instant now) {
        ExecutionId id = orphan.executionId();
        JobKey jobKey = orphan.jobKey();
        String batchId = head == null ? null : head.correlationId();
        Instant createdAt = head == null ? null : head.createdAt();
        boolean retired = storedJob == null;
        JobDefinition definition = storedJob == null ? null : storedJob.definition();

        if (orphan.cancelRequested()) {
            // o nó morreu, mas a ordem do operador já estava dada — reagendar
            // seria desobedecê-la (ADR-0034); attempt CANCELLED com error nulo
            return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.epoch(),
                    orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.CANCELLED, null, null,
                    ExecutionState.CANCELLED, createdAt, null, batchId, rearmFor(head, definition, retired, now)),
                    ExecutionState.CANCELLED, false, null);
        }
        Optional<Instant> nextRetry = retired
                ? Optional.empty() // job aposentado nunca reagenda: a entrada ficaria presa pra sempre (claim... a fila não filtra retired — a aposentadoria drena a fila, S5.4)
                : RetrySchedule.nextRetryAt(orphan.attemptNumber(), Objects.requireNonNull(definition).retries(), now);
        if (nextRetry.isPresent()) {
            Instant retryAt = nextRetry.orElseThrow();
            return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.epoch(),
                    orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.FAILED,
                    IllegalStateException.class.getName(), NODE_DEAD_ERROR,
                    null, createdAt,
                    new WorkQueue.ReadyEntry(id, jobKey, Shards.of(id), orphan.priority(), orphan.attemptNumber() + 1, retryAt),
                    batchId, null),
                    ExecutionState.RETRY_WAITING, false, retryAt);
        }
        return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.epoch(),
                orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.FAILED,
                IllegalStateException.class.getName(), NODE_DEAD_ERROR,
                ExecutionState.FAILED, createdAt, null, batchId, rearmFor(head, definition, retired, now)),
                ExecutionState.FAILED, !retired, null);
    }

    /**
     * ADR-0035: reclaim terminal de ocorrência do scheduler em job
     * fixed-delay rearma a corrente — o "fim" de um zumbi é desconhecido,
     * {@code now} (a observação do reaper) ancora. Execução manual não é a
     * corrente; job aposentado nunca rearma.
     */
    private static @Nullable Instant rearmFor(HistoryStore.@Nullable ExecutionHead head,
            @Nullable JobDefinition definition, boolean retired, Instant now) {
        return head != null && !retired && Execution.SCHEDULER_ACTOR.equals(head.actor())
                && definition != null && definition.schedule() instanceof IntervalSpec interval && interval.afterFinish()
                ? now.plus(interval.interval())
                : null;
    }

    /**
     * Desfechos do reclaim publicam os mesmos eventos do caminho de
     * dispatch, espelhando exatamente os pares do {@link Dispatcher}:
     * retry → {@code AttemptFailed} + {@code RetryScheduled}; cancel
     * pendente honrado (ADR-0034) → {@code Cancelled}; terminal → só
     * {@code Failed}. É o gancho de alerta de morte de nó que o Javadoc de
     * {@link Failed} anuncia.
     */
    private void publishReclaimOutcome(ReclaimDecision decision) {
        LeaseStore.CompletionResult result = decision.result();
        ExecutionEventPublisher events = dispatcher.events();
        int attemptNumber = result.attemptNumber();
        switch (decision.reclaimedState()) {
            case CANCELLED -> events.publish(new Cancelled(result.executionId(), result.jobKey(), attemptNumber));
            case RETRY_WAITING -> {
                Exception error = new IllegalStateException(NODE_DEAD_ERROR);
                events.publish(new AttemptFailed(result.executionId(), result.jobKey(), attemptNumber, error));
                events.publish(new RetryScheduled(result.executionId(), result.jobKey(), attemptNumber + 1,
                        Objects.requireNonNull(decision.retryAt())));
            }
            default -> events.publish(new Failed(result.executionId(), result.jobKey(), attemptNumber,
                    new IllegalStateException(NODE_DEAD_ERROR), decision.attemptsExhausted()));
        }
    }

    // ─── aquisição → dispatch (§5.4) ────────────────────────────────────────

    /**
     * A etapa "aquisição → dispatch" do tick, com o gate ocioso do S6.5 na
     * frente: enquanto a rodada anterior voltou vazia, uma sonda de
     * existência responde pelo lap inteiro — 1 statement no lugar de
     * {@link Shards#SHARD_COUNT}, que media 96% do custo de consulta de um
     * nó parado (BASELINE "Phase 6 — S6.4"). Quando a sonda acha trabalho,
     * o lap roda NESTE mesmo tick: economia, nunca latência.
     */
    private int claimAndDispatch(Map<JobKey, StoredJob> definitions, List<Integer> ownedShards) {
        if (queueLooksEmpty && !probeSaysThereIsWork(ownedShards)) {
            return 0;
        }
        LapOutcome outcome = claimLaps(definitions, ownedShards);
        // dispatch saturado e orçamento estourado devolvem zero SEM a fila estar
        // vazia: armar neles faria o gate pagar uma sonda por tick no caminho
        // quente, que é o oposto do que ele existe pra fazer
        queueLooksEmpty = outcome.claimed() == 0 && outcome.sweptEveryOwnedShard();
        return outcome.claimed();
    }

    /**
     * A sonda do gate, à prova de si mesma: ela é economia, nunca
     * autoridade — sonda que falha devolve o tick ao lap. Sem isto, uma
     * falha PERSISTENTE dela deixaria este node vivo, heartbeatando e dono
     * de 1/n dos shards, sem nunca reivindicar nada — ninguém o reaparia,
     * porque ele não está morto.
     */
    private boolean probeSaysThereIsWork(List<Integer> ownedShards) {
        try {
            return workQueue.hasVisibleWork(ownedShards, clock.instant());
        } catch (RuntimeException e) {
            log.warn("idle-gate probe failed — falling back to the full claim lap this tick", e);
            return true;
        }
    }

    /**
     * O resultado de uma rodada de claim: quanto veio e se a volta pelos
     * shards próprios foi COMPLETA. Só uma volta completa e vazia é prova
     * de fila vazia — as saídas antecipadas (folga de dispatch esgotada,
     * orçamento de tempo, saída de {@code RUNNING}) devolvem zero por
     * outros motivos.
     *
     * <p>A saída por esgotar {@code claimRounds} devolve {@code false} por
     * conservadorismo, e é inobservável: chegar lá exige todo lap ter
     * enchido o orçamento, o que garante {@code claimed > 0}. Não
     * "uniformize" pra {@code true} — o valor não muda nada e a regra
     * "só arma com prova" para de ser legível.
     */
    private record LapOutcome(int claimed, boolean sweptEveryOwnedShard) {
    }

    /**
     * Os laps de claim propriamente ditos. ADR-0039: cada claim é limitado
     * pela folga de dispatch — node saturado não reivindica; o excedente
     * fica na fila, reivindicável por qualquer node com folga. ADR-0040:
     * sob backlog, um tick encadeia até {@code claimRounds} claims, com
     * dois guards entre rounds: drain/pause interrompe o encadeamento, e
     * um orçamento monotônico de {@code nodeLeaseTtl/4} limita a duração
     * total — o heartbeat roda UMA vez por tick, antes daqui, e rounds que
     * se aproximassem do TTL deixariam a lease do NÓ vencer no meio do
     * tick (lease é detecção de falha — DDIA).
     *
     * <p>§5.4: a lista de inadmissíveis é recomputada por round (janela
     * fechada, cap sem folga — derivado de {@code mohs_lease}, §5.7 —,
     * rate limit sem saldo), e a sobra de admissão pós-claim volta pra
     * fila SEM consumir orçamento ({@link #admit}).
     */
    private LapOutcome claimLaps(Map<JobKey, StoredJob> definitions, List<Integer> ownedShards) {
        long lapsBudgetNanos = settings.nodeLeaseTtl().toNanos() / 4;
        long startNanos = System.nanoTime();
        int totalClaimed = 0;
        for (int lap = 0; lap < settings.claimRounds(); lap++) {
            if (lap > 0 && mustStopClaiming(startNanos, lapsBudgetNanos)) {
                return new LapOutcome(totalClaimed, false);
            }
            int lapBudget = Math.min(settings.batchSize(), settings.dispatchConcurrency() - inFlight.size());
            if (lapBudget <= 0) {
                return new LapOutcome(totalClaimed, false);
            }
            Instant now = clock.instant();
            // admissão UMA vez por lap, nunca por statement (§5.7): o bound de
            // sobre-admissão continua "nós × 1 lap", mesmo sentido de erro
            Admission admission = Admission.compute(definitions, windowRegistry, rateLimitStore, leaseStore, now);
            // o filtro no SQL é otimização de churn, não correção — o admit() pós-claim
            // é a autoridade. Acima do teto de parâmetros de IN (SQL Server quebra
            // ~2100), claim sem filtro e o admit devolve os inadmissíveis (mesmo
            // attempt, orçamento intacto) em vez de TODA rodada falhar no driver
            Collection<JobKey> inadmissibleFilter = admission.inadmissible().size() > MAX_INADMISSIBLE_FILTER
                    ? List.of()
                    : admission.inadmissible();
            // o LAP (§5.4/E2): um shard PRÓPRIO por statement, round-robin com
            // cursor persistente entre ticks — multi-shard no predicado mataria
            // a ordenação do índice (lição do E2); volta completa vazia encerra
            int remaining = lapBudget;
            for (int probe = 0; probe < ownedShards.size() && remaining > 0; probe++) {
                // o mesmo guard da fronteira de lap, POR SONDA: um lap são até 64
                // statements, e um orçamento de tempo só protege na granularidade
                // em que é checado — banco degradado a 300ms/claim estouraria o
                // lease do NÓ no meio do tick (review S6.1); a primeira sonda do
                // tick sempre roda
                if ((lap > 0 || probe > 0) && mustStopClaiming(startNanos, lapsBudgetNanos)) {
                    return new LapOutcome(totalClaimed, false);
                }
                int shard = ownedShards.get(Math.floorMod(shardCursor++, ownedShards.size()));
                long claimStartNanos = System.nanoTime();
                List<WorkQueue.ClaimedWork> claimed =
                        workQueue.claim(shard, nodeId, nodeEpoch, remaining, inadmissibleFilter, now);
                metrics.claimRound(System.nanoTime() - claimStartNanos, claimed.size());
                if (claimed.isEmpty()) {
                    continue;
                }
                // o reivindicado desconta MESMO quando o admit devolve losers:
                // deliberado — simplicidade > utilização (o churn de admissão é
                // raro por construção e autolimitado a um lap)
                remaining -= claimed.size();
                totalClaimed += claimed.size();
                List<WorkQueue.ClaimedWork> admitted = admit(claimed, definitions, admission, now);
                if (!admitted.isEmpty() && !dispatchClaimedBatch(admitted, definitions, now)) {
                    return new LapOutcome(totalClaimed, false);
                }
            }
            if (remaining > 0) {
                // lap não encheu o orçamento — fila drenada o bastante; um lap a
                // mais seria uma volta de SELECTs em filas já vazias (ADR-0040).
                // Chegar aqui exige o loop interno ter esgotado os shards
                // próprios: a volta foi COMPLETA, e zero aqui É fila vazia
                return new LapOutcome(totalClaimed, true);
            }
        }
        return new LapOutcome(totalClaimed, false);
    }

    /**
     * O guard entre claims do mesmo tick (ADR-0040): drain/pause interrompe
     * o encadeamento, e o orçamento monotônico de {@code nodeLeaseTtl/4}
     * idem — o porquê de cada ponto de checagem (fronteira de lap e por
     * sonda) está nos chamadores.
     */
    private boolean mustStopClaiming(long startNanos, long lapsBudgetNanos) {
        return state.get() != EngineState.RUNNING || System.nanoTime() - startNanos >= lapsBudgetNanos;
    }

    /**
     * A resolução da corrida de admissão (§5.4, "Admission races, named"):
     * a lista de inadmissíveis é um snapshot do início da rodada — um cap
     * pode ter sido parcial (folga 2, rodada trouxe 5) e o rate limit é
     * cobrado AGORA, tudo-ou-nada por job ({@code RateLimitStore.charge},
     * ADR-0042: cobrar o reivindicado, nunca entregar sem token — a única
     * violação inaceitável). Perdedor volta pra fila com o MESMO attempt
     * (nada rodou, orçamento intacto) e {@code visible_at = now} — a
     * rodada seguinte deste ou de outro node decide; o churn é limitado a
     * uma rodada por virada de guard e contado
     * ({@code mohs.claim.requeued}).
     */
    private List<WorkQueue.ClaimedWork> admit(List<WorkQueue.ClaimedWork> claimed, Map<JobKey, StoredJob> definitions,
            Admission admission, Instant now) {
        Map<JobKey, List<WorkQueue.ClaimedWork>> byJob = new LinkedHashMap<>();
        for (WorkQueue.ClaimedWork work : claimed) {
            byJob.computeIfAbsent(work.jobKey(), _ -> new ArrayList<>()).add(work);
        }
        List<WorkQueue.ClaimedWork> admitted = new ArrayList<>(claimed.size());
        List<WorkQueue.Requeue> losers = new ArrayList<>();
        for (Map.Entry<JobKey, List<WorkQueue.ClaimedWork>> entry : byJob.entrySet()) {
            JobKey jobKey = entry.getKey();
            List<WorkQueue.ClaimedWork> ofJob = entry.getValue();
            // consulta fresca no miss (storedJobFor): o recém-nascido passa
            // pelos MESMOS guards — janela inclusive; sem ela, ele rodaria
            // dentro de janela fechada, o bug que o E2E do starter pegou.
            // Removido de verdade segue pro dispatch falhar com a mensagem certa
            StoredJob stored = storedJobFor(jobKey, definitions);
            Admitted share = stored == null
                    ? Admitted.all(ofJob.size())
                    : admitFor(stored.definition(), ofJob.size(), admission, now);
            admitted.addAll(ofJob.subList(0, share.count()));
            admission.consume(jobKey, share.count());
            for (WorkQueue.ClaimedWork loser : ofJob.subList(share.count(), ofJob.size())) {
                losers.add(new WorkQueue.Requeue(loser.executionId(), nodeId, nodeEpoch,
                        new WorkQueue.ReadyEntry(loser.executionId(), jobKey, Shards.of(loser.executionId()),
                                loser.priority(), loser.attemptNumber(), now)));
            }
            if (!share.reason().isEmpty()) {
                metrics.claimRequeued(share.reason(), ofJob.size() - share.count());
            }
        }
        if (!losers.isEmpty()) {
            workQueue.requeue(losers);
        }
        return admitted;
    }

    /** Quanto da rodada um job levou e — quando nem tudo — qual guard barrou (o label de {@code mohs.claim.requeued}). */
    private record Admitted(int count, String reason) {

        static Admitted all(int requested) {
            return new Admitted(requested, "");
        }
    }

    /**
     * Os guards pós-claim do §5.4, na ordem: janela, folga de cap (derivada
     * de {@code mohs_lease}) e cobrança tudo-ou-nada do rate limit — quem
     * reduziu por último assina o {@code reason}. A janela aqui é
     * segunda linha de defesa, não redundância: o filtro pré-claim é um
     * snapshot (e é DESCARTADO acima de {@link #MAX_INADMISSIBLE_FILTER}) —
     * job recém-nascido ou rodada sem filtro chegam aqui como a única
     * barreira entre a fila e uma janela fechada.
     */
    private Admitted admitFor(JobDefinition definition, int requested, Admission admission, Instant now) {
        if (windowRegistry.excludes(definition.window(), now)) {
            return new Admitted(0, "window-closed");
        }
        int allowed = requested;
        String reason = "";
        if (!definition.allowConcurrentExecutions()) {
            int headroom = Math.max(0, definition.maxConcurrentExecutions() - admission.leaseCount(definition.key()));
            if (headroom < allowed) {
                allowed = headroom;
                reason = "concurrency-cap";
            }
        }
        String rateLimit = definition.rateLimit();
        if (rateLimit != null && allowed > 0) {
            int granted = Math.min(allowed, rateLimitStore.available(rateLimit, now));
            if (granted > 0 && !rateLimitStore.charge(rateLimit, granted, now)) {
                granted = 0; // outro nó levou o saldo entre as duas fases — a rodada devolve tudo deste job
            }
            if (granted < allowed) {
                allowed = granted;
                reason = "rate-limit";
            }
        }
        return new Admitted(allowed, reason);
    }

    /**
     * Os guards por job do lap (§5.4): quem está fora deste lap e as
     * contagens de posse que decidem folga de cap. Classe, não record: o
     * lap MUTA as contagens ({@link #consume}) e record comunica valor
     * imutável (Effective Java 17 — a convenção do projeto reserva record
     * pra value object). Confinado à thread do tick (JCIP 3.3).
     */
    private static final class Admission {

        private final Set<JobKey> inadmissible;
        private final Map<JobKey, Integer> leaseCounts;

        private Admission(Set<JobKey> inadmissible, Map<JobKey, Integer> leaseCounts) {
            this.inadmissible = inadmissible;
            this.leaseCounts = leaseCounts;
        }

        Set<JobKey> inadmissible() {
            return inadmissible;
        }

        int leaseCount(JobKey jobKey) {
            return leaseCounts.getOrDefault(jobKey, 0);
        }

        /**
         * Desconta do headroom o que este lap acabou de admitir — o
         * snapshot é UM por lap (§5.7), mas o lap tem N statements: sem o
         * desconto, cada sonda re-concederia o mesmo headroom e um cap de 1
         * admitiria 1 por shard (o perdedor devolvido com
         * {@code visible_at = now} num shard à frente do cursor seria
         * re-admitido na MESMA volta). Com ele, o bound de sobre-admissão
         * segue "nós × 1 lap", o que o plano prometeu.
         */
        void consume(JobKey jobKey, int count) {
            if (count > 0) {
                leaseCounts.merge(jobKey, count, Integer::sum);
            }
        }

        static Admission compute(Map<JobKey, StoredJob> definitions, ExecutionWindowRegistry windowRegistry,
                RateLimitStore rateLimitStore, LeaseStore leaseStore, Instant now) {
            Set<JobKey> inadmissible = new HashSet<>();
            List<JobKey> capped = new ArrayList<>();
            for (StoredJob stored : definitions.values()) {
                JobDefinition definition = stored.definition();
                if (windowRegistry.excludes(definition.window(), now)) {
                    inadmissible.add(definition.key());
                    continue;
                }
                if (!definition.allowConcurrentExecutions()) {
                    capped.add(definition.key());
                }
                String rateLimit = definition.rateLimit();
                if (rateLimit != null && rateLimitStore.available(rateLimit, now) <= 0) {
                    inadmissible.add(definition.key());
                }
            }
            // cópia mutável: o lap desconta o que admite ({@link #consume})
            Map<JobKey, Integer> leaseCounts = new HashMap<>(capped.isEmpty() ? Map.of() : leaseStore.countByJob(capped));
            for (JobKey jobKey : capped) {
                StoredJob stored = definitions.get(jobKey);
                if (stored != null
                        && leaseCounts.getOrDefault(jobKey, 0) >= stored.definition().maxConcurrentExecutions()) {
                    inadmissible.add(jobKey);
                }
            }
            return new Admission(inadmissible, leaseCounts);
        }
    }

    /**
     * ADR-0047: UMA consulta de payload+cabeçalho por round, não uma por
     * execução. Falha da consulta é infra, nunca veredito sobre as
     * execuções: o lote fica com a lease de pé e o reaper de outro node o
     * devolve se este morrer — o soluço nunca vira falha TERMINAL imediata
     * (o achado do S8).
     *
     * @return {@code false} se a consulta falhou — o chamador encerra os
     *         rounds do tick (o lote já reivindicado fica pro reaper)
     */
    private boolean dispatchClaimedBatch(List<WorkQueue.ClaimedWork> claimed, Map<JobKey, StoredJob> definitions,
            Instant claimInstant) {
        HistoryStore.PayloadBatch payloads;
        try {
            payloads = historyStore.findPayloads(claimed.stream().map(WorkQueue.ClaimedWork::executionId).toList());
        } catch (RuntimeException e) {
            log.warn("could not load the payloads of the claimed batch ({} execution(s)) — already leased, "
                    + "they will stand until a reaper reclaims them if this node dies", claimed.size(), e);
            return false;
        }
        for (WorkQueue.ClaimedWork work : claimed) {
            submitDispatch(work, payloads, definitions, claimInstant);
        }
        return true;
    }

    /**
     * Fire-and-forget em relação ao próximo tick: um handler lento não trava
     * a próxima rodada de claim. O executor do runner pode rejeitar
     * ({@link MohsExecutors#ioBoundExecutor} rejeita acima do teto de
     * concorrência, de propósito — backpressure real) — sem capturar aqui,
     * uma única rejeição no meio do lote abortaria o {@code for} e deixaria
     * as execuções seguintes já possuídas órfãs até um reaper.
     */
    private void submitDispatch(WorkQueue.ClaimedWork work, HistoryStore.PayloadBatch payloads,
            Map<JobKey, StoredJob> definitions, Instant claimInstant) {
        ExecutionId id = work.executionId();
        HistoryStore.PayloadRow row = payloads.rows().get(id);
        Dispatcher.Grant grant = new Dispatcher.Grant(nodeId, nodeEpoch, work.attemptNumber(), claimInstant,
                row == null ? null : row.head().createdAt());
        // consulta fresca no miss (storedJobFor) — sem ela, um job
        // recém-nascido morreria FAILED terminal como se tivesse sido removido
        StoredJob storedJob = storedJobFor(work.jobKey(), definitions);
        if (storedJob == null) {
            failBeforeDispatchGuarded(executionFor(work, row, claimInstant), null, new IllegalStateException(
                    "job definition for " + work.jobKey() + " was removed after this execution was claimed (e.g. Mohs.remove between claim and dispatch)"), grant);
            return;
        }
        JobDefinition definition = storedJob.definition();

        AsyncTaskExecutor executor;
        try {
            executor = runnerRegistry.resolve(definition.runner());
        } catch (NoSuchElementException e) {
            failBeforeDispatchGuarded(executionFor(work, row, claimInstant), definition, new IllegalStateException(
                    "runner could not be resolved: " + Objects.requireNonNullElse(e.getMessage(), e.toString()), e), grant);
            return;
        }

        // ADR-0047: o payload chegou na leitura em lote do round. Linha que
        // não desserializou é terminal por natureza (payload corrompido não
        // sara re-lendo); a falha TRANSIENTE da consulta nunca chega aqui
        // (tratada no round, lote inteiro fica pro reaper).
        RuntimeException unreadable = payloads.unreadable().get(id);
        if (unreadable != null) {
            failUnreadablePayload(executionFor(work, row, claimInstant), definition, unreadable, grant);
            return;
        }
        if (row == null) {
            failUnreadablePayload(executionFor(work, null, claimInstant), definition, new IllegalStateException(
                    "execution " + id + " has a lease but no history row — the enqueue unit was broken (§7.5-1 violated upstream)"), grant);
            return;
        }
        Execution execution = executionFor(work, row, claimInstant);

        // registrado ANTES do runAsync: a posse existe desde o claim — a
        // varredura do tick (timeout/cancel/watchdog) precisa alcançá-la
        // mesmo que o executor a deixe na fila por um tick inteiro. Remoções
        // sempre com o remove de dois argumentos: o mesmo ExecutionId pode
        // ser re-reivindicado por este node após um requeue, e o whenComplete
        // tardio de um zumbi não pode apagar a entrada da encarnação nova
        // (ABA em memória — InFlightAttempt tem igualdade de identidade).
        InFlightAttempt attempt = new InFlightAttempt(definition.timeout(), execution, definition, grant);
        inFlightAttempts.put(id, attempt);
        CompletableFuture<Void> future;
        try {
            future = CompletableFuture.runAsync(() -> dispatcher.dispatch(execution, definition, Objects.requireNonNull(row).payload(), attempt.signal, grant), executor);
        } catch (RuntimeException e) {
            inFlightAttempts.remove(id, attempt);
            log.warn("runner executor rejected execution {} — already leased, it will stand until a reaper reclaims it",
                    id, e);
            return;
        }
        inFlight.add(future);
        future.whenComplete((_, thrown) -> {
            // Dispatcher.dispatch trata as falhas conhecidas — throwable aqui
            // significa que a própria gravação do desfecho lançou; engolir
            // esconderia o único rastro do zumbi
            if (thrown != null) {
                log.error("dispatch of execution {} threw outside the normal failure paths", id, thrown);
            }
            inFlightAttempts.remove(id, attempt);
            inFlight.remove(future);
        });
    }

    /** A visão pública da encarnação em voo — construída do cabeçalho da história; sem attempts (§5.3: nada conta attempts no hot path). */
    private Execution executionFor(WorkQueue.ClaimedWork work, HistoryStore.@Nullable PayloadRow row, Instant claimInstant) {
        if (row == null) {
            // linha de história sumida/ilegível: o mínimo verdadeiro pros
            // eventos — identidade, estado e a posse; o resto é desconhecido
            return new Execution(work.executionId(), work.jobKey(), ExecutionState.RUNNING, claimInstant, claimInstant,
                    List.of(), "unknown", Priority.NORMAL, null, null, nodeId);
        }
        HistoryStore.ExecutionHead head = row.head();
        return new Execution(work.executionId(), work.jobKey(), ExecutionState.RUNNING, head.scheduledAt(), claimInstant,
                List.of(), head.actor(), Priority.fromValue(head.priority()), null, head.correlationId(), nodeId);
    }

    // ─── triggers (ADR-0035) ────────────────────────────────────────────────

    /**
     * ADR-0035: a etapa "trigger devido → aquisição" — ANTES do claim, no
     * mesmo tick, pra ocorrência recém-materializada já ser reivindicável
     * sem esperar o próximo poll. {@link FiringPlanner} decide o que a
     * política de misfire do job dispara; {@link TriggerFirer#fire} é o
     * CAS transacional que resolve a corrida entre nós — perder é rotina,
     * não erro. Falha de um job não derruba a varredura dos demais.
     */
    private boolean fireDueTriggers() {
        Instant now = clock.instant();
        List<StoredJob> due = jobStore.findDueRecurring(now, FIRE_LIMIT);
        for (StoredJob job : due) {
            JobDefinition definition = job.definition();
            try {
                // dentro do try: contrato violado por um store custom falha SÓ este job, não a varredura nem o claim do tick
                Instant observed = Objects.requireNonNull(job.nextFireAt(), "findDueRecurring returned an unarmed trigger");
                fireTrigger(definition, observed, now);
            } catch (RuntimeException e) {
                log.error("firing job '{}' failed — will retry next tick", definition.key().value(), e);
            }
        }
        // trigger devido = trabalho no banco, MESMO perdendo o CAS (outro nó
        // materializou ocorrências que podem cair nos shards deste) — zera o
        // backoff pelo lado conservador
        return !due.isEmpty();
    }

    /** Um trigger devido: o plano da política de misfire vira ocorrências materializadas pelo CAS — perder o CAS sai calado (ver {@link #fireDueTriggers}). */
    private void fireTrigger(JobDefinition definition, Instant observed, Instant now) {
        FiringPlanner.Plan plan = firingPlanner.plan(definition.schedule(), definition.misfire(), observed, now);
        List<Execution> occurrences = plan.occurrences().stream()
                .map(occurrenceAt -> newOccurrence(definition.key(), occurrenceAt))
                .toList();
        if (!triggerFirer.fire(definition.key(), observed, plan.nextFireAt(), occurrences, emptyPayload(), now)) {
            return;
        }
        if (plan.misfired()) {
            log.warn("job '{}' missed occurrence(s) — next_fire_at was {} at tick time {}; misfire policy {} applied: "
                    + "{} occurrence(s) materialized, next fire at {}",
                    definition.key().value(), observed, now, definition.misfire(), occurrences.size(), plan.nextFireAt());
        } else {
            log.debug("job '{}' fired {} occurrence(s), next fire at {}",
                    definition.key().value(), occurrences.size(), plan.nextFireAt());
        }
    }

    /**
     * O payload vazio das ocorrências do scheduler — classe concreta de
     * propósito, nunca {@code Map.of()}: {@code payload_type} persiste a
     * classe exata, e a releitura precisa de um tipo que o Jackson
     * instancie de volta.
     */
    private static LinkedHashMap<String, Object> emptyPayload() {
        return new LinkedHashMap<>();
    }

    /**
     * Ocorrência materializada pelo trigger: {@code scheduled_at} = o
     * instante da ocorrência (identidade do disparo), {@code actor}
     * distingue a corrente do scheduler de disparos manuais, payload vazio.
     */
    private static Execution newOccurrence(JobKey jobKey, Instant occurrenceAt) {
        return new Execution(ExecutionId.of(UUIDv7.randomUUIDString()), jobKey, ExecutionState.ENQUEUED,
                occurrenceAt, null, List.of(), Execution.SCHEDULER_ACTOR, Priority.NORMAL, null);
    }

    // ─── a escada de timeout/cancel/watchdog (ADR-0034/0051) ────────────────

    /**
     * ADR-0034: o deadline de {@code JobDefinition.timeout} verificado de
     * carona na varredura do tick — atraso de até um intervalo do loop
     * (entre o piso e {@code min(max-poll-interval, node-lease-ttl/3)},
     * conforme o backoff), zero thread nova, ativo também em
     * PAUSED/DRAINING.
     * Dispara flag + interrupt; o desfecho é passivo — gravado quando o
     * handler parar ({@link Dispatcher}), nunca aqui. O relógio corre do
     * início REAL do handler — fila de runner não conta.
     */
    private void signalJobTimeouts() {
        inFlightAttempts.forEach((id, attempt) -> {
            if (attempt.needsTimeoutSignal()) {
                log.warn("execution {} exceeded its job timeout {} — cancellation signalled and the handler interrupted; "
                        + "the outcome follows when the handler stops (ADR-0034)", id, attempt.timeout);
                attempt.signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, true);
            }
        });
    }

    /**
     * ADR-0034: observa {@code POST /executions/{id}/cancel} — lê em lote
     * (na posse, só quando há in-flight) a flag {@code cancel_requested} do
     * próprio in-flight e levanta {@code MANUAL} como flag pura, sem
     * interrupt: cancel é cooperativo por contrato. Staleness efetiva ≤ um
     * intervalo do loop (teto {@code min(max-poll-interval,
     * node-lease-ttl/3)}) + duração do tick. Ativo também em
     * PAUSED/DRAINING.
     */
    private void pollCancelRequests() {
        List<ExecutionId> unsignalled = inFlightAttempts.entrySet().stream()
                .filter(entry -> !entry.getValue().signal.cancellationRequested())
                .map(Map.Entry::getKey)
                .toList();
        if (unsignalled.isEmpty()) {
            return;
        }
        for (ExecutionId id : leaseStore.findCancelRequested(unsignalled)) {
            InFlightAttempt attempt = inFlightAttempts.get(id);
            if (attempt != null) {
                log.info("execution {} has a standing cancel request — cooperative cancellation signalled to the handler", id);
                attempt.signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
            }
        }
    }

    /**
     * O Watchdog Bound (ADR-0051): execução cujo runtime monotônico passou
     * de {@code mohs.engine.watchdog-timeout} tem a posse LIBERADA
     * explicitamente ({@link Dispatcher#abandonOwnership} — attempt
     * sintético pelo orçamento de retry, cercado pela posse desta
     * encarnação). MARCA a encarnação em vez de removê-la (review
     * ADR-0034): o zumbi continua no mapa pra escalada de drain e poll de
     * cancel o alcançarem. Falha na liberação não marca — o próximo tick
     * tenta de novo.
     */
    private void signalWatchdogOverruns() {
        inFlightAttempts.forEach((id, attempt) -> {
            if (!attempt.ownershipReleased && watchdogBoundExceeded(attempt.submittedNanos)) {
                releaseOverrunOwnership(id, attempt);
            }
        });
    }

    /** Um estouro do bound: libera a posse cercada; falha na liberação não marca — o próximo tick tenta de novo. */
    private void releaseOverrunOwnership(ExecutionId id, InFlightAttempt attempt) {
        try {
            dispatcher.abandonOwnership(attempt.execution, attempt.definition, attempt.grant,
                    "watchdog bound " + settings.watchdogTimeout() + " exceeded — node released ownership; "
                            + "the local handler keeps running as a zombie and its result will be discarded");
            attempt.ownershipReleased = true;
            log.warn("execution {} exceeded mohs.engine.watchdog-timeout {} — ownership released (retry budget "
                    + "applied); the local handler keeps running as a zombie until it finishes",
                    id, settings.watchdogTimeout());
        } catch (RuntimeException e) {
            log.warn("could not release ownership of watchdog-overrun execution {} — will retry next tick",
                    id, e);
        }
    }

    /** Runtime por tempo monotônico ({@code System.nanoTime}) — duração nunca vem do {@code Clock} injetado, que pode saltar no resync. */
    private boolean watchdogBoundExceeded(long startedNanos) {
        Duration bound = settings.watchdogTimeout();
        return bound != null && System.nanoTime() - startedNanos >= bound.toNanos();
    }

    /**
     * Uma encarnação de dispatch em voo neste node: o instante monotônico do
     * submit (base do Watchdog Bound — fila conta de propósito), o
     * {@code timeout} do job (ADR-0034; {@code null} = sem timeout), o
     * {@link CancellationSignal} e a posse ({@link Dispatcher.Grant}).
     * Classe, não record, e sem {@code equals} de propósito: a igualdade É
     * a identidade — cada dispatch é uma encarnação distinta, e o
     * {@code remove(id, attempt)} de dois argumentos só pode remover ESTA.
     */
    private static final class InFlightAttempt {

        final long submittedNanos = System.nanoTime();
        final @Nullable Duration timeout;
        final CancellationSignal signal = new CancellationSignal();
        /** A encarnação em si — o que o watchdog precisa pra liberar a posse sem consulta nova. */
        final Execution execution;
        final JobDefinition definition;
        final Dispatcher.Grant grant;
        /**
         * Posse liberada pelo watchdog — escrito só pela thread do tick.
         * A entrada continua no mapa até a conclusão: zumbi marcado ainda
         * recebe o interrupt do shutdown e o sinal de cancel (review
         * ADR-0034).
         */
        volatile boolean ownershipReleased;

        InFlightAttempt(@Nullable Duration timeout, Execution execution, JobDefinition definition, Dispatcher.Grant grant) {
            this.timeout = timeout;
            this.execution = execution;
            this.definition = definition;
            this.grant = grant;
        }

        /** O gatilho da varredura do tick (ADR-0034): tem timeout próprio, o handler roda há mais que ele e o sinal ainda não foi levantado. */
        boolean needsTimeoutSignal() {
            return timeout != null && !signal.cancellationRequested() && signal.handlerRunningLongerThan(timeout);
        }
    }

    /**
     * Falha terminal decidida ainda na thread do tick — definição removida
     * ou runner sem correspondente falha só esta execução, nunca o tick:
     * se a própria gravação da falha lançar, o resto do lote ainda precisa
     * ser despachado — loga e deixa a posse de pé pra um reaper.
     */
    private void failBeforeDispatchGuarded(Execution execution, @Nullable JobDefinition definition,
            IllegalStateException error, Dispatcher.Grant grant) {
        try {
            dispatcher.failBeforeDispatch(execution, definition, error, grant);
        } catch (RuntimeException e) {
            log.warn("could not record the terminal failure of execution {} ({}) — its lease stands until a reaper reclaims it",
                    execution.id(), error.getMessage(), e);
        }
    }

    /**
     * Payload corrompido/classe sumida do classpath não trava o ciclo —
     * falha só esta execução, direto, sem passar pelo handler. Desde a
     * ADR-0047 só o veredito POR LINHA de {@code findPayloads} chega aqui:
     * falha transiente da consulta em lote fica pro reaper, nunca vira
     * terminal. {@link Dispatcher#failBeforeDispatch} sintetiza o attempt
     * e publica {@code Failed} — mesmo caminho de qualquer falha terminal.
     */
    private void failUnreadablePayload(Execution execution, JobDefinition definition, RuntimeException cause,
            Dispatcher.Grant grant) {
        String message = "payload could not be read: " + Objects.requireNonNullElse(cause.getMessage(), cause.toString());
        failBeforeDispatchGuarded(execution, definition, new IllegalStateException(message, cause), grant);
    }

    /** Package-private — só {@code EngineTest} usa isto pra confirmar a identidade do node por trás de heartbeat/claim. */
    String nodeId() {
        return nodeId;
    }
}
