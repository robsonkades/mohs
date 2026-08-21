package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.github.robsonkades.uuidv7.UUIDv7;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;

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
 * <p><b>Liveness (ADR-0012):</b> a lease de tudo que este node executa é
 * renovada em lote a cada tick — handler saudável mais lento que
 * {@code lease-ttl} nunca é reclamado enquanto o node vive; a lease vira
 * detecção de falha, não teto de runtime. O teto opcional é o Watchdog
 * Bound ({@code mohs.engine.watchdog-timeout}): atingido, o node para de
 * renovar, a lease expira e o reaper decide com o orçamento de retry
 * (ADR-0033). Sob falha de nó a garantia é <b>at-least-once</b> quando
 * {@code retries > 0} — com o default {@code retries = 0}, at-most-once.
 *
 * <p><b>Timeout e cancelamento (ADR-0034):</b> o {@code timeout} do job é
 * verificado de carona no tick — flag + interrupt via
 * {@link CancellationSignal}, desfecho passivo no {@link Dispatcher}.
 * Handler que ignora o interrupt continua zumbi até terminar sozinho
 * (a JVM não mata thread que não coopera) e tem o resultado descartado
 * pelo CAS de conclusão — é exatamente o caso do Watchdog Bound, o
 * degrau seguinte da escada.
 */
public final class Engine implements MohsLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    /**
     * Teto de triggers disparados por tick — mesma razão do
     * {@code RECLAIM_LIMIT} do reaper: um boot depois de downtime longo
     * não vira uma varredura sem limite; o excedente continua devido e
     * drena nos ticks seguintes, mais antigo primeiro
     * ({@code findDueRecurring} ordena por {@code next_fire_at}).
     */
    static final int FIRE_LIMIT = 500;

    /**
     * Retention da linha de heartbeat (ADR-0041), em múltiplos de
     * {@code lease-ttl} — derivada de propósito, nenhum knob novo (CLAUDE.md:
     * sem configuração para cenário hipotético). 10 leases (5 min no default)
     * mantém o node morto VISÍVEL como stale por tempo de sobra para
     * {@code GET /nodes} e alertas; depois disso a linha é lixo — cada boot
     * gera {@code node_id} novo, então ela nunca seria reutilizada.
     */
    static final int STALE_NODE_RETENTION_LEASES = 10;

    private final Claimer claimer;
    private final Dispatcher dispatcher;
    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    private final NodeStore nodeStore;
    private final Reaper reaper;
    private final TriggerFirer triggerFirer;
    private final FiringPlanner firingPlanner;
    private final Clock clock;
    private final EngineSettings settings;
    private final String nodeId;

    private final AtomicReference<EngineState> state = new AtomicReference<>(EngineState.CREATED);
    private final Set<CompletableFuture<Void>> inFlight = ConcurrentHashMap.newKeySet();
    /**
     * As execuções em voo NESTE node — a base da renovação de lease
     * (ADR-0012) e da escada de timeout/cancelamento (ADR-0034) a cada
     * tick. Entrada nasce no submit e SÓ morre na conclusão
     * ({@code whenComplete}): os drops de renovação (watchdog, posse
     * perdida) MARCAM a encarnação ({@code renewalStopped}) em vez de
     * removê-la — o zumbi continua alcançável pela escalada de drain e
     * pelo poll de cancel; removê-lo o deixaria fora do alcance de
     * qualquer sinal exatamente no cenário pra que a escalada existe
     * (review ADR-0034). Exceção única: um re-claim DESTE node pro mesmo
     * id substitui a entrada marcada no {@code put} do submit — o zumbi
     * antigo volta a ficar inalcançável (mesma lacuna de antes da marca;
     * o mapa comporta uma encarnação por id de propósito).
     */
    private final ConcurrentHashMap<ExecutionId, InFlightAttempt> inFlightAttempts = new ConcurrentHashMap<>();
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
            TriggerFirer triggerFirer,
            Clock clock,
            EngineSettings settings,
            TaskScheduler tickScheduler,
            RunnerRegistry runnerRegistry
    ) {
        this.claimer = Objects.requireNonNull(claimer, "claimer");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
        this.reaper = Objects.requireNonNull(reaper, "reaper");
        this.triggerFirer = Objects.requireNonNull(triggerFirer, "triggerFirer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.firingPlanner = new FiringPlanner(new NextFireCalculator(), settings.misfireThreshold());
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
        ScheduledFuture<?> handle = tickScheduler.scheduleWithFixedDelay(this::tick, settings.pollInterval());
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
        writeFinalStoppedHeartbeat();
    }

    /**
     * ADR-0041: o último heartbeat do shutdown gracioso grava STOPPED — sem
     * ele, stop limpo e crash ficam indistinguíveis no banco (a linha para
     * no último estado tickado, tipicamente RUNNING/DRAINING, para sempre).
     * Best-effort de propósito: shutdown nunca falha por banco fora — quem
     * morre sem conseguir escrever fica coberto pela staleness (ADR-0012) e
     * pelo purge (ver {@link #purgeStaleNodeRows}).
     *
     * <p>Corrida aceita de propósito (review ADR-0041): um tick que leu o
     * estado antes do {@code state.set(STOPPED)} pode commitar seu heartbeat
     * DEPOIS deste — a linha fica RUNNING/DRAINING apesar do stop limpo.
     * Fechar isso exigiria o stop esperar o tick em curso (lock
     * compartilhado; até {@code leaseTtl/4} de claim rounds), acoplando a
     * latência de shutdown a um cosmético autocurável: staleness + purge
     * cobrem a linha do mesmo jeito que cobririam um crash.
     */
    private void writeFinalStoppedHeartbeat() {
        try {
            nodeStore.heartbeat(nodeId, EngineState.STOPPED, clock.instant());
        } catch (RuntimeException e) {
            log.warn("could not write the final STOPPED heartbeat for node {} — the row will read as its last "
                    + "ticked state until the stale purge collects it", nodeId, e);
        }
    }

    /**
     * ADR-0041: recolhe heartbeats mais velhos que
     * {@code lease-ttl × }{@link #STALE_NODE_RETENTION_LEASES} — de carona
     * no tick, como o reaper. Não é detecção de morte (essa é derivada na
     * LEITURA pela staleness, ADR-0012 — crash não escreve nada): é só
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
                escalateAfterDrainGrace(grace);
                return;
            }
            CompletableFuture<?>[] snapshot = inFlight.toArray(CompletableFuture[]::new);
            try {
                CompletableFuture.allOf(snapshot).get(remainingMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                escalateAfterDrainGrace(grace);
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
     * seguem o retry normal pelo caminho de conclusão de sempre, na própria
     * thread do handler (independe do tick, que {@link #stop} cancela em
     * seguida). Sem segunda janela de espera configurável (YAGNI, ADR-0034);
     * handler surdo ao interrupt perde a renovação com o fim dos ticks e o
     * reaper de outro node o reclama na expiração da lease. Durante o grace
     * nada disso acontece: drain ≠ cancel (ADR-0007).
     */
    private void escalateAfterDrainGrace(Duration grace) {
        // as duas contagens de propósito: inFlight (futures) é quem segura o
        // grace; o mapa é quem a escalada alcança — com a marca de renovação
        // (zumbi fica no mapa), divergem só na janela de conclusão do whenComplete
        log.warn("drain grace period ({}) elapsed with {} dispatch(es) still in flight ({} still signallable) — "
                + "signalling cancellation and interrupting them; their attempts will fail with a node-shutdown cause "
                + "and follow the retry policy (ADR-0034)",
                grace, inFlight.size(), inFlightAttempts.size());
        for (InFlightAttempt attempt : inFlightAttempts.values()) {
            attempt.signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
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
            signalJobTimeouts();
            pollCancelRequests();
            // node vivo renova o que ainda executa — PAUSED/DRAINING incluídos
            // (ADR-0007: drain ≠ cancel; a lease detecta morte, não pausa —
            // sem isto, um drain mais longo que a lease viraria dupla execução
            // do trabalho que ele mesmo espera terminar). Só reaper e claim
            // são exclusivos de RUNNING: "isto ainda é meu" ≠ "quero mais".
            renewOwnedLeases();
            if (current != EngineState.RUNNING) {
                return;
            }
            for (Reaper.Reclaimed reclaimed : reaper.reclaimExpired()) {
                publishReclaimOutcome(reclaimed);
            }
            purgeStaleNodeRows();
            fireDueTriggers();
            claimAndDispatch();
        } catch (RuntimeException e) {
            log.error("engine tick failed — will retry next tick", e);
        }
    }

    /**
     * A etapa "aquisição → dispatch" do tick. ADR-0039: cada claim é
     * limitado pela folga de dispatch — reivindicar além dela só produzia
     * rejeição do runner e execução RUNNING presa até o reaper (medido:
     * 56k rejeições e 11,6k execuções duplicadas num drain de 50k). Node
     * saturado não reivindica; o excedente fica ENQUEUED no banco,
     * reivindicável por qualquer node com folga.
     *
     * <p>ADR-0040: sob backlog, um tick encadeia até {@code claimRounds}
     * claims — a folga é recomputada a cada round (o dispatch é assíncrono,
     * mas {@code inFlight} cresce sincronamente aqui na thread do tick),
     * então os rounds param sozinhos quando o node satura; lote que volta
     * menor que o pedido encerra também (a fila drenou — o round seguinte
     * seria um SELECT vazio). Não há cursor: o CAS de estado do próprio
     * claim tira a linha reivindicada do predicado (e do índice parcial)
     * no commit — o SELECT do round seguinte já começa na cabeça real da
     * fila de prioridade.
     *
     * <p>Dois guards entre rounds (review ADR-0040): (1) {@code drain()}/
     * {@code pause()} no meio dos rounds interrompe o encadeamento — a
     * partir do sinal, o node para de ACEITAR trabalho novo (ADR-0007;
     * Burns, shutdown coordenado), mesma janela de 1 lote de antes; (2) um
     * orçamento monotônico de {@code leaseTtl/4} limita a duração total dos
     * rounds — a renovação de lease roda UMA vez por tick, antes daqui, e
     * rounds que se aproximassem do TTL fariam um handler longo de tick
     * anterior perder a renovação e ser duplicado pelo reaper de outro node
     * (lease é detecção de falha — DDIA; tick longo vira "node morto" para
     * o cluster). Duração por {@code System.nanoTime}, nunca pelo
     * {@code Clock} injetado, que pode saltar no resync.
     *
     * <p>DBTUNE-18: uma consulta de definição por job_key DISTINTO do
     * tick, não por execução — o mapa de memoização atravessa os rounds de
     * propósito (mesma janela de staleness de um tick só; ver
     * {@link #submitDispatch}).
     */
    private void claimAndDispatch() {
        Map<JobKey, Optional<StoredJob>> definitionsByKey = new HashMap<>();
        long roundsBudgetNanos = settings.leaseTtl().toNanos() / 4;
        long startNanos = System.nanoTime();
        for (int round = 0; round < settings.claimRounds(); round++) {
            if (round > 0 && (state.get() != EngineState.RUNNING || System.nanoTime() - startNanos >= roundsBudgetNanos)) {
                return;
            }
            int claimLimit = Math.min(settings.batchSize(), settings.dispatchConcurrency() - inFlight.size());
            if (claimLimit <= 0) {
                return;
            }
            List<Execution> claimed = claimer.claim(nodeId, claimLimit);
            for (Execution execution : claimed) {
                submitDispatch(execution, definitionsByKey);
            }
            if (claimed.size() < claimLimit) {
                return;
            }
        }
    }

    /**
     * ADR-0035: a etapa "trigger devido → aquisição" — ANTES do claim, no
     * mesmo tick, pra ocorrência recém-materializada já ser reivindicável
     * sem esperar o próximo poll. {@link FiringPlanner} decide o que a
     * política de misfire do job dispara; {@link TriggerFirer#fire} é o
     * CAS transacional que resolve a corrida entre nós — perder é rotina,
     * não erro. Falha de um job (ex.: cron irrealizável) não derruba a
     * varredura dos demais — mesma postura de {@link #submitDispatch}.
     */
    private void fireDueTriggers() {
        Instant now = clock.instant();
        for (StoredJob job : jobStore.findDueRecurring(now, FIRE_LIMIT)) {
            JobDefinition definition = job.definition();
            try {
                // dentro do try: contrato violado por um store custom falha SÓ este job, não a varredura nem o claim do tick
                Instant observed = Objects.requireNonNull(job.nextFireAt(), "findDueRecurring returned an unarmed trigger");
                fireTrigger(definition, observed, now);
            } catch (RuntimeException e) {
                log.error("firing job '{}' failed — will retry next tick", definition.key().value(), e);
            }
        }
    }

    /** Um trigger devido: o plano da política de misfire vira ocorrências materializadas pelo CAS — perder o CAS sai calado (ver {@link #fireDueTriggers}). */
    private void fireTrigger(JobDefinition definition, Instant observed, Instant now) {
        FiringPlanner.Plan plan = firingPlanner.plan(definition.schedule(), definition.misfire(), observed, now);
        List<Execution> occurrences = plan.occurrences().stream()
                .map(occurrenceAt -> newOccurrence(definition.key(), occurrenceAt))
                .toList();
        if (!triggerFirer.fire(definition.key(), observed, plan.nextFireAt(), occurrences, emptyPayload())) {
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
     * classe exata do payload, e a releitura
     * ({@code ExecutionStore#findPayload}) precisa de um tipo que o Jackson
     * instancie de volta.
     */
    private static LinkedHashMap<String, Object> emptyPayload() {
        return new LinkedHashMap<>();
    }

    /**
     * Ocorrência materializada pelo trigger: {@code scheduled_at} = o
     * instante da ocorrência (identidade do disparo, não o instante da
     * inserção), {@code actor} distingue a corrente do scheduler de
     * disparos manuais, payload vazio — job recorrente não tem fonte de
     * payload (validação de boot pra handler que exige um: §5.13,
     * pendente).
     */
    private static Execution newOccurrence(JobKey jobKey, Instant occurrenceAt) {
        return new Execution(ExecutionId.of(UUIDv7.randomUUIDString()), jobKey, ExecutionState.ENQUEUED,
                occurrenceAt, null, List.of(), Execution.SCHEDULER_ACTOR, Priority.NORMAL, null);
    }

    /**
     * ADR-0034: o deadline de {@code JobDefinition.timeout} verificado de
     * carona na varredura do tick — granularidade de até 1 poll-interval de
     * atraso (irrelevante pra timeout de job, tipicamente minutos), zero
     * thread nova, ativo também em PAUSED/DRAINING. Dispara flag +
     * interrupt; o desfecho é passivo — gravado quando o handler parar
     * ({@link Dispatcher}), nunca aqui: falhar o attempt com o handler vivo
     * liberaria o slot de concorrência por job com a thread ainda ocupada.
     * Handler surdo ao interrupt é exatamente o caso do Watchdog Bound, o
     * degrau seguinte da escada. O relógio corre do início REAL do handler
     * ({@link CancellationSignal#handlerRunningLongerThan}) — fila de
     * runner não conta.
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
     * (por PK, só quando há in-flight) a flag {@code cancel_requested} do
     * próprio in-flight e levanta {@code MANUAL} como flag pura, sem
     * interrupt: cancel é cooperativo por contrato; quem força é o timeout
     * do job e o Watchdog, que continuam valendo. Staleness efetiva ≤ 1
     * poll-interval + duração do tick (limitada pelo orçamento de rounds
     * da ADR-0040). Ids já sinalizados saem da consulta — o SELECT
     * encolhe enquanto o handler ainda não respondeu, e o INFO sai uma vez
     * só. Ativo também em PAUSED/DRAINING, como o resto da varredura.
     */
    private void pollCancelRequests() {
        List<ExecutionId> unsignalled = inFlightAttempts.entrySet().stream()
                .filter(entry -> !entry.getValue().signal.cancellationRequested())
                .map(Map.Entry::getKey)
                .toList();
        if (unsignalled.isEmpty()) {
            return;
        }
        for (ExecutionId id : executionStore.findCancelRequested(unsignalled)) {
            InFlightAttempt attempt = inFlightAttempts.get(id);
            if (attempt != null) {
                log.info("execution {} has a standing cancel request — cooperative cancellation signalled to the handler", id);
                attempt.signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
            }
        }
    }

    /**
     * ADR-0012: renova em lote a lease de tudo que este node ainda executa
     * — ANTES do reaper no mesmo tick, para as próprias execuções nunca
     * parecerem abandonadas enquanto os ticks rodam. Duas marcas de saída
     * da renovação ({@code renewalStopped} — a entrada fica no mapa),
     * cada uma com WARN único:
     * {@link #dropFromRenewalPastWatchdogBound} (runtime além do bound) e
     * {@link #dropFromRenewalAfterLostLease} (renovação perdida — reclaim
     * concorrente ou conclusão local na corrida).
     */
    private void renewOwnedLeases() {
        List<Map.Entry<ExecutionId, InFlightAttempt>> owned = new ArrayList<>(inFlightAttempts.size());
        for (Map.Entry<ExecutionId, InFlightAttempt> entry : inFlightAttempts.entrySet()) {
            InFlightAttempt attempt = entry.getValue();
            if (attempt.renewalStopped) {
                continue;
            }
            if (watchdogBoundExceeded(attempt.submittedNanos)) {
                dropFromRenewalPastWatchdogBound(entry.getKey(), attempt);
            } else {
                owned.add(Map.entry(entry.getKey(), attempt));
            }
        }
        if (owned.isEmpty()) {
            return;
        }
        Set<ExecutionId> renewed = executionStore.renewLeases(nodeId,
                owned.stream().map(Map.Entry::getKey).toList(), clock.instant().plus(settings.leaseTtl()));
        List<Map.Entry<ExecutionId, InFlightAttempt>> lost = owned.stream()
                .filter(entry -> !renewed.contains(entry.getKey()))
                .toList();
        if (lost.isEmpty()) {
            return;
        }
        Map<ExecutionId, ExecutionState> lostStates = fetchStatesBestEffort(lost.stream().map(Map.Entry::getKey).toList());
        for (Map.Entry<ExecutionId, InFlightAttempt> entry : lost) {
            dropFromRenewalAfterLostLease(entry.getKey(), entry.getValue(), lostStates.get(entry.getKey()));
        }
    }

    /**
     * Consulta diagnóstica dos ids que perderam a renovação — em lote e
     * best-effort. Em lote porque perda em massa (stall > lease seguido de
     * reclaim de todo o in-flight por outro node) é exatamente quando este
     * caminho fica grande: N+1 serial aqui atrasaria heartbeat e renovação
     * do que sobrou, no pior momento. Best-effort porque a consulta só
     * qualifica o WARN — ela nunca pode ter mais autoridade pra abortar o
     * tick do que a informação que agrega: falhou, os drops acontecem do
     * mesmo jeito, com estado desconhecido.
     */
    private Map<ExecutionId, ExecutionState> fetchStatesBestEffort(List<ExecutionId> lostIds) {
        try {
            return executionStore.findByIds(lostIds).stream()
                    .collect(Collectors.toMap(Execution::id, Execution::state));
        } catch (RuntimeException e) {
            log.warn("could not fetch the state of {} lost-lease execution(s) — dropping them from renewal with unknown state",
                    lostIds.size(), e);
            return Map.of();
        }
    }

    /**
     * Watchdog Bound: execução cujo runtime monotônico passou de
     * {@code mohs.engine.watchdog-timeout} sai da renovação — a lease
     * expira sozinha e o reaper decide com o orçamento de retry
     * (ADR-0033). É a rede de segurança de último caso da ADR-0012: entra
     * depois que o {@code timeout} do job falhou em parar o handler.
     * MARCA a encarnação em vez de removê-la (review ADR-0034): a marca na
     * PRÓPRIA instância é inerentemente segura contra o ABA de re-claim
     * (encarnação nova é outro objeto), e o zumbi continua no mapa pra
     * escalada de drain e poll de cancel ainda o alcançarem — a remoção é
     * exclusiva da conclusão ({@code whenComplete}).
     */
    private void dropFromRenewalPastWatchdogBound(ExecutionId id, InFlightAttempt attempt) {
        attempt.renewalStopped = true;
        log.warn("execution {} exceeded mohs.engine.watchdog-timeout {} — lease renewal stopped; the reaper will "
                        + "reclaim it when the lease expires (retry budget applies) and the local handler keeps "
                        + "running as a zombie until it finishes (it already survived or lacked a job timeout interrupt)",
                id, settings.watchdogTimeout());
    }

    /**
     * Id que a renovação perdeu e ainda está no mapa sai da renovação. A
     * causa é ambígua por estado: reclaim concorrente (o handler local
     * virou zumbi, e a escrita terminal dele será descartada pelo CAS de
     * conclusão) ou conclusão local que commitou entre o snapshot da
     * renovação e este drop — {@code SUCCEEDED} cala o WARN: o reaper
     * nunca o produz diretamente, então na prática é a conclusão local
     * vencendo a corrida (exceção não coberta: reclaim seguido de
     * re-execução bem-sucedida em outro node durante um stall local maior
     * que a lease fica sem aviso — o CAS de conclusão ainda descarta o
     * resultado do zumbi local); nos demais estados, o WARN nomeia as
     * duas hipóteses em vez de acusar uma: log operacional é contrato com
     * o operador, e causa errada manda caçar um reaper fantasma às 3h da
     * manhã. MARCA em vez de remover, como
     * {@link #dropFromRenewalPastWatchdogBound} — o zumbi continua
     * alcançável pela escalada e pelo poll; {@code actual} vem de
     * {@link #fetchStatesBestEffort} ({@code null} = desconhecido,
     * impresso como {@code unknown}).
     */
    private void dropFromRenewalAfterLostLease(ExecutionId id, InFlightAttempt attempt, @Nullable ExecutionState actual) {
        attempt.renewalStopped = true;
        if (actual == ExecutionState.SUCCEEDED) {
            return; // na prática, conclusão local venceu a corrida — a exceção (re-execução remota pós-reclaim) está no Javadoc
        }
        log.warn("execution {} dropped from lease renewal (state now {}) — either reclaimed by another node's reaper "
                + "(the local handler is now a zombie; its result will be discarded by the completion CAS) "
                + "or a local completion raced this check",
                id, Objects.toString(actual, "unknown"));
    }

    /** Runtime por tempo monotônico ({@code System.nanoTime}) — duração nunca vem do {@code Clock} injetado, que pode saltar no resync. */
    private boolean watchdogBoundExceeded(long startedNanos) {
        Duration bound = settings.watchdogTimeout();
        return bound != null && System.nanoTime() - startedNanos >= bound.toNanos();
    }

    /**
     * Desfechos do reclaim publicam os mesmos eventos do caminho de
     * dispatch, espelhando exatamente os pares do {@link Dispatcher}:
     * retry → {@code AttemptFailed} + {@code RetryScheduled}; cancel
     * pendente honrado (ADR-0034) → {@code Cancelled}; terminal → só
     * {@code Failed}. É o gancho de alerta de morte de nó que o Javadoc
     * de {@link Failed} anuncia — sem isto, um listener de observabilidade
     * via retries de falha de handler mas ficava cego para os de
     * crash-recovery, que são os que importam às 3h da manhã.
     */
    private void publishReclaimOutcome(Reaper.Reclaimed reclaimed) {
        Execution execution = reclaimed.execution();
        Attempt lastAttempt = execution.attempts().getLast();
        ExecutionEventPublisher events = dispatcher.events();
        if (execution.state() == ExecutionState.CANCELLED) {
            events.publish(new Cancelled(execution.id(), execution.jobKey(), lastAttempt.number()));
            return;
        }
        Exception error = new IllegalStateException(Objects.requireNonNullElse(lastAttempt.error(), "lease expired"));
        if (execution.state() == ExecutionState.RETRY_SCHEDULED) {
            events.publish(new AttemptFailed(execution.id(), execution.jobKey(), lastAttempt.number(), error));
            events.publish(new RetryScheduled(execution.id(), execution.jobKey(), lastAttempt.number() + 1, execution.scheduledAt()));
        } else {
            events.publish(new Failed(execution.id(), execution.jobKey(), lastAttempt.number(), error, reclaimed.attemptsExhausted()));
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
    private void submitDispatch(Execution execution, Map<JobKey, Optional<StoredJob>> definitionsByKey) {
        // Consulta síncrona, na própria thread do tick — mesmo padrão de
        // claimer.claim()/reaper.reclaimExpired() em tick(). DBTUNE-18:
        // memoizada por job_key dentro do tick — os N finds seriais eram o
        // maior componente do ciclo (medido no drain de 50k, Postgres,
        // 2026-08-16: ~260 ms de um ciclo de 454 ms com batch 500; o lote
        // inteiro gotejava pro dispatch no ritmo de um round trip por
        // execução). O snapshot por tick corresponde a uma ordem serial
        // legal do código anterior (todos os finds podiam rodar antes de
        // qualquer escrita concorrente); falha de consulta não é memoizada
        // — a próxima execução do mesmo job_key tenta de novo, mesmo
        // caminho de recuperação de antes.
        Optional<StoredJob> storedJob;
        try {
            storedJob = definitionsByKey.computeIfAbsent(execution.jobKey(), jobStore::find);
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
            failBeforeDispatchGuarded(execution, null, new IllegalStateException(
                    "job definition for " + execution.jobKey() + " was removed after this execution was claimed (e.g. Mohs.remove between claim and dispatch)"));
            return;
        }
        JobDefinition definition = storedJob.orElseThrow().definition();

        AsyncTaskExecutor executor;
        try {
            executor = runnerRegistry.resolve(definition.runner());
        } catch (NoSuchElementException e) {
            failBeforeDispatchGuarded(execution, definition, new IllegalStateException(
                    "runner could not be resolved: " + Objects.requireNonNullElse(e.getMessage(), e.toString()), e));
            return;
        }

        // registrado ANTES do runAsync: a execução está RUNNING no banco desde o
        // claim — a primeira renovação precisa cobri-la mesmo que o executor a
        // deixe na fila por um tick inteiro. Remoções sempre com o remove de
        // dois argumentos: o mesmo ExecutionId pode ser re-reivindicado por
        // este node após um reclaim, e o whenComplete tardio de um zumbi não
        // pode apagar a entrada da encarnação nova (ABA em memória —
        // InFlightAttempt tem igualdade de identidade de propósito).
        InFlightAttempt attempt = new InFlightAttempt(definition.timeout());
        inFlightAttempts.put(execution.id(), attempt);
        CompletableFuture<Void> future;
        try {
            future = CompletableFuture.runAsync(() -> resolvePayloadAndDispatch(execution, definition, attempt.signal), executor);
        } catch (RuntimeException e) {
            inFlightAttempts.remove(execution.id(), attempt);
            log.warn("runner executor rejected execution {} — already claimed, will sit RUNNING until the reaper reclaims it on lease expiry",
                    execution.id(), e);
            return;
        }
        inFlight.add(future);
        future.whenComplete((_, thrown) -> {
            // resolvePayloadAndDispatch trata as falhas conhecidas — throwable aqui
            // significa que a própria gravação da falha lançou (ex.: colisão de PK
            // de attempts numa conclusão concorrente); engolir esconderia o único
            // rastro do zumbi
            if (thrown != null) {
                log.error("dispatch of execution {} threw outside the normal failure paths", execution.id(), thrown);
            }
            inFlightAttempts.remove(execution.id(), attempt);
            inFlight.remove(future);
        });
    }

    /**
     * Uma encarnação de dispatch em voo neste node: o instante monotônico do
     * submit (base do Watchdog Bound — fila conta de propósito, fila
     * entupida também é problema de liveness), o {@code timeout} do job
     * (ADR-0034; {@code null} = sem timeout próprio) e o
     * {@link CancellationSignal} da encarnação. Classe, não record, e sem
     * {@code equals} de propósito: a igualdade É a identidade — cada
     * dispatch é uma encarnação distinta, e o {@code remove(id, attempt)}
     * de dois argumentos do mapa só pode remover ESTA.
     */
    private static final class InFlightAttempt {

        final long submittedNanos = System.nanoTime();
        final @Nullable Duration timeout;
        final CancellationSignal signal = new CancellationSignal();
        /**
         * Renovação parou (watchdog ou posse perdida) — escrito só pela
         * thread do tick, lido por ela e pela escalada. A entrada continua
         * no mapa até a conclusão: zumbi marcado ainda recebe o interrupt
         * do shutdown e o sinal de cancel (review ADR-0034).
         */
        volatile boolean renewalStopped;

        InFlightAttempt(@Nullable Duration timeout) {
            this.timeout = timeout;
        }

        /** O gatilho da varredura do tick (ADR-0034): tem timeout próprio, o handler roda há mais que ele e o sinal ainda não foi levantado. */
        boolean needsTimeoutSignal() {
            return timeout != null && !signal.cancellationRequested() && signal.handlerRunningLongerThan(timeout);
        }
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
    private void failBeforeDispatchGuarded(Execution execution, @Nullable JobDefinition definition, IllegalStateException error) {
        try {
            dispatcher.failBeforeDispatch(execution, definition, error);
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
    private void resolvePayloadAndDispatch(Execution execution, JobDefinition definition, CancellationSignal signal) {
        Object payload;
        try {
            payload = executionStore
                    .findPayload(execution.id())
                    .orElseThrow(() -> new IllegalStateException("execution " + execution.id() + " vanished before payload could be read — should be unreachable"));
        } catch (RuntimeException e) {
            failUnreadablePayload(execution, definition, e);
            return;
        }
        dispatcher.dispatch(execution, definition, payload, signal);
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
    private void failUnreadablePayload(Execution execution, JobDefinition definition, RuntimeException cause) {
        String message = "payload could not be read: " + Objects.requireNonNullElse(cause.getMessage(), cause.toString());
        dispatcher.failBeforeDispatch(execution, definition, new IllegalStateException(message, cause));
    }

    /** Package-private — só {@code EngineTest} usa isto pra confirmar a identidade do node por trás de heartbeat/claim. */
    String nodeId() {
        return nodeId;
    }
}
