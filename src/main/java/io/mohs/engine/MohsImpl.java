package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.robsonkades.uuidv7.UUIDv7;

import io.mohs.core.Batch;
import io.mohs.core.BatchBuilder;
import io.mohs.core.BatchSnapshot;
import io.mohs.core.ExecutionQuery;
import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.NodeSnapshot;
import io.mohs.core.OverviewSnapshot;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.ScheduleCommand;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.core.resource.RateLimit;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Schedule;

/**
 * {@link Mohs} sobre {@link JobStore}/{@link ExecutionStore} — {@code
 * define}/{@code remove} delegam direto; {@code schedule} monta um
 * {@link ScheduleCommandImpl}. {@link #lifecycle()} devolve o {@link Engine}
 * já injetado, que implementa {@link MohsLifecycle} diretamente — sem
 * adapter, {@code io.mohs.autoconfigure} injeta o mesmo bean nos dois
 * papéis.
 *
 * <p>{@link #batch} escreve a linha do lote e os membros; a contagem de
 * conclusão é do {@code ExecutionStore.complete}, na mesma transação do CAS
 * de estado (ADR-0043) — aqui não há contador nenhum.
 */
public final class MohsImpl implements Mohs {

    private static final Logger log = LoggerFactory.getLogger(MohsImpl.class);

    /** Actor de quem chama {@link Mohs#schedule}/{@link Mohs#batch} sem {@link ScheduleCommand#as(String)} explícito — o próprio processo, não um usuário identificável. */
    static final String DEFAULT_ACTOR = "application";

    private final JobStore jobStore;
    private final ExecutionStore executionStore;
    private final NodeStore nodeStore;
    private final RateLimitStore rateLimitStore;
    private final HandlerRegistry handlerRegistry;
    private final Clock clock;
    private final MohsLifecycle lifecycle;
    private final BatchStore batchStore;
    private final BatchCompletionCallbacks callbacks;

    public MohsImpl(JobStore jobStore, ExecutionStore executionStore, NodeStore nodeStore, RateLimitStore rateLimitStore,
            HandlerRegistry handlerRegistry, Clock clock, MohsLifecycle lifecycle, BatchStore batchStore,
            BatchCompletionCallbacks callbacks) {
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
        this.rateLimitStore = Objects.requireNonNull(rateLimitStore, "rateLimitStore");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
        this.callbacks = Objects.requireNonNull(callbacks, "callbacks");
    }

    @Override
    public <T> ScheduleCommand schedule(JobRef<T> ref, T payload) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(payload, "payload");
        return new ScheduleCommandImpl(jobStore, executionStore, clock, ref.key(), payload);
    }

    @Override
    public ScheduleCommand schedule(String jobId, Object payload) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(payload, "payload");
        return new ScheduleCommandImpl(jobStore, executionStore, clock, JobKey.of(jobId), payload);
    }

    /**
     * O total do lote é fixado na criação, então os membros são coletados
     * ANTES de a linha existir: não há estado "ainda aceitando membros" para
     * rastrear, e o lote nasce já sabendo quantas conclusões o fecham
     * (ADR-0043). Lote vazio é recusado na entrada — ele nunca completaria,
     * e um lote eternamente aberto é pior que um erro.
     */
    @Override
    public Batch batch(String name, Consumer<BatchBuilder> configurer) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(configurer, "configurer");
        List<Member> members = collectMembers(configurer);
        if (members.isEmpty()) {
            throw new IllegalArgumentException("a batch needs at least one member — an empty batch would never complete");
        }

        requireAllDefined(members);

        String batchId = UUIDv7.randomUUIDString();
        batchStore.insert(batchId, members.size());
        enqueueMembers(members, batchId);
        return new BatchImpl(batchId, callbacks);
    }

    private static List<Member> collectMembers(Consumer<BatchBuilder> configurer) {
        CollectingBatchBuilder collected = new CollectingBatchBuilder();
        configurer.accept(collected);
        return collected.members;
    }

    /**
     * Todo membro nasce {@code ENQUEUED} carregando o {@code batchId} — é ele
     * que faz a conclusão contar no lote, e todos compartilham o mesmo
     * {@code scheduledAt} porque o lote foi pedido de uma vez só.
     */
    private void enqueueMembers(List<Member> members, String batchId) {
        Instant scheduledAt = clock.instant();
        for (Member member : members) {
            executionStore.insert(new Execution(ExecutionId.of(UUIDv7.randomUUIDString()), member.key(),
                    ExecutionState.ENQUEUED, scheduledAt, null, List.of(), DEFAULT_ACTOR, Priority.NORMAL, null, batchId),
                    member.payload());
        }
    }

    /**
     * TODOS os membros são validados antes de qualquer escrita, e não um a
     * um durante o enfileiramento: um job inexistente no meio do lote
     * deixaria a linha do lote gravada com {@code total} cheio e só parte dos
     * membros enfileirada — o resto nunca existiria, o lote nunca fecharia e
     * o {@code BatchCompleted} nunca dispararia, em silêncio. É exatamente o
     * modo de falha que a ADR-0043 existe para não ter.
     *
     * <p>Chaves distintas, não uma consulta por membro: um lote grande
     * costuma apontar para poucos jobs, e validar 1.000 membros de um job só
     * é uma pergunta, não mil.
     */
    private void requireAllDefined(List<Member> members) {
        members.stream().map(Member::key).distinct().forEach(this::requireDefined);
    }

    /** Mesmo motivo de {@code ScheduleCommandImpl.at}: sem isto o chamador veria uma violação de FK crua, não uma mensagem que ensina. */
    private void requireDefined(JobKey key) {
        if (jobStore.find(key).isEmpty()) {
            throw new IllegalArgumentException(
                    "no job registered for id '" + key.value() + "' — call Mohs.define first");
        }
    }

    @Override
    public Optional<BatchSnapshot> findBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId");
        return batchStore.find(batchId)
                .map(counters -> new BatchSnapshot(counters.batchId(), counters.total(), counters.succeeded(),
                        counters.failed()));
    }

    /**
     * O lote já contou esta falha, e o contador não tem como devolvê-la sem
     * reabrir o lote — o que faria {@code BatchCompleted} deixar de ser
     * terminal e, pior, o segundo evento não encontraria mais o callback
     * one-shot de {@code onCompletion}: quem salvou o membro é justamente
     * quem ficaria sem a notificação do fim real. Recusa explícita, então,
     * em vez de contagem dupla silenciosa (ADR-0043).
     */
    private static IllegalStateException batchMemberNotRetryable(ExecutionId executionId, String batchId) {
        return new IllegalStateException("execution " + executionId + " is a member of batch " + batchId
                + " — a batch member is not retried individually, because the batch already counted this"
                + " failure and counting it again would close the batch early. Schedule the job standalone"
                + " to redo the work (ADR-0043)");
    }

    private record Member(JobKey key, Object payload) {
    }

    /** Acumula os membros; nada é persistido enquanto o total não está fechado. */
    private static final class CollectingBatchBuilder implements BatchBuilder {

        private final List<Member> members = new ArrayList<>();

        @Override
        public <T> void add(JobRef<T> ref, T payload) {
            Objects.requireNonNull(ref, "ref");
            Objects.requireNonNull(payload, "payload");
            members.add(new Member(ref.key(), payload));
        }
    }

    /** O recibo: {@code batchId} ja e duravel quando isto volta (ADR-0003, clausula 2). */
    private record BatchImpl(String batchId, BatchCompletionCallbacks callbacks) implements Batch {

        @Override
        public Batch onCompletion(Consumer<BatchCompleted> callback) {
            callbacks.register(batchId, callback);
            return this;
        }
    }

    @Override
    public void define(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        jobStore.upsert(definition);
    }

    /** Job desconhecido (ou já aposentado) é no-op — mesma postura de {@link Mohs#pause}. */
    @Override
    public void remove(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.find(jobKey).ifPresent(stored -> {
            if (stored.definition().source() == DefinitionSource.ANNOTATION) {
                throw new IllegalArgumentException("job '" + jobKey.value()
                        + "' is @MohsJob-annotated — Mohs.remove only retires PROGRAMMATIC definitions; "
                        + "remove the annotation instead and the scanner marks it ORPHANED on the next boot");
            }
            jobStore.remove(jobKey);
        });
    }

    @Override
    public Optional<JobSnapshot> findJob(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        return jobStore.find(jobKey).map(this::toSnapshot);
    }

    @Override
    public List<JobSnapshot> jobs() {
        try (var stored = jobStore.findAll()) {
            return stored.map(this::toSnapshot).toList();
        }
    }

    @Override
    public void pause(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.pause(jobKey);
    }

    @Override
    public void resume(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.resume(jobKey);
    }

    @Override
    public Optional<JobSnapshot> reschedule(JobKey jobKey, Schedule schedule) {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(schedule, "schedule");
        if (!jobStore.reschedule(jobKey, schedule)) {
            return Optional.empty();
        }
        return findJob(jobKey);
    }

    @Override
    public Optional<Class<?>> payloadType(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        return handlerRegistry.payloadType(jobKey);
    }

    /**
     * ADR-0034 — a orquestração das duas metades do cancel: primeiro o CAS
     * de pendente; perdeu (a linha já roda, ou já terminou), tenta a flag
     * cooperativa de {@code RUNNING}. Os dois predicados particionam o
     * espaço de estados, mas o estado pode migrar ENTRE as checagens
     * (TOCTOU — DDIA cap. 7: um CAS cobre um predicado, não uma sequência):
     * uma conclusão de attempt que leva {@code RUNNING → RETRY_SCHEDULED}
     * no meio do par faria a ordem do operador cair no vazio. A segunda
     * passada fecha a janela — outra migração exigiria um ciclo de attempt
     * inteiro dentro de microssegundos. Em terminal ambas continuam no-op:
     * cancelar o que já decidiu não muda nada, e o retorno mostra o estado
     * que valeu.
     */
    @Override
    public Optional<Execution> cancel(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        boolean cancelledPending = executionStore.cancelIfPending(executionId);
        if (!cancelledPending && !executionStore.requestCancellation(executionId)) {
            cancelledPending = executionStore.cancelIfPending(executionId);
            if (!cancelledPending) {
                executionStore.requestCancellation(executionId);
            }
        }
        Optional<Execution> result = executionStore.find(executionId);
        if (cancelledPending) {
            result.ifPresent(this::rearmAfterFinishChain);
        }
        return result;
    }

    /**
     * ADR-0035 — cura da corrente fixed-delay: ocorrência do scheduler
     * cancelada ainda pendente não passa pelo caminho de conclusão que
     * rearma o trigger; sem isto a corrente morreria em silêncio (o
     * {@code next_fire_at} ficou {@code NULL} aguardando um fim que nunca
     * vem). Só ocorrência do scheduler — execução manual cancelada não é
     * a corrente. O guard {@code IS NULL} de {@link JobStore#armNextFire}
     * protege contra rearmar uma série já viva.
     *
     * <p>Janela residual aceita: crash entre {@code cancelIfPending} e este
     * rearme deixa a corrente desarmada — a cura de {@code NULL} do upsert
     * (boot/define) rearma; mesma postura da janela residual documentada na
     * ADR-0033. Transacionar exigiria vazar a fronteira de storage pra cá.
     */
    private void rearmAfterFinishChain(Execution execution) {
        if (!Execution.SCHEDULER_ACTOR.equals(execution.actor())) {
            return;
        }
        jobStore.find(execution.jobKey()).ifPresent(stored -> {
            if (stored.definition().schedule() instanceof IntervalSpec interval && interval.afterFinish()) {
                jobStore.armNextFire(execution.jobKey(), clock.instant().plus(interval.interval()));
            }
        });
    }

    @Override
    public Optional<Execution> findExecution(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        return executionStore.find(executionId);
    }

    /**
     * O CAS da porta é a autoridade; a leitura só distingue os motivos de
     * derrota (inexistente × job aposentado × estado errado) — nunca
     * decide. Derrota com a linha ainda {@code FAILED} = o guard de
     * {@code retired} barrou (o CAS só recusa FAILED por essa via);
     * {@code RETRY_SCHEDULED} = provável POST duplicado. Perder a corrida
     * pra outra mutação entre o CAS e a leitura muda a mensagem, não o
     * desfecho: quem venceu o CAS foi ela.
     */
    @Override
    public Optional<Execution> retry(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        if (executionStore.rearmForManualRetry(executionId, clock.instant())) {
            log.info("execution {} manually rearmed for retry — rejoins the claim path bypassing the retries budget (ADR-0033)",
                    executionId.value());
            return executionStore.find(executionId);
        }
        Execution current = executionStore.find(executionId).orElse(null);
        if (current == null) {
            return Optional.empty();
        }
        throw switch (current.state()) {
            case FAILED -> current.batchId() != null
                    ? batchMemberNotRetryable(executionId, current.batchId())
                    : new IllegalStateException("execution " + executionId + " belongs to a removed job — "
                            + "a retried execution of a retired job would never be claimed (ADR-0033)");
            case RETRY_SCHEDULED -> new IllegalStateException("execution " + executionId
                    + " is already rearmed for retry — likely a duplicate retry request");
            default -> new IllegalStateException("execution " + executionId + " is " + current.state()
                    + " — only FAILED executions can be manually retried (a cancelled execution was an explicit "
                    + "decision; the other states are owned by the engine)");
        };
    }

    @Override
    public List<Execution> executions(ExecutionQuery query) {
        Objects.requireNonNull(query, "query");
        // cursor em branco (ex.: ?cursor= na REST) = primeira página, não IAE de ExecutionId.of
        String rawCursor = query.cursor();
        ExecutionId cursor = rawCursor == null || rawCursor.isBlank() ? null : ExecutionId.of(rawCursor);
        return executionStore.findPage(query.jobKey(), query.status(), query.from(), query.to(), cursor, query.limit());
    }

    /** Mais recente primeiro (empate por nodeId — ordem exposta em API é contrato, nunca a ordem física da tabela): o vivo interessa antes do suspeito — a idade do heartbeat É a informação (ADR-0012). */
    @Override
    public List<NodeSnapshot> nodes() {
        return nodeStore.findAll().stream()
                .sorted(Comparator.comparing(StoredNode::lastHeartbeatAt).reversed()
                        .thenComparing(StoredNode::nodeId))
                .map(stored -> new NodeSnapshot(stored.nodeId(), stored.state(), stored.lastHeartbeatAt()))
                .toList();
    }

    /** Ordenado por nome: a lista é lida por gente, e ordem estável entre chamadas é o mínimo pra comparar dois retratos. */
    @Override
    public List<RateLimitSnapshot> rateLimits() {
        try (var declared = rateLimitStore.findAll()) {
            return declared.sorted(Comparator.comparing(snapshot -> snapshot.rateLimit().name())).toList();
        }
    }

    /**
     * Ajuste em duas etapas deliberadamente: {@link RateLimitStore#upsert}
     * criaria o limite se ele não existisse, e criar limite não é ato de
     * PATCH (ADR-0042/ADR-0006 — declarar é boot). A corrida entre o find e
     * o upsert é aceita: dois operadores ajustando o MESMO limite no mesmo
     * instante é "última escrita vence", que é o que um PATCH promete.
     */
    @Override
    public Optional<RateLimitSnapshot> adjustRateLimit(String name, int max, Duration window) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(window, "window");
        if (rateLimitStore.find(name).isEmpty()) {
            return Optional.empty();
        }
        rateLimitStore.upsert(new RateLimit(name, max, window));
        return rateLimitStore.find(name);
    }

    /**
     * Pura composição das duas contagens da porta — a normalização (zeros,
     * só estados vivos) é do próprio {@link OverviewSnapshot}. A janela é
     * validada ANTES de tocar o banco (Effective Java, Item 49): com
     * {@code ?window=} exposto na REST, deixar o snapshot rejeitar no fim
     * custaria duas queries com {@code since} no futuro pra chegar na
     * mesma IAE — a regra continua uma só, a do record.
     */
    @Override
    public OverviewSnapshot overview(Duration throughputWindow) {
        Objects.requireNonNull(throughputWindow, "throughputWindow");
        if (throughputWindow.isNegative() || throughputWindow.isZero()) {
            throw new IllegalArgumentException("throughputWindow must be positive, got " + throughputWindow);
        }
        Map<ExecutionState, Long> outcomes =
                executionStore.countTerminalOutcomesSince(clock.instant().minus(throughputWindow));
        return new OverviewSnapshot(executionStore.countActiveByState(), throughputWindow,
                outcomes.getOrDefault(ExecutionState.SUCCEEDED, 0L),
                outcomes.getOrDefault(ExecutionState.FAILED, 0L));
    }

    @Override
    public MohsLifecycle lifecycle() {
        return lifecycle;
    }

    /**
     * {@code nextFireAt} é o estado real do trigger (ADR-0035), não um
     * recálculo por cima do relógio — que mentia pra fixed-delay (o
     * próximo disparo é desconhecido até a execução terminar) e ignorava
     * misfire. Pausado exibe {@code null}: pausa bloqueia o trigger.
     */
    private JobSnapshot toSnapshot(StoredJob stored) {
        return new JobSnapshot(stored.definition(), stored.paused(), stored.paused() ? null : stored.nextFireAt());
    }
}
