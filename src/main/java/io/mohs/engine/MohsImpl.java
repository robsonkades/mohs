package io.mohs.engine;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import io.mohs.core.Batch;
import io.mohs.core.BatchBuilder;
import io.mohs.core.ExecutionQuery;
import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.ScheduleCommand;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;

/**
 * {@link Mohs} sobre {@link JobStore}/{@link ExecutionStore} — {@code
 * define}/{@code remove} delegam direto; {@code schedule} monta um
 * {@link ScheduleCommandImpl}. {@link #lifecycle()} devolve o {@link Engine}
 * já injetado, que implementa {@link MohsLifecycle} diretamente — sem
 * adapter, {@code io.mohs.autoconfigure} injeta o mesmo bean nos dois
 * papéis.
 *
 * <p>{@link #batch} ainda não está ligado: contagem de conclusão
 * ({@code BatchStore.incrementSucceeded}/{@code incrementFailed}) nunca foi
 * conectada a {@link Dispatcher}/{@code ExecutionStore.complete} — feature
 * de motor ainda não construída, não coberta por esta rodada de
 * {@code io.mohs.autoconfigure}.
 */
public final class MohsImpl implements Mohs {

    /** Actor de quem chama {@link Mohs#schedule}/{@link Mohs#batch} sem {@link ScheduleCommand#as(String)} explícito — o próprio processo, não um usuário identificável. */
    static final String DEFAULT_ACTOR = "application";

    private final JobStore jobStore;
    private final ExecutionStore executionStore;
    private final HandlerRegistry handlerRegistry;
    private final Clock clock;
    private final MohsLifecycle lifecycle;
    private final NextFireCalculator nextFireCalculator = new NextFireCalculator();

    public MohsImpl(JobStore jobStore, ExecutionStore executionStore, HandlerRegistry handlerRegistry, Clock clock, MohsLifecycle lifecycle) {
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
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

    @Override
    public Batch batch(String name, Consumer<BatchBuilder> configurer) {
        throw new UnsupportedOperationException(
                "Mohs.batch is not wired yet — batch completion counters and Batch.onCompletion "
                        + "still need to be connected through Dispatcher/ExecutionStore.complete");
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
        if (!executionStore.cancelIfPending(executionId) && !executionStore.requestCancellation(executionId)
                && !executionStore.cancelIfPending(executionId)) {
            executionStore.requestCancellation(executionId);
        }
        return executionStore.find(executionId);
    }

    @Override
    public Optional<Execution> findExecution(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        return executionStore.find(executionId);
    }

    @Override
    public List<Execution> executions(ExecutionQuery query) {
        Objects.requireNonNull(query, "query");
        // cursor em branco (ex.: ?cursor= na REST) = primeira página, não IAE de ExecutionId.of
        String rawCursor = query.cursor();
        ExecutionId cursor = rawCursor == null || rawCursor.isBlank() ? null : ExecutionId.of(rawCursor);
        return executionStore.findPage(query.jobKey(), query.status(), query.from(), query.to(), cursor, query.limit());
    }

    @Override
    public MohsLifecycle lifecycle() {
        return lifecycle;
    }

    private JobSnapshot toSnapshot(StoredJob stored) {
        Instant nextFireAt = stored.paused() ? null : nextFireCalculator.nextFireAfter(stored.definition().schedule(), clock.instant()).orElse(null);
        return new JobSnapshot(stored.definition(), stored.paused(), nextFireAt);
    }
}
