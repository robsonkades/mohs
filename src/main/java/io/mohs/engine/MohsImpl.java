package io.mohs.engine;

import java.time.Clock;
import java.util.Objects;
import java.util.function.Consumer;

import io.mohs.core.Batch;
import io.mohs.core.BatchBuilder;
import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.ScheduleCommand;
import io.mohs.core.definition.JobDefinition;
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
    private final Clock clock;
    private final MohsLifecycle lifecycle;

    public MohsImpl(JobStore jobStore, ExecutionStore executionStore, Clock clock, MohsLifecycle lifecycle) {
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
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

    @Override
    public void remove(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.remove(jobKey);
    }

    @Override
    public MohsLifecycle lifecycle() {
        return lifecycle;
    }
}
