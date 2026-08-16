package io.mohs.engine;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.definition.JobDefinition;

/**
 * {@link JobDefinition} mais o estado operacional que a ADR-0006
 * distingue do definicional — {@code orphaned}/{@code paused} não são
 * campos de {@link JobDefinition} de propósito (upsert nunca os toca);
 * este tipo só existe pra leitura combinada em {@link JobStore}.
 * {@code runningExecutionCount} é o contador de mutex por job
 * (ADR-0018/0020). {@code nextFireAt} é o estado do trigger (ADR-0035):
 * {@code null} = nada a disparar — on-demand, ou fixed-delay aguardando
 * o fim da execução anterior.
 */
public record StoredJob(JobDefinition definition, boolean orphaned, boolean paused, int runningExecutionCount,
        @Nullable Instant nextFireAt) {

    public StoredJob {
        Objects.requireNonNull(definition, "definition");
        if (runningExecutionCount < 0) {
            throw new IllegalArgumentException("runningExecutionCount must not be negative");
        }
    }
}
