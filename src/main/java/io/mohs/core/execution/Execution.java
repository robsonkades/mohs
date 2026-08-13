package io.mohs.core.execution;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import io.mohs.core.job.JobKey;
import io.mohs.core.definition.JobDefinition;

import org.jspecify.annotations.Nullable;

/**
 * Um único disparo de um {@link JobDefinition}: identidade, estado atual,
 * o actor que causou o disparo e cada {@link Attempt} feita até agora. A
 * trilha de actor é inegociável em toda invocação (ver
 * {@code docs/API-DESIGN.md} §"Actor e regressão ergonômica assumida").
 * {@code firedAt} é {@code null} enquanto a execução ainda não disparou
 * (ex.: estado {@link ExecutionState#ENQUEUED}).
 */
public record Execution(
        ExecutionId id,
        JobKey jobKey,
        ExecutionState state,
        Instant scheduledAt,
        @Nullable Instant firedAt,
        List<Attempt> attempts,
        String actor) {

    public Execution {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
        if (actor.isBlank()) {
            throw new IllegalArgumentException("actor must not be blank");
        }
        attempts = List.copyOf(attempts); // cópia defensiva (Effective Java, Item 50)
    }
}
