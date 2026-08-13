package io.mohs;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Um único disparo de um {@link JobDefinition}: identidade, estado atual,
 * o actor que causou o disparo e cada {@link Attempt} feita até agora. A
 * trilha de actor é inegociável em toda invocação (ver
 * {@code docs/API-DESIGN.md} §"Actor e regressão ergonômica assumida").
 */
public record Execution(
        ExecutionId id,
        JobKey jobKey,
        ExecutionState state,
        Instant scheduledAt,
        Instant firedAt,
        List<Attempt> attempts,
        String actor) {

    public Execution {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
        attempts = List.copyOf(attempts); // cópia defensiva (Effective Java, Item 50)
    }
}
