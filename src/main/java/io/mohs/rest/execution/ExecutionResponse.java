package io.mohs.rest.execution;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.ExecutionState;

/** Forma de wire de {@link io.mohs.core.execution.Execution}. */
public record ExecutionResponse(
        String executionId,
        String jobKey,
        ExecutionState state,
        Instant scheduledAt,
        @Nullable Instant firedAt,
        String actor,
        List<AttemptResponse> attempts) {

    public ExecutionResponse {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
        attempts = List.copyOf(attempts);
    }
}
