package io.mohs.rest.execution;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionState;

/** Forma de wire de {@link Attempt}. */
public record AttemptResponse(int number, Instant startedAt, @Nullable Instant finishedAt, ExecutionState outcome, @Nullable String error) {

    public AttemptResponse {
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(outcome, "outcome");
    }

    public static AttemptResponse from(Attempt attempt) {
        return new AttemptResponse(attempt.number(), attempt.startedAt(), attempt.finishedAt(), attempt.outcome(), attempt.error());
    }
}
