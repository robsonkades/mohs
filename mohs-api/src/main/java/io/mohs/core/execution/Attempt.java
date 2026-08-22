package io.mohs.core.execution;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Uma única tentativa de uma {@link Execution}. Retry incrementa
 * {@link #number()}; o {@link ExecutionId} da execução dona nunca muda
 * entre tentativas. {@code finishedAt} é {@code null} enquanto a tentativa
 * ainda está em execução. {@code error} é a mensagem da falha, presente só
 * quando {@code outcome} é {@link ExecutionState#FAILED}.
 */
public record Attempt(int number, Instant startedAt, @Nullable Instant finishedAt, ExecutionState outcome, @Nullable String error) {

    public Attempt {
        if (number < 1) {
            throw new IllegalArgumentException("number is 1-based, must be >= 1");
        }
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(outcome, "outcome");
        if (outcome == ExecutionState.ENQUEUED || outcome == ExecutionState.RETRY_WAITING) {
            throw new IllegalArgumentException(
                    "outcome " + outcome + " does not describe a single attempt's result — it describes the owning Execution's state");
        }
        if ((outcome == ExecutionState.FAILED) != (error != null)) {
            throw new IllegalArgumentException("error must be present if and only if outcome is FAILED");
        }
    }
}
