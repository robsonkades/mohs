package io.mohs.rest.error;

import java.io.Serial;
import java.util.Objects;

import io.mohs.core.execution.ExecutionId;

/** Execução inexistente numa rota que espera um {@code executionId}/{@code id} válido. */
public final class ExecutionNotFoundException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ExecutionId executionId;

    public ExecutionNotFoundException(ExecutionId executionId) {
        super("Execution not found: " + executionId.value());
        this.executionId = Objects.requireNonNull(executionId, "executionId");
    }

    public ExecutionId executionId() {
        return executionId;
    }
}
