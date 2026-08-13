package io.mohs;

import java.util.Objects;

/** A execução terminou com sucesso nesta tentativa. */
public record Succeeded(ExecutionId executionId, JobKey jobKey, int attempt) implements ExecutionEvent {

    public Succeeded {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
    }
}
