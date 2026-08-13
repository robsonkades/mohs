package io.mohs.core.event;

import java.util.Objects;

import io.mohs.core.ExecutionId;
import io.mohs.core.JobKey;

/** A execução terminou com sucesso nesta tentativa. */
public record Succeeded(ExecutionId executionId, JobKey jobKey, int attempt) implements ExecutionEvent {

    public Succeeded {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
    }
}
