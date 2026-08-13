package io.mohs.event;

import java.util.Objects;

import io.mohs.ExecutionId;
import io.mohs.JobKey;

/** A execução terminou com sucesso nesta tentativa. */
public record Succeeded(ExecutionId executionId, JobKey jobKey, int attempt) implements ExecutionEvent {

    public Succeeded {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
    }
}
