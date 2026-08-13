package io.mohs.core.event;

import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/** A execução terminou com sucesso nesta tentativa. */
public record Succeeded(ExecutionId executionId, JobKey jobKey, int attempt) implements ExecutionEvent {

    public Succeeded {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
    }
}
