package io.mohs.core.event;

import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/**
 * A execução falhou definitivamente. {@link #attemptsExhausted()} distingue
 * "esgotou a política de retry" de outras causas de falha terminal — é o
 * gancho típico de alerta, ex.:
 * {@code case Failed f when f.attemptsExhausted() -> alert(...)}.
 */
public record Failed(ExecutionId executionId, JobKey jobKey, int attempt, Throwable error, boolean attemptsExhausted) implements ExecutionEvent {

    public Failed {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(error, "error");
    }
}
