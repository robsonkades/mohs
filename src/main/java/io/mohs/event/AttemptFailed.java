package io.mohs.event;

import java.util.Objects;

import io.mohs.ExecutionId;
import io.mohs.JobKey;

/** Uma tentativa falhou; o retry (se a política ainda tiver saldo) segue em {@link RetryScheduled}. */
public record AttemptFailed(ExecutionId executionId, JobKey jobKey, int attempt, Throwable error)
        implements ExecutionEvent {

    public AttemptFailed {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(error, "error");
    }
}
