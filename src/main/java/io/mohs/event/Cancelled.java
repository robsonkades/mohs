package io.mohs.event;

import java.util.Objects;

import io.mohs.ExecutionId;
import io.mohs.JobKey;
import io.mohs.execution.JobContext;

/** A execução foi cancelada cooperativamente (ver {@link JobContext#cancellationRequested()}). */
public record Cancelled(ExecutionId executionId, JobKey jobKey, int attempt) implements ExecutionEvent {

    public Cancelled {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
    }
}
