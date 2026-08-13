package io.mohs.core.event;

import java.util.Objects;

import io.mohs.core.ExecutionId;
import io.mohs.core.JobKey;
import io.mohs.core.execution.JobContext;

/** A execução foi cancelada cooperativamente (ver {@link JobContext#cancellationRequested()}). */
public record Cancelled(ExecutionId executionId, JobKey jobKey, int attempt) implements ExecutionEvent {

    public Cancelled {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
    }
}
