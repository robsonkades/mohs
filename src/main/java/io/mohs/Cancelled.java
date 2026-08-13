package io.mohs;

import java.util.Objects;

/** A execução foi cancelada cooperativamente (ver {@link JobContext#cancellationRequested()}). */
public record Cancelled(ExecutionId executionId, JobKey jobKey, int attempt) implements ExecutionEvent {

    public Cancelled {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
    }
}
