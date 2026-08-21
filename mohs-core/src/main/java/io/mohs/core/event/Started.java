package io.mohs.core.event;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/** Uma tentativa começou a executar. */
public record Started(ExecutionId executionId, JobKey jobKey, int attempt, Instant firedAt)
        implements ExecutionEvent {

    public Started {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(firedAt, "firedAt");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt is 1-based, must be >= 1");
        }
    }
}
