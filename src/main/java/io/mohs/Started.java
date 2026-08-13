package io.mohs;

import java.time.Instant;
import java.util.Objects;

/** Uma tentativa começou a executar. */
public record Started(ExecutionId executionId, JobKey jobKey, int attempt, Instant firedAt)
        implements ExecutionEvent {

    public Started {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(firedAt, "firedAt");
    }
}
