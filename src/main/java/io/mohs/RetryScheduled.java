package io.mohs;

import java.time.Instant;
import java.util.Objects;

/** Uma nova tentativa foi agendada após {@link AttemptFailed}. */
public record RetryScheduled(ExecutionId executionId, JobKey jobKey, int nextAttempt, Instant retryAt)
        implements ExecutionEvent {

    public RetryScheduled {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(retryAt, "retryAt");
    }
}
