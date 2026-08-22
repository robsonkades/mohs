package io.mohs.core.execution;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AttemptTest {

    @Test
    void rejectsAttemptNumberBelowOne() {
        assertThatThrownBy(() -> new Attempt(0, Instant.now(), null, ExecutionState.RUNNING, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullStartedAt() {
        assertThatThrownBy(() -> new Attempt(1, null, null, ExecutionState.RUNNING, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullOutcome() {
        assertThatThrownBy(() -> new Attempt(1, Instant.now(), null, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsEnqueuedAndRetryScheduledAsOutcome() {
        assertThatThrownBy(() -> new Attempt(1, Instant.now(), null, ExecutionState.ENQUEUED, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Attempt(1, Instant.now(), null, ExecutionState.RETRY_WAITING, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsErrorPresentWhenOutcomeIsNotFailed() {
        assertThatThrownBy(() -> new Attempt(1, Instant.now(), Instant.now(), ExecutionState.SUCCEEDED, "boom"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissingErrorWhenOutcomeIsFailed() {
        assertThatThrownBy(() -> new Attempt(1, Instant.now(), Instant.now(), ExecutionState.FAILED, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
