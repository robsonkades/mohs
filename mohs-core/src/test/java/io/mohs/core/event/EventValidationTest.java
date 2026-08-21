package io.mohs.core.event;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cobertura das validações adicionadas aos 8 tipos de {@link ExecutionEvent}
 * (achados API-9/API-10 do code review) — {@link ExecutionEventTest} cobre o
 * dispatch do switch exaustivo, não validação por tipo.
 */
class EventValidationTest {

    private static final ExecutionId ID = ExecutionId.of("exec-1");
    private static final JobKey KEY = JobKey.of("welcome-email");

    @Test
    void enqueuedRejectsBlankActor() {
        assertThatThrownBy(() -> new Enqueued(ID, KEY, Instant.now(), " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void startedRejectsAttemptBelowOne() {
        assertThatThrownBy(() -> new Started(ID, KEY, 0, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void succeededRejectsAttemptBelowOne() {
        assertThatThrownBy(() -> new Succeeded(ID, KEY, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cancelledRejectsAttemptBelowOne() {
        assertThatThrownBy(() -> new Cancelled(ID, KEY, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void attemptFailedRejectsAttemptBelowOne() {
        assertThatThrownBy(() -> new AttemptFailed(ID, KEY, 0, new RuntimeException("boom")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failedRejectsAttemptBelowOne() {
        assertThatThrownBy(() -> new Failed(ID, KEY, 0, new RuntimeException("boom"), false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retryScheduledRejectsNextAttemptBelowOne() {
        assertThatThrownBy(() -> new RetryScheduled(ID, KEY, 0, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
