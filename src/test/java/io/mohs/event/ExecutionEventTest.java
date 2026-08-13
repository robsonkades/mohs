package io.mohs.event;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import io.mohs.ExecutionId;
import io.mohs.JobKey;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionEventTest {

    /**
     * O switch abaixo é exaustivo por construção — se alguém adicionar uma
     * variante nova a {@link ExecutionEvent} sem tratá-la aqui, o build
     * quebra na compilação, não em runtime. Esse é o comportamento que este
     * teste protege.
     */
    private static String describe(ExecutionEvent event) {
        return switch (event) {
            case Enqueued e -> "enqueued:" + e.jobKey().value();
            case Started s -> "started:" + s.attempt();
            case AttemptFailed f -> "attempt-failed:" + f.attempt();
            case RetryScheduled r -> "retry-scheduled:" + r.nextAttempt();
            case Succeeded s -> "succeeded:" + s.attempt();
            case Failed f -> "failed:" + f.attemptsExhausted();
            case Cancelled c -> "cancelled:" + c.attempt();
            case BatchCompleted b -> "batch-completed:" + b.batchId();
        };
    }

    @Test
    void dispatchesEachVariant() {
        Instant now = Instant.now();
        ExecutionId id = ExecutionId.of("exec-1");
        JobKey key = JobKey.of("welcome-email");

        assertThat(describe(new Enqueued(id, key, now, "application"))).isEqualTo("enqueued:welcome-email");
        assertThat(describe(new Failed(id, key, 3, new RuntimeException("boom"), true)))
                .isEqualTo("failed:true");
        assertThat(describe(new BatchCompleted("batch-1", 1, 1, 0))).isEqualTo("batch-completed:batch-1");
    }
}
