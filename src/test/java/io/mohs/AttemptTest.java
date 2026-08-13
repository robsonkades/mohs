package io.mohs;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AttemptTest {

    @Test
    void rejectsAttemptNumberBelowOne() {
        assertThatThrownBy(() -> new Attempt(0, Instant.now(), null, ExecutionState.RUNNING))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
