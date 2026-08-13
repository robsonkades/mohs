package io.mohs.execution;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.mohs.ExecutionId;
import io.mohs.JobKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionTest {

    @Test
    void copiesAttemptsDefensively() {
        List<Attempt> mutable = new ArrayList<>();
        mutable.add(new Attempt(1, Instant.now(), null, ExecutionState.RUNNING));

        Execution execution = new Execution(
                ExecutionId.of("exec-1"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, mutable, "application");

        mutable.add(new Attempt(2, Instant.now(), null, ExecutionState.RUNNING));

        assertThat(execution.attempts()).hasSize(1);
    }

    @Test
    void rejectsNullActor() {
        assertThatThrownBy(() -> new Execution(
                ExecutionId.of("exec-1"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
