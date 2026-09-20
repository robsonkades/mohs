/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.core.execution;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionTest {

    @Test
    void exposesTheCompletedAttemptSeparatelyFromCurrentExecutionState() {
        Instant now = Instant.parse("2026-09-08T12:00:00Z");
        var attempt = new Attempt(3, now.minusSeconds(1), now, ExecutionState.FAILED, "boom");
        var execution = new Execution(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-1"),
                ExecutionState.RETRY_WAITING, now, null, Optional.of(attempt), "application");
        assertThat(execution.lastAttempt()).contains(attempt);
        assertThat(execution.lastAttempt().orElseThrow().number()).isEqualTo(3);
        assertThat(execution.state()).isEqualTo(ExecutionState.RETRY_WAITING);
    }

    @Test
    void acceptsNoCompletedAttemptAndRejectsANullOptional() {
        Instant now = Instant.parse("2026-09-08T12:00:00Z");
        assertThat(Execution.enqueued(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-1"),
                now, "application", Priority.NORMAL).lastAttempt()).isEmpty();
        assertThatThrownBy(() -> new Execution(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-1"),
                ExecutionState.ENQUEUED, now, null, null, "application"))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("lastAttempt");
    }

    @Test
    void rejectsAnUnfinishedLastAttempt() {
        Instant now = Instant.parse("2026-09-08T12:00:00Z");
        assertThatThrownBy(() -> new Execution(ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-1"),
                ExecutionState.RUNNING, now, now,
                Optional.of(new Attempt(1, now, null, ExecutionState.RUNNING, null)), "application"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("completed attempt");
    }

    @Test
    void rejectsNullActor() {
        assertThatThrownBy(() -> new Execution(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, Optional.empty(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsBlankActor() {
        assertThatThrownBy(() -> new Execution(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, Optional.empty(), " "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
