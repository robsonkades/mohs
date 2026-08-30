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
import java.util.ArrayList;
import java.util.List;

import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionTest {

    @Test
    void copiesAttemptsDefensively() {
        List<Attempt> mutable = new ArrayList<>();
        mutable.add(new Attempt(1, Instant.now(), null, ExecutionState.RUNNING, null));

        Execution execution = new Execution(
                ExecutionId.of("exec-1"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, mutable, "application");

        mutable.add(new Attempt(2, Instant.now(), null, ExecutionState.RUNNING, null));

        assertThat(execution.attempts()).hasSize(1);
    }

    @Test
    void attemptsIsImmutable() {
        Execution execution = new Execution(
                ExecutionId.of("exec-1"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, List.of(new Attempt(1, Instant.now(), null, ExecutionState.RUNNING, null)), "application");

        assertThatThrownBy(() -> execution.attempts().add(new Attempt(2, Instant.now(), null, ExecutionState.RUNNING, null)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsNullActor() {
        assertThatThrownBy(() -> new Execution(
                ExecutionId.of("exec-1"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsBlankActor() {
        assertThatThrownBy(() -> new Execution(
                ExecutionId.of("exec-1"), JobKey.of("job-1"), ExecutionState.RUNNING,
                Instant.now(), null, List.of(), " "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
