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
