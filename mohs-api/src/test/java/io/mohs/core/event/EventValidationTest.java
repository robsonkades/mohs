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
package io.mohs.core.event;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage of the validations on the eight {@link ExecutionEvent} types —
 * {@link ExecutionEventTest} covers the exhaustive switch's dispatch, not per-type validation.
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
