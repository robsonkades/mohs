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

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionEventTest {

    /**
     * The switch below is exhaustive by construction: adding a new variant to
     * {@link ExecutionEvent} without handling it here breaks the build at compile time, not at
     * runtime. That is the behaviour this test protects.
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
        assertThat(describe(new BatchCompleted("batch-1", "n", 1, 1, 0))).isEqualTo("batch-completed:batch-1");
    }
}
