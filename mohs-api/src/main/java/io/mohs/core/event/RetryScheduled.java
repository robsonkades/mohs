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

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/**
 * A new attempt was scheduled after an {@link AttemptFailed}.
 *
 * @param executionId the identity of the execution
 * @param jobKey the stable identity of the job
 * @param nextAttempt the one-based number of the scheduled retry
 * @param retryAt the instant when the retry becomes visible to a claim
 */
public record RetryScheduled(ExecutionId executionId, JobKey jobKey, int nextAttempt, Instant retryAt) implements ExecutionEvent {

    /**
     * Creates a {@code RetryScheduled} with the supplied values.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param nextAttempt the one-based number of the scheduled retry
     * @param retryAt the instant when the retry becomes visible to a claim
     */
    public RetryScheduled {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(retryAt, "retryAt");
        if (nextAttempt < 1) {
            throw new IllegalArgumentException("nextAttempt is 1-based, must be >= 1");
        }
    }
}
