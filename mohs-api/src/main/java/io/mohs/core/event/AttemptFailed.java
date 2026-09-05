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

import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/**
 * An attempt failed; the retry, if the policy still has budget, follows in {@link RetryScheduled}.
 *
 * @param executionId the identity of the execution
 * @param jobKey the stable identity of the job
 * @param attempt the one-based attempt number
 * @param error the failure that ended the attempt
 */
public record AttemptFailed(ExecutionId executionId, JobKey jobKey, int attempt, Throwable error)
        implements ExecutionEvent {

    /**
     * Creates a {@code AttemptFailed} with the supplied values.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param attempt the one-based attempt number
     * @param error the failure that ended the attempt
     */
    public AttemptFailed {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(error, "error");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt is 1-based, must be >= 1");
        }
    }
}
