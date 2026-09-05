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
 * The execution failed for good. {@link #attemptsExhausted()} distinguishes "the retry policy ran
 * out" from other causes of terminal failure — the typical alerting hook, e.g.
 * {@code case Failed f when f.attemptsExhausted() -> alert(...)}.
 *
 * @param executionId the identity of the execution
 * @param jobKey the stable identity of the job
 * @param attempt the one-based attempt number
 * @param error the failure that ended the attempt
 * @param attemptsExhausted whether the retry policy refused further attempts because its budget was exhausted
 */
/*
 * A contract note that applies to this record and to AttemptFailed:
 *
 * equals()/hashCode() are IDENTITY-based on the `error` component, because Throwable does not
 * override them. Two events describing the same failure are never equals(): do not use this record
 * as a deduplication key — the identity of the fact is executionId + attempt.
 *
 * `error` is SHARED between all listeners and is mutable (addSuppressed/initCause/setStackTrace).
 * Treat it as read-only: mutating it corrupts what the other listeners are still going to observe.
 * There is no defensive copy for a Throwable, which is why the rule is written down rather than
 * enforced.
 *
 * The handler exception's MESSAGE travels from here into the log of anyone writing
 * log.info("{}", event). Execution deliberately does not carry the payload; that discipline is lost
 * through the error-message door, and whoever writes the handler needs to know it.
 */
public record Failed(ExecutionId executionId, JobKey jobKey, int attempt, Throwable error, boolean attemptsExhausted) implements ExecutionEvent {

    /**
     * Creates a {@code Failed} with the supplied values.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param attempt the one-based attempt number
     * @param error the failure that ended the attempt
     * @param attemptsExhausted whether the retry policy refused further attempts because its budget was exhausted
     */
    public Failed {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(error, "error");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt is 1-based, must be >= 1");
        }
    }
}
