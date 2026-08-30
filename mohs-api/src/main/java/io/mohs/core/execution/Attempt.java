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

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * A single attempt of an {@link Execution}.
 *
 * <p>A retry increments {@link #number()}; the owning execution's {@link ExecutionId} never changes
 * between attempts. {@code finishedAt} is {@code null} while the attempt is still running, and
 * {@code error} is the failure message, present only when {@code outcome} is
 * {@link ExecutionState#FAILED}.
 */
public record Attempt(int number, Instant startedAt, @Nullable Instant finishedAt, ExecutionState outcome, @Nullable String error) {

    public Attempt {
        if (number < 1) {
            throw new IllegalArgumentException("number is 1-based, must be >= 1");
        }
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(outcome, "outcome");
        if (outcome == ExecutionState.ENQUEUED || outcome == ExecutionState.RETRY_WAITING) {
            throw new IllegalArgumentException(
                    "outcome " + outcome + " does not describe a single attempt's result — it describes the owning Execution's state");
        }
        if ((outcome == ExecutionState.FAILED) != (error != null)) {
            throw new IllegalArgumentException("error must be present if and only if outcome is FAILED");
        }
    }
}
