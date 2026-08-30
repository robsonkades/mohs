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
package io.mohs.rest.execution;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.ExecutionState;

/** Forma de wire de {@link Attempt}. */
public record AttemptResponse(int number, Instant startedAt, @Nullable Instant finishedAt, ExecutionState outcome, @Nullable String error) {

    public AttemptResponse {
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(outcome, "outcome");
    }

    public static AttemptResponse from(Attempt attempt) {
        return new AttemptResponse(attempt.number(), attempt.startedAt(), attempt.finishedAt(), attempt.outcome(), attempt.error());
    }
}
