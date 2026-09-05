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
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;

/** The wire form of {@link Execution} — the detail view, attempts included. */
public record ExecutionResponse(
        String executionId,
        String jobKey,
        ExecutionState state,
        Instant scheduledAt,
        @Nullable Instant firedAt,
        String actor,
        List<AttemptResponse> attempts) {

    public ExecutionResponse {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
        attempts = List.copyOf(attempts);
    }

    public static ExecutionResponse from(Execution execution) {
        return new ExecutionResponse(
                execution.id().value(),
                execution.jobKey().value(),
                execution.state(),
                execution.scheduledAt(),
                execution.firedAt(),
                execution.actor(),
                execution.attempts().stream().map(AttemptResponse::from).toList());
    }
}
