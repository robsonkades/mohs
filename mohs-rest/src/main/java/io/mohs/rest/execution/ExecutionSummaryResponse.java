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

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionState;

/**
 * The LIST form of {@link Execution} — a summary without {@code attempts}, which belong to the
 * detail view ({@code GET /executions/{id}}, {@link ExecutionResponse}), as the REST design's table
 * always said.
 *
 * <p>What the list stops carrying is not only volume: an attempt's {@code error} is arbitrarily
 * long text that used to travel on every page and on every tick of the dashboard's stream.
 */
public record ExecutionSummaryResponse(
        String executionId,
        String jobKey,
        ExecutionState state,
        Instant scheduledAt,
        @Nullable Instant firedAt,
        String actor) {

    public ExecutionSummaryResponse {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
    }

    public static ExecutionSummaryResponse from(Execution execution) {
        return new ExecutionSummaryResponse(
                execution.id().value(),
                execution.jobKey().value(),
                execution.state(),
                execution.scheduledAt(),
                execution.firedAt(),
                execution.actor());
    }
}
