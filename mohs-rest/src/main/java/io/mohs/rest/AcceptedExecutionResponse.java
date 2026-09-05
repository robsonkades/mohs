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
package io.mohs.rest;

import java.time.Instant;
import java.util.Objects;

import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;

/**
 * The {@code 202 Accepted} receipt of every invocation (schedule, retry) — the flat form of
 * {@link Enqueued} used at the HTTP boundary: identities cross as {@code String}, never
 * {@code JobKey}/{@code ExecutionId} directly, as in the {@code POST .../schedule} example.
 *
 * @param executionId the identity of the execution
 * @param jobKey the stable identity of the job
 * @param scheduledAt the intended firing instant
 * @param actor the identity attributed to the operation
 */
public record AcceptedExecutionResponse(String executionId, String jobKey, Instant scheduledAt, String actor) {

    /**
     * Creates a {@code AcceptedExecutionResponse} with the supplied values.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param scheduledAt the intended firing instant
     * @param actor the identity attributed to the operation
     */
    public AcceptedExecutionResponse {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
    }

    /**
     * Converts the supplied snapshot to its REST representation.
     *
     * @param enqueued the durable scheduling receipt
     * @return the corresponding REST representation
     */
    public static AcceptedExecutionResponse from(Enqueued enqueued) {
        return new AcceptedExecutionResponse(
                enqueued.executionId().value(), enqueued.jobKey().value(), enqueued.scheduledAt(), enqueued.actor());
    }

    /**
     * The 202 form of a manual retry — the SAME execution rearmed; the actor is the original invocation's, since nothing new is inserted.
     *
     * @param execution the execution to represent
     * @return the corresponding REST representation
     */
    public static AcceptedExecutionResponse from(Execution execution) {
        return new AcceptedExecutionResponse(
                execution.id().value(), execution.jobKey().value(), execution.scheduledAt(), execution.actor());
    }
}
