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
import io.mohs.core.ScheduleCommand;

/**
 * A deliberate dual role: it is both the receipt returned by {@link ScheduleCommand}'s terminals
 * ({@code now}/{@code at}/{@code after}) and the corresponding {@link ExecutionEvent} variant —
 * "the return value is a receipt, never a {@code Future} of the result" already describes both
 * roles with the same data.
 */
public record Enqueued(ExecutionId executionId, JobKey jobKey, Instant scheduledAt, String actor) implements ExecutionEvent {

    public Enqueued {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(actor, "actor");
        if (actor.isBlank()) {
            throw new IllegalArgumentException("actor must not be blank");
        }
    }
}
