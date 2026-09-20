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
package io.mohs.core.schedule;

import io.mohs.core.job.JobKey;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;

/**
 * Receipt of a recurring upsert, subject to commit of an enclosing transaction.
 * Use key with Mohs pause/resume/remove/findJob and ExecutionQuery; each occurrence gets its own execution ID.
 *
 * @param key           this named schedule's identity
 * @param executableKey the reusable executable's identity
 * @param recurrence    the stored schedule
 * @param paused        actual operational pause state
 * @param nextFireAt    stored trigger progress, or null when disarmed/awaiting fixed-delay completion
 */
public record RecurringSchedule(JobKey key, JobKey executableKey, Schedule recurrence, boolean paused, @Nullable Instant nextFireAt) {
    /**
     * Creates and validates this immutable value.
     *
     * @param key           this named schedule's identity
     * @param executableKey the reusable executable's identity
     * @param recurrence    the stored schedule
     * @param paused        actual operational pause state
     * @param nextFireAt    stored trigger progress, or null when disarmed/awaiting fixed-delay completion
     */
    public RecurringSchedule {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(executableKey, "executableKey");
        Objects.requireNonNull(recurrence, "recurrence");
    }
}
