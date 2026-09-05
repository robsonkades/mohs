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
package io.mohs.rest.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.mohs.core.JobSnapshot;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.schedule.Misfire;

/**
 * The wire form of {@link JobDefinition} — {@code paused} and {@code nextFireAt} are operational
 * state with no counterpart in the definition itself (which models only static configuration); they
 * come from {@link JobSnapshot}.
 *
 * @param jobKey the stable identity of the job
 * @param name the human-readable name
 * @param handlerType the fully qualified handler class name
 * @param schedule the firing schedule representation
 * @param runner the selected runner name, or {@code null} for the default
 * @param window the exclusion-window name, or {@code null} for no exclusion
 * @param rateLimit the rate-limit name, or {@code null} for no throughput limit
 * @param misfire the policy for missed automatic firings
 * @param retries the allowed retry attempts beyond the first
 * @param timeout the per-attempt timeout, or {@code null} for no job-specific timeout
 * @param retryPolicy the custom retry-policy bean name, or {@code null} for the default
 * @param source the origin of the definition
 * @param paused whether automatic firing is suspended
 * @param nextFireAt the next automatic firing instant, or {@code null} when disarmed
 */
public record JobResponse(
        String jobKey,
        String name,
        String handlerType,
        ScheduleView schedule,
        @Nullable String runner,
        @Nullable String window,
        @Nullable String rateLimit,
        Misfire misfire,
        int retries,
        @Nullable Duration timeout,
        @Nullable String retryPolicy,
        DefinitionSource source,
        boolean paused,
        @Nullable Instant nextFireAt) {

    /**
     * Creates a {@code JobResponse} with the supplied values.
     *
     * @param jobKey the stable identity of the job
     * @param name the human-readable name
     * @param handlerType the fully qualified handler class name
     * @param schedule the firing schedule representation
     * @param runner the selected runner name, or {@code null} for the default
     * @param window the exclusion-window name, or {@code null} for no exclusion
     * @param rateLimit the rate-limit name, or {@code null} for no throughput limit
     * @param misfire the policy for missed automatic firings
     * @param retries the allowed retry attempts beyond the first
     * @param timeout the per-attempt timeout, or {@code null} for no job-specific timeout
     * @param retryPolicy the custom retry-policy bean name, or {@code null} for the default
     * @param source the origin of the definition
     * @param paused whether automatic firing is suspended
     * @param nextFireAt the next automatic firing instant, or {@code null} when disarmed
     */
    public JobResponse {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(handlerType, "handlerType");
        Objects.requireNonNull(schedule, "schedule");
        Objects.requireNonNull(misfire, "misfire");
        Objects.requireNonNull(source, "source");
    }

    /**
     * {@code name} falls back to the {@code jobKey}'s value when no human-readable label was set.
     *
     * @param snapshot the current operational snapshot
     * @return the corresponding REST representation
     */
    public static JobResponse from(JobSnapshot snapshot) {
        JobDefinition definition = snapshot.definition();
        String name = definition.name() != null ? definition.name() : definition.key().value();
        return new JobResponse(
                definition.key().value(),
                name,
                definition.handlerType().getName(),
                ScheduleView.from(definition.schedule()),
                definition.runner(),
                definition.window(),
                definition.rateLimit(),
                definition.misfire(),
                definition.retries(),
                definition.timeout(),
                definition.retryPolicy(),
                definition.source(),
                snapshot.paused(),
                snapshot.nextFireAt());
    }
}
