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

import java.util.Objects;

import org.jspecify.annotations.Nullable;
import io.mohs.core.definition.JobOptions;

/**
 * Immutable options for a named recurrence. A supplied job policy is a complete replacement.
 *
 * @param jobOptions  explicit execution policy, or null to copy the executable's current policy
 * @param misfire     missed-firing policy
 * @param startPaused whether the recurrence is born paused; updates preserve operator state
 */
public record RecurringOptions(@Nullable JobOptions jobOptions, Misfire misfire, boolean startPaused) {
    /**
     * Creates and validates this immutable value.
     *
     * @param jobOptions  explicit execution policy, or null to copy the executable's current policy
     * @param misfire     missed-firing policy
     * @param startPaused whether the recurrence is born paused; updates preserve operator state
     */
    public RecurringOptions {
        Objects.requireNonNull(misfire, "misfire");
    }

    /**
     * Copies the executable's policy, ignores missed firings and starts unpaused.
     *
     * @return the default options
     */
    public static RecurringOptions defaults() {
        return new RecurringOptions(null, Misfire.IGNORE, false);
    }

    /**
     * Returns a copy using this complete execution policy.
     *
     * @param options the complete immutable policy
     * @return a new value with the requested change
     */
    public RecurringOptions withJobOptions(JobOptions options) {
        return new RecurringOptions(Objects.requireNonNull(options, "options"), misfire, startPaused);
    }

    /**
     * Returns a copy with the supplied missed-firing policy.
     *
     * @param misfire missed-firing policy
     * @return a new value with the requested change
     */
    public RecurringOptions withMisfire(Misfire misfire) {
        return new RecurringOptions(jobOptions, misfire, startPaused);
    }

    /**
     * Returns a copy with the supplied initial pause decision.
     *
     * @param startPaused whether the recurrence is born paused; updates preserve operator state
     * @return a new value with the requested change
     */
    public RecurringOptions withStartPaused(boolean startPaused) {
        return new RecurringOptions(jobOptions, misfire, startPaused);
    }
}
