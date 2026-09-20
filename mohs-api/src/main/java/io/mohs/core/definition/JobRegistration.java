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
package io.mohs.core.definition;

import io.mohs.core.job.JobRef;

import java.util.Objects;

/**
 * Declares reusable executable code. Expose this value as a Spring singleton bean:
 * the starter registers it before workers start. Creating the value alone has no effect.
 * The action stays local; only the stable job key and invocation data are persisted.
 *
 * @param <T>     the input contract
 * @param job     the shared typed reference used by callers
 * @param action  the application action, typically calling an injected service
 * @param options the reusable execution policy
 */
public record JobRegistration<T>(JobRef<T> job, JobAction<T> action, JobOptions options) {
    /**
     * Creates and validates this immutable value.
     *
     * @param job     the shared typed reference used by callers
     * @param action  the application action, typically calling an injected service
     * @param options the reusable execution policy
     */
    public JobRegistration {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(options, "options");
    }

    /**
     * Declares an on-demand executable with default policy; no invocation is created.
     *
     * @param <T>    the input contract
     * @param job    the shared typed reference used by callers
     * @param action the application action, typically calling an injected service
     * @return the executable declaration; no work is submitted
     */
    public static <T> JobRegistration<T> of(JobRef<T> job, JobAction<T> action) {
        return new JobRegistration<>(job, action, JobOptions.defaults());
    }

    /**
     * Returns a declaration with the supplied complete policy.
     *
     * @param options the reusable execution policy
     * @return a new value with the requested change
     */
    public JobRegistration<T> withOptions(JobOptions options) {
        return new JobRegistration<>(job, action, options);
    }
}
