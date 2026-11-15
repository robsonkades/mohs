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

import java.time.Duration;
import java.util.Objects;

import io.mohs.core.TechnicalKey;
import io.mohs.core.TimePrecision;
import org.jspecify.annotations.Nullable;

/**
 * Immutable execution policy. Use {@link #defaults()} and named copy methods.
 * A concurrency ceiling of zero means unlimited; one prevents overlap.
 * Retry, timeout and resource policy belong to the job, not an individual submission.
 *
 * @param name                    optional display label
 * @param runner                  named runner, or null for the default
 * @param window                  named execution window, or null for unrestricted
 * @param rateLimit               named shared rate limit, or null for none
 * @param retries                 retries beyond the first attempt
 * @param timeout                 positive attempt timeout, or null for no job-specific timeout
 * @param retryPolicy             custom retry-policy bean name, or null for the built-in policy
 * @param maxConcurrentExecutions nonnegative per-job ceiling; zero means unlimited
 */
public record JobOptions(
        @Nullable String name,
        @Nullable String runner,
        @Nullable String window,
        @Nullable String rateLimit,
        int retries,
        @Nullable Duration timeout,
        @Nullable String retryPolicy,
        int maxConcurrentExecutions) {
    /**
     * Creates and validates this immutable value.
     *
     * @param name                    optional display label
     * @param runner                  named runner, or null for the default
     * @param window                  named execution window, or null for unrestricted
     * @param rateLimit               named shared rate limit, or null for none
     * @param retries                 retries beyond the first attempt
     * @param timeout                 positive attempt timeout, or null for no job-specific timeout
     * @param retryPolicy             custom retry-policy bean name, or null for the built-in policy
     * @param maxConcurrentExecutions nonnegative per-job ceiling; zero means unlimited
     */
    public JobOptions {
        checkText(name, "name");
        if (runner != null) TechnicalKey.require(runner, "runner");
        if (window != null) TechnicalKey.require(window, "window");
        if (rateLimit != null) TechnicalKey.require(rateLimit, "rateLimit");
        if (retryPolicy != null) TechnicalKey.require(retryPolicy, "retryPolicy");
        if (retries < 0) throw new IllegalArgumentException("retries must not be negative");

        if (maxConcurrentExecutions < 0) {
            throw new IllegalArgumentException("maxConcurrentExecutions must not be negative; 0 means unlimited");
        }

        if (timeout != null) TimePrecision.micros(timeout);

        if (timeout != null && !timeout.isPositive()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    private static void checkText(@Nullable String value, String field) {
        if (value != null && (value.isBlank() || value.length() > 255)) {
            throw new IllegalArgumentException(field + " must be nonblank and at most 255 characters");
        }
    }

    /**
     * Default runner, one retry, no own timeout or resource restrictions, unlimited concurrency.
     *
     * @return the default options
     */
    public static JobOptions defaults() {
        return new JobOptions(null, null, null, null, 1, null, null, 0);
    }

    /**
     * Returns a copy with the supplied name.
     *
     * @param name nonblank display label
     * @return a new value with the requested change
     */
    public JobOptions withName(String name) {
        Objects.requireNonNull(name, "name");
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

    /**
     * Returns a copy with the supplied runner.
     *
     * @param runner nonblank name of a configured runner
     * @return a new value with the requested change
     */
    public JobOptions withRunner(String runner) {
        Objects.requireNonNull(runner, "runner");
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

    /**
     * Returns a copy with the supplied window.
     *
     * @param window nonblank name of a configured execution window
     * @return a new value with the requested change
     */
    public JobOptions withWindow(String window) {
        Objects.requireNonNull(window, "window");
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

    /**
     * Returns a copy with the supplied rateLimit.
     *
     * @param rateLimit nonblank name of a configured shared rate limit
     * @return a new value with the requested change
     */
    public JobOptions withRateLimit(String rateLimit) {
        Objects.requireNonNull(rateLimit, "rateLimit");
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

    /**
     * Returns a copy with the supplied retries.
     *
     * @param retries retries beyond the first attempt
     * @return a new value with the requested change
     */
    public JobOptions withRetries(int retries) {
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

    /**
     * Returns a copy with the supplied timeout.
     *
     * @param timeout non-null positive attempt timeout
     * @return a new value with the requested change
     */
    public JobOptions withTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

    /**
     * Returns a copy with the supplied retryPolicy.
     *
     * @param retryPolicy nonblank name of a custom retry-policy bean
     * @return a new value with the requested change
     */
    public JobOptions withRetryPolicy(String retryPolicy) {
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

    /**
     * Returns a copy with the supplied maxConcurrentExecutions.
     *
     * @param maxConcurrentExecutions nonnegative per-job ceiling; zero means unlimited
     * @return a new value with the requested change
     */
    public JobOptions withMaxConcurrentExecutions(int maxConcurrentExecutions) {
        return new JobOptions(name, runner, window, rateLimit, retries, timeout, retryPolicy, maxConcurrentExecutions);
    }

}
