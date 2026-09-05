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

import io.mohs.core.schedule.Misfire;

/**
 * The stage of {@link JobSpec} reached once a trigger has been chosen. It carries every job policy
 * that is not the trigger itself — runner, window, misfire, retries, timeout — each optional and
 * settable in any order.
 */
public sealed interface PolicySpec permits JobSpecImpl {

    /**
     * Selects the named runner for this job.
     *
     * @param name the name of the declared runner
     * @return this configuration stage for further customization
     */
    PolicySpec runner(String name);

    /**
     * Applies the named firing exclusion window.
     *
     * @param name the name of the declared execution window
     * @return this configuration stage for further customization
     */
    PolicySpec window(String name);

    /**
     * The name of the {@code RateLimit} bounding this job's firing rate, cluster-wide.
     *
     * @param name the name of the declared rate limit
     * @return this configuration stage for further customization
     */
    PolicySpec rateLimit(String name);

    /**
     * Selects how missed automatic firings are handled.
     *
     * @param policy the policy for handling missed firings
     * @return this configuration stage for further customization
     */
    PolicySpec misfire(Misfire policy);

    /**
     * Born paused on the FIRST registration of the definition: the schedule is declared but
     * disarmed until a {@code resume}; manual on-demand execution still works while paused. After
     * birth, {@code paused} is an operator decision — a redeploy never re-pauses.
     *
     * @return this configuration stage for further customization
     */
    PolicySpec startPaused();

    /**
     * Prevents more than one execution of this job from being {@code RUNNING} at the same time.
     *
     * <p>The default is to allow concurrency: most jobs are invoked many times with independent
     * payloads (an email job, one invocation per recipient), and those invocations have no reason
     * to serialise against each other merely for sharing a {@code job_key}.
     *
     * <p>Use this for the narrower case: a cron or interval job whose own next firing may occur
     * before the previous one finishes (a synchronisation that sometimes takes longer than its
     * interval, say) — there the two "executions" are the same task overlapping, not independent
     * work. This matches Quartz's {@code @DisallowConcurrentExecution} default, which is likewise
     * opt-in rather than opt-out.
     *
     * @return this configuration stage for further customization
     */
    PolicySpec preventOverlap();

    /**
     * Like {@link #preventOverlap()}, but with an explicit ceiling above 1 rather than full mutual
     * exclusion — a report whose handler shares an external resource with its own per-{@code job_key}
     * capacity, and may run up to N instances at once but never N+1.
     *
     * @param max the positive ceiling on concurrent executions of this job
     * @return this configuration stage for further customization
     */
    PolicySpec maxConcurrentExecutions(int max);

    /**
     * Attempts BEYOND the first. Defaults to 1 — see {@link MohsJob#retries()} for why it is not zero.
     *
     * @param max the nonnegative retry count beyond the first attempt
     * @return this configuration stage for further customization
     */
    PolicySpec retries(int max);

    /**
     * Sets the per-attempt execution timeout.
     *
     * @param timeout the maximum duration allowed for an attempt
     * @return this configuration stage for further customization
     */
    PolicySpec timeout(Duration timeout);

    /**
     * The bean name of a custom retry policy, for cases {@link #retries(int)} cannot express.
     *
     * @param beanName the name of the custom retry-policy bean
     * @return this configuration stage for further customization
     */
    PolicySpec retryPolicy(String beanName);
}
