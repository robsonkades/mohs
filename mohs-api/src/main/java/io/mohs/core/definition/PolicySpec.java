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

    PolicySpec runner(String name);

    PolicySpec window(String name);

    /** The name of the {@code RateLimit} bounding this job's firing rate, cluster-wide. */
    PolicySpec rateLimit(String name);

    PolicySpec misfire(Misfire policy);

    /**
     * Born paused on the FIRST registration of the definition: the schedule is declared but
     * disarmed until a {@code resume}; manual on-demand execution still works while paused. After
     * birth, {@code paused} is an operator decision — a redeploy never re-pauses.
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
     */
    PolicySpec preventOverlap();

    /**
     * Like {@link #preventOverlap()}, but with an explicit ceiling above 1 rather than full mutual
     * exclusion — a report whose handler shares an external resource with its own per-{@code job_key}
     * capacity, and may run up to N instances at once but never N+1.
     */
    PolicySpec maxConcurrentExecutions(int max);

    /** Attempts BEYOND the first. Defaults to 1 — see {@link MohsJob#retries()} for why it is not zero. */
    PolicySpec retries(int max);

    PolicySpec timeout(Duration timeout);

    /** The bean name of a custom retry policy, for cases {@link #retries(int)} cannot express. */
    PolicySpec retryPolicy(String beanName);
}
