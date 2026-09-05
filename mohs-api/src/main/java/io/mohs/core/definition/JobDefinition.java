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
import java.util.function.Consumer;

import io.mohs.core.job.JobKey;
import io.mohs.core.Mohs;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.Schedule;

import org.jspecify.annotations.Nullable;

/**
 * A job, defined once: handler, schedule and policies. It is invoked in N ways (cron,
 * {@link Mohs#schedule}, {@link Mohs#batch}, the dashboard) — and none of them redefines policy.
 *
 * <p>Use {@link #of(String, Class, Consumer)} to build one programmatically (dynamic, data-driven
 * schedules — per-tenant registration, for instance). In a typical Spring application you do not
 * call that directly: the starter translates each method annotated with {@link MohsJob @MohsJob}
 * into exactly one of these at boot.
 *
 * @param key the job's stable identity — the upsert key, which never changes across redeploys of the same job
 * @param name an optional human-readable label, with no role in identity or scheduling
 * @param handlerType the class that processes each execution of this job
 * @param schedule when the job fires — {@code CronSpec}/{@code IntervalSpec}/{@code OnDemandSpec}
 * @param runner the name of the {@code MohsRunner} that runs the invocations; {@code null} uses the default runner
 * @param window the name of the {@code ExecutionWindow} restricting when the job may run; {@code null} means unrestricted
 * @param rateLimit the name of the {@code RateLimit} bounding this job's firing rate cluster-wide; {@code null} means no limit
 * @param misfire the policy applied when a firing is missed
 * @param startPaused born paused on the FIRST registration of the definition — the schedule is declared but disarmed until a {@code resume}; after birth, {@code paused} is an operator decision and a redeploy never re-pauses
 * @param allowConcurrentExecutions when {@code true}, there is no ceiling on concurrent executions of this job — {@code maxConcurrentExecutions} must then be {@code 0}
 * @param maxConcurrentExecutions the ceiling on concurrent executions when {@code allowConcurrentExecutions} is {@code false} — at least {@code 1}
 * @param retries the number of attempts beyond the first failure
 * @param timeout the maximum time per attempt; {@code null} means no timeout of its own (the cluster-wide Watchdog Bound, if configured, still applies)
 * @param retryPolicy the name of a Spring bean holding a custom retry policy, for cases {@code retries} cannot express; {@code null} uses the default policy
 * @param source {@code ANNOTATION} (through {@link MohsJob}) or {@code PROGRAMMATIC} (through {@link #of})
 */
public record JobDefinition(
        JobKey key,
        @Nullable String name,
        Class<?> handlerType,
        Schedule schedule,
        @Nullable String runner,
        @Nullable String window,
        @Nullable String rateLimit,
        Misfire misfire,
        boolean startPaused,
        boolean allowConcurrentExecutions,
        int maxConcurrentExecutions,
        int retries,
        @Nullable Duration timeout,
        @Nullable String retryPolicy,
        DefinitionSource source) {

    /**
     * Creates a {@code JobDefinition} with the supplied values.
     *
     * @param key the job's stable identity — the upsert key, which never changes across redeploys of the same job
     * @param name an optional human-readable label, with no role in identity or scheduling
     * @param handlerType the class that processes each execution of this job
     * @param schedule when the job fires — {@code CronSpec}/{@code IntervalSpec}/{@code OnDemandSpec}
     * @param runner the name of the {@code MohsRunner} that runs the invocations; {@code null} uses the default runner
     * @param window the name of the {@code ExecutionWindow} restricting when the job may run; {@code null} means unrestricted
     * @param rateLimit the name of the {@code RateLimit} bounding this job's firing rate cluster-wide; {@code null} means no limit
     * @param misfire the policy applied when a firing is missed
     * @param startPaused born paused on the FIRST registration of the definition — the schedule is declared but disarmed until a {@code resume}; after birth, {@code paused} is an operator decision and a redeploy never re-pauses
     * @param allowConcurrentExecutions when {@code true}, there is no ceiling on concurrent executions of this job — {@code maxConcurrentExecutions} must then be {@code 0}
     * @param maxConcurrentExecutions the ceiling on concurrent executions when {@code allowConcurrentExecutions} is {@code false} — at least {@code 1}
     * @param retries the number of attempts beyond the first failure
     * @param timeout the maximum time per attempt; {@code null} means no timeout of its own (the cluster-wide Watchdog Bound, if configured, still applies)
     * @param retryPolicy the name of a Spring bean holding a custom retry policy, for cases {@code retries} cannot express; {@code null} uses the default policy
     * @param source {@code ANNOTATION} (through {@link MohsJob}) or {@code PROGRAMMATIC} (through {@link #of})
     */
    public JobDefinition {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(handlerType, "handlerType");
        Objects.requireNonNull(schedule, "schedule");
        Objects.requireNonNull(misfire, "misfire");
        Objects.requireNonNull(source, "source");
        if (allowConcurrentExecutions) {
            if (maxConcurrentExecutions != 0) {
                throw new IllegalArgumentException("maxConcurrentExecutions must be 0 when allowConcurrentExecutions is true");
            }
        } else if (maxConcurrentExecutions < 1) {
            throw new IllegalArgumentException("maxConcurrentExecutions must be at least 1 when allowConcurrentExecutions is false");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("retries must not be negative");
        }
        if (timeout != null && !timeout.isPositive()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        requireNotBlankIfPresent(runner, "runner");
        requireNotBlankIfPresent(window, "window");
        requireNotBlankIfPresent(rateLimit, "rateLimit");
        requireNotBlankIfPresent(retryPolicy, "retryPolicy");
    }

    private static void requireNotBlankIfPresent(@Nullable String value, String field) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    /**
     * Builds a {@code PROGRAMMATIC} definition through the staged {@link JobSpec} builder, e.g.
     * {@code JobDefinition.of("id", Handler.class, spec -> spec.cron(expr, zone).runner("io"))}.
     *
     * @param id the stable identity used to upsert the definition
     * @param handlerType the class that declares the job handler
     * @param configurer the callback that selects a schedule and configures its policies
     * @return the validated programmatic definition
     */
    public static JobDefinition of(String id, Class<?> handlerType, Consumer<JobSpec> configurer) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(handlerType, "handlerType");
        Objects.requireNonNull(configurer, "configurer");
        JobSpecImpl spec = new JobSpecImpl();
        configurer.accept(spec);
        return spec.toDefinition(JobKey.of(id), handlerType);
    }
}
