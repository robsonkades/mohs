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
import io.mohs.core.job.JobKey;
import io.mohs.core.Mohs;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.Schedule;

import org.jspecify.annotations.Nullable;

/**
 * A job, defined once: handler, schedule and policies. It is invoked in N ways (cron,
 * {@link Mohs#enqueue}, {@link Mohs#batch}, the dashboard) — and none of them redefines policy.
 *
 * <p>This is a definition value. Executable code is declared with {@link JobRegistration};
 * application invocations use the direct typed methods of {@link Mohs}.
 *
 * @param key                     the job's stable identity — the upsert key, which never changes across redeploys of the same job
 * @param name                    an optional human-readable label, with no role in identity or scheduling
 * @param executableKey           the stable identity of locally registered executable code
 * @param schedule                when the job fires — {@code CronSpec}/{@code IntervalSpec}/{@code OnDemandSpec}
 * @param runner                  the name of the {@code MohsRunner} that runs the invocations; {@code null} uses the default runner
 * @param window                  the name of the {@code ExecutionWindow} restricting when the job may run; {@code null} means unrestricted
 * @param rateLimit               the name of the {@code RateLimit} bounding this job's firing rate cluster-wide; {@code null} means no limit
 * @param misfire                 the policy applied when a firing is missed
 * @param startPaused             born paused on the FIRST registration of the definition — the schedule is declared but disarmed until a {@code resume}; after birth, {@code paused} is an operator decision and a redeploy never re-pauses
 * @param maxConcurrentExecutions global grant ceiling per job; zero is unlimited
 * @param retries                 the number of attempts beyond the first failure
 * @param timeout                 the maximum time per attempt; {@code null} means no timeout of its own (the cluster-wide Watchdog Bound, if configured, still applies)
 * @param retryPolicy             the name of a Spring bean holding a custom retry policy, for cases {@code retries} cannot express; {@code null} uses the default policy
 * @param source                  the declaration source, used for audit
 * @param inputContract           the stable persisted input format identifier
 */
public record JobDefinition(
        JobKey key,
        @Nullable String name,
        JobKey executableKey,
        Schedule schedule,
        @Nullable String runner,
        @Nullable String window,
        @Nullable String rateLimit,
        Misfire misfire,
        boolean startPaused,
        int maxConcurrentExecutions,
        int retries,
        @Nullable Duration timeout,
        @Nullable String retryPolicy,
        DefinitionSource source,
        String inputContract) {

    /**
     * Creates a {@code JobDefinition} with the supplied values.
     *
     * @param key                     the job's stable identity — the upsert key, which never changes across redeploys of the same job
     * @param name                    an optional human-readable label, with no role in identity or scheduling
     * @param executableKey           the stable identity of locally registered executable code
     * @param schedule                when the job fires — {@code CronSpec}/{@code IntervalSpec}/{@code OnDemandSpec}
     * @param runner                  the name of the {@code MohsRunner} that runs the invocations; {@code null} uses the default runner
     * @param window                  the name of the {@code ExecutionWindow} restricting when the job may run; {@code null} means unrestricted
     * @param rateLimit               the name of the {@code RateLimit} bounding this job's firing rate cluster-wide; {@code null} means no limit
     * @param misfire                 the policy applied when a firing is missed
     * @param startPaused             born paused on the FIRST registration of the definition — the schedule is declared but disarmed until a {@code resume}; after birth, {@code paused} is an operator decision and a redeploy never re-pauses
     * @param maxConcurrentExecutions global grant ceiling per job; zero is unlimited
     * @param retries                 the number of attempts beyond the first failure
     * @param timeout                 the maximum time per attempt; {@code null} means no timeout of its own (the cluster-wide Watchdog Bound, if configured, still applies)
     * @param retryPolicy             the name of a Spring bean holding a custom retry policy, for cases {@code retries} cannot express; {@code null} uses the default policy
     * @param source                  {@code ANNOTATION} (through {@link MohsJob}) or {@code PROGRAMMATIC} (through {@link #of})
     */
    public JobDefinition {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(executableKey, "executableKey");
        TechnicalKey.require(inputContract, "inputContract");

        Objects.requireNonNull(schedule, "schedule");
        Objects.requireNonNull(misfire, "misfire");
        Objects.requireNonNull(source, "source");
        if (maxConcurrentExecutions < 0)
            throw new IllegalArgumentException("maxConcurrentExecutions must be nonnegative");

        if (retries < 0) {
            throw new IllegalArgumentException("retries must not be negative");
        }

        if (timeout != null) TimePrecision.micros(timeout);

        if (timeout != null && !timeout.isPositive()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        requireNotBlankIfPresent(runner, "runner");
        requireNotBlankIfPresent(window, "window");
        requireNotBlankIfPresent(rateLimit, "rateLimit");
        requireNotBlankIfPresent(retryPolicy, "retryPolicy");
    }

    private static void requireNotBlankIfPresent(@Nullable String value, String field) {
        if (value != null) TechnicalKey.require(value, field);
    }

    /**
     * Constructs a definition value from one schedule and immutable execution policy.
     */
    public static JobDefinition of(String id, Schedule schedule, JobOptions options, Misfire misfire, boolean startPaused) {
        Objects.requireNonNull(options, "options");
        JobKey key = JobKey.of(id);
        return new JobDefinition(key, options.name(), key, schedule, options.runner(), options.window(),
                options.rateLimit(), misfire, startPaused, options.maxConcurrentExecutions(), options.retries(),
                options.timeout(), options.retryPolicy(), DefinitionSource.PROGRAMMATIC, key.value());
    }

    /**
     * Creates the read model for a direct binding; executable identity never names a Java class.
     */
    public JobDefinition withExecutableKey(JobKey executable) {
        return new JobDefinition(key, name, executable, schedule, runner, window, rateLimit, misfire,
                startPaused, maxConcurrentExecutions, retries, timeout, retryPolicy, source, inputContract);
    }

    /**
     * Declares the durable input format independently of the local Java class.
     */
    public JobDefinition withInputContract(String contract) {
        return new JobDefinition(key, name, executableKey, schedule, runner, window, rateLimit, misfire,
                startPaused, maxConcurrentExecutions, retries, timeout, retryPolicy, source, contract);
    }
}
