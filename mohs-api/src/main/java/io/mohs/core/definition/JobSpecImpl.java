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
import java.time.ZoneId;
import java.util.Objects;

import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;

import org.jspecify.annotations.Nullable;

/**
 * The single implementation of {@link JobSpec}/{@link PolicySpec} — the mutable accumulator behind
 * the staged builder, package-private because nothing outside {@link JobDefinition#of} needs to see
 * it (Effective Java Item 15: minimise accessibility).
 */
final class JobSpecImpl implements JobSpec, PolicySpec {

    private @Nullable Schedule schedule;
    private @Nullable String runner;
    private @Nullable String window;
    private @Nullable String rateLimit;
    private Misfire misfire = Misfire.IGNORE;
    private boolean startPaused;
    private boolean allowConcurrentExecutions = true;
    private int maxConcurrentExecutions;
    /** The same default as the annotations ({@link MohsJob#retries()}): the delivery guarantee must not depend on the declaration style. */
    private int retries = 1;
    private @Nullable Duration timeout;
    private @Nullable String retryPolicy;

    @Override
    public PolicySpec cron(String expression, ZoneId zone) {
        requireNoTriggerYet();
        this.schedule = new CronSpec(expression, zone);
        return this;
    }

    @Override
    public PolicySpec every(Duration interval) {
        requireNoTriggerYet();
        this.schedule = new IntervalSpec(interval, false);
        return this;
    }

    @Override
    public PolicySpec everyAfterFinish(Duration interval) {
        requireNoTriggerYet();
        this.schedule = new IntervalSpec(interval, true);
        return this;
    }

    @Override
    public PolicySpec onDemand() {
        requireNoTriggerYet();
        this.schedule = new OnDemandSpec();
        return this;
    }

    /**
     * {@code cron}/{@code every}/{@code everyAfterFinish}/{@code onDemand} are mutually exclusive by
     * construction only in the chained style ({@code PolicySpec} exposes no trigger methods) — in
     * separate statements, nothing stopped a second call from silently overwriting the first.
     */
    private void requireNoTriggerYet() {
        if (schedule != null) {
            throw new IllegalStateException(
                    "a trigger (cron/every/everyAfterFinish/onDemand) was already chosen for this JobSpec — "
                            + "call exactly one, not several statements on the same configurer");
        }
    }

    @Override
    public PolicySpec runner(String name) {
        this.runner = name;
        return this;
    }

    @Override
    public PolicySpec window(String name) {
        this.window = name;
        return this;
    }

    @Override
    public PolicySpec rateLimit(String name) {
        this.rateLimit = name;
        return this;
    }

    @Override
    public PolicySpec misfire(Misfire policy) {
        this.misfire = Objects.requireNonNull(policy, "policy");
        return this;
    }

    @Override
    public PolicySpec startPaused() {
        this.startPaused = true;
        return this;
    }

    @Override
    public PolicySpec preventOverlap() {
        this.allowConcurrentExecutions = false;
        this.maxConcurrentExecutions = 1;
        return this;
    }

    @Override
    public PolicySpec maxConcurrentExecutions(int max) {
        this.allowConcurrentExecutions = false;
        this.maxConcurrentExecutions = max;
        return this;
    }

    @Override
    public PolicySpec retries(int max) {
        this.retries = max;
        return this;
    }

    @Override
    public PolicySpec timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public PolicySpec retryPolicy(String beanName) {
        this.retryPolicy = beanName;
        return this;
    }

    JobDefinition toDefinition(JobKey key, Class<?> handlerType) {
        if (schedule == null) {
            throw new IllegalStateException(
                    "JobSpec configurer must call cron(...), every(...), everyAfterFinish(...) "
                            + "or onDemand() before JobDefinition.of returns");
        }
        return new JobDefinition(key, null, handlerType, schedule, runner, window, rateLimit, misfire, startPaused,
                allowConcurrentExecutions, maxConcurrentExecutions, retries, timeout, retryPolicy, DefinitionSource.PROGRAMMATIC);
    }
}
