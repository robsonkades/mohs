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
 * Única implementação de {@link JobSpec}/{@link PolicySpec} — acumulador
 * mutável por trás do builder staged, package-private porque nada fora de
 * {@link JobDefinition#of} precisa vê-la (Effective Java Item 15: minimize
 * acessibilidade).
 */
final class JobSpecImpl implements JobSpec, PolicySpec {

    private @Nullable Schedule schedule;
    private @Nullable String runner;
    private @Nullable String queue;
    private @Nullable String window;
    private Misfire misfire = Misfire.IGNORE;
    private int retries;
    private @Nullable Duration timeout;

    @Override
    public PolicySpec cron(String expression, ZoneId zone) {
        this.schedule = new CronSpec(expression, zone);
        return this;
    }

    @Override
    public PolicySpec every(Duration interval) {
        this.schedule = new IntervalSpec(interval, false);
        return this;
    }

    @Override
    public PolicySpec everyAfterFinish(Duration interval) {
        this.schedule = new IntervalSpec(interval, true);
        return this;
    }

    @Override
    public PolicySpec onDemand() {
        this.schedule = new OnDemandSpec();
        return this;
    }

    @Override
    public PolicySpec runner(String name) {
        this.runner = name;
        return this;
    }

    @Override
    public PolicySpec queue(String name) {
        this.queue = name;
        return this;
    }

    @Override
    public PolicySpec window(String name) {
        this.window = name;
        return this;
    }

    @Override
    public PolicySpec misfire(Misfire policy) {
        this.misfire = Objects.requireNonNull(policy, "policy");
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

    JobDefinition toDefinition(JobKey key, Class<?> handlerType) {
        if (schedule == null) {
            throw new IllegalStateException(
                    "JobSpec configurer must call cron(...), every(...), everyAfterFinish(...) "
                            + "or onDemand() before JobDefinition.of returns");
        }
        return new JobDefinition(key, null, handlerType, schedule, runner, queue, window,
                misfire, retries, timeout, DefinitionSource.PROGRAMMATIC);
    }
}
