package io.mohs;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Objects;

/**
 * Sole implementation of {@link JobSpec}/{@link Configured} — mutable
 * accumulator behind the staged builder, package-private because nothing
 * outside {@link JobDefinition#of} needs to see it (Effective Java Item 15:
 * minimize accessibility).
 */
final class JobSpecImpl implements JobSpec, Configured {

    private Schedule schedule;
    private String runner;
    private String queue;
    private String window;
    private Misfire misfire = Misfire.IGNORE;
    private int retries;
    private Duration timeout;

    @Override
    public Configured cron(String expression, ZoneId zone) {
        this.schedule = new CronSpec(expression, zone);
        return this;
    }

    @Override
    public Configured every(Duration interval) {
        this.schedule = new IntervalSpec(interval, false);
        return this;
    }

    @Override
    public Configured everyAfterFinish(Duration interval) {
        this.schedule = new IntervalSpec(interval, true);
        return this;
    }

    @Override
    public Configured onDemand() {
        this.schedule = new OnDemandSpec();
        return this;
    }

    @Override
    public Configured runner(String name) {
        this.runner = name;
        return this;
    }

    @Override
    public Configured queue(String name) {
        this.queue = name;
        return this;
    }

    @Override
    public Configured window(String name) {
        this.window = name;
        return this;
    }

    @Override
    public Configured misfire(Misfire policy) {
        this.misfire = Objects.requireNonNull(policy, "policy");
        return this;
    }

    @Override
    public Configured retries(int max) {
        this.retries = max;
        return this;
    }

    @Override
    public Configured timeout(Duration timeout) {
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
