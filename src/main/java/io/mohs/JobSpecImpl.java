package io.mohs;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Objects;

/**
 * Única implementação de {@link JobSpec}/{@link Configured} — acumulador
 * mutável por trás do builder staged, package-private porque nada fora de
 * {@link JobDefinition#of} precisa vê-la (Effective Java Item 15: minimize
 * acessibilidade).
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
