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
    private @Nullable String window;
    private @Nullable String rateLimit;
    private Misfire misfire = Misfire.IGNORE;
    private boolean startPaused;
    private boolean allowConcurrentExecutions = true;
    private int maxConcurrentExecutions;
    /** Mesmo default das anotações ({@link MohsJob#retries()}): a garantia de entrega não pode depender do estilo de declaração. */
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
     * {@code cron}/{@code every}/{@code everyAfterFinish}/{@code onDemand}
     * são mutuamente exclusivos só por construção no estilo encadeado
     * ({@code PolicySpec} não expõe métodos de gatilho) — em statements
     * separados, nada barrava uma segunda chamada sobrescrever a primeira
     * em silêncio.
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
