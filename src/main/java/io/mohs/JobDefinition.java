package io.mohs;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A job, defined once: handler, schedule, and policies. Invoked N ways
 * (cron, {@link Mohs#schedule}, {@link Mohs#batch}, the dashboard) — none
 * of which redefine policy (see
 * {@code docs/adr/0002-definition-vs-invocation.md}).
 *
 * <p>Use {@link #of(String, Class, Consumer)} to build one programmatically
 * (dynamic, data-driven schedules — e.g. per-tenant registration). In a
 * typical Spring app you don't call this directly: the starter translates
 * each {@link MohsJob @MohsJob}-annotated method into exactly one of these
 * at boot.
 */
public record JobDefinition(
        JobKey key,
        String name,
        Class<?> handlerType,
        Schedule schedule,
        String runner,
        String queue,
        String window,
        Misfire misfire,
        int retries,
        Duration timeout,
        DefinitionSource source) {

    public JobDefinition {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(handlerType, "handlerType");
        Objects.requireNonNull(schedule, "schedule");
        Objects.requireNonNull(misfire, "misfire");
        Objects.requireNonNull(source, "source");
        if (retries < 0) {
            throw new IllegalArgumentException("retries must not be negative");
        }
    }

    /**
     * Builds a {@code PROGRAMMATIC} definition via the staged {@link JobSpec}
     * builder, e.g. {@code JobDefinition.of("id", Handler.class, spec ->
     * spec.cron(expr, zone).runner("io").queue("tenant-sync"))}.
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
