package io.mohs;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Um job, definido uma vez: handler, agenda e políticas. Invocado de N
 * formas (cron, {@link Mohs#schedule}, {@link Mohs#batch}, o dashboard) —
 * nenhuma delas redefine política (ver
 * {@code docs/adr/0002-definition-vs-invocation.md}).
 *
 * <p>Use {@link #of(String, Class, Consumer)} para montar uma
 * programaticamente (agendas dinâmicas, orientadas a dados — ex.: registro
 * por tenant). Num app Spring típico você não chama isso diretamente: o
 * starter traduz cada método anotado com {@link MohsJob @MohsJob} em
 * exatamente uma destas no boot.
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
     * Monta uma definição {@code PROGRAMMATIC} via o builder staged
     * {@link JobSpec}, ex. {@code JobDefinition.of("id", Handler.class, spec ->
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
