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
 *
 * @param key identidade estável do job — chave de upsert, nunca muda entre redeploys do mesmo job
 * @param name rótulo legível opcional, sem papel na identidade nem na agenda
 * @param handlerType classe que processa cada execução deste job
 * @param schedule quando o job dispara — {@code CronSpec}/{@code IntervalSpec}/{@code OnDemandSpec}
 * @param runner nome do {@code MohsRunner} que executa as invocações; {@code null} usa o runner default
 * @param window nome da {@code ExecutionWindow} que restringe quando o job pode rodar; {@code null} = sem restrição
 * @param misfire política aplicada quando um disparo é perdido
 * @param startPaused nasce pausado no PRIMEIRO registro da definição (ADR-0037) — a agenda fica declarada e desarmada até um {@code resume}; depois do nascimento, {@code paused} é decisão de operador e redeploy nunca re-pausa
 * @param allowConcurrentExecutions se {@code true}, sem teto de execuções concorrentes deste job — {@code maxConcurrentExecutions} deve ser {@code 0}
 * @param maxConcurrentExecutions teto de execuções concorrentes quando {@code allowConcurrentExecutions} é {@code false} — pelo menos {@code 1}
 * @param retries número de tentativas adicionais após a primeira falha
 * @param timeout prazo máximo por tentativa; {@code null} = sem timeout próprio (o Watchdog Bound cluster-wide, se configurado, ainda se aplica)
 * @param retryPolicy nome do bean Spring de uma política de retry customizada, para casos que {@code retries} não expressa; {@code null} usa a política default
 * @param source {@code ANNOTATION} (via {@link MohsJob}) ou {@code PROGRAMMATIC} (via {@link #of})
 */
public record JobDefinition(
        JobKey key,
        @Nullable String name,
        Class<?> handlerType,
        Schedule schedule,
        @Nullable String runner,
        @Nullable String window,
        Misfire misfire,
        boolean startPaused,
        boolean allowConcurrentExecutions,
        int maxConcurrentExecutions,
        int retries,
        @Nullable Duration timeout,
        @Nullable String retryPolicy,
        DefinitionSource source) {

    /** Assinatura pré-ADR-0037 — {@code startPaused = false}, o job nasce armado (comportamento de sempre). */
    public JobDefinition(JobKey key, @Nullable String name, Class<?> handlerType, Schedule schedule,
            @Nullable String runner, @Nullable String window, Misfire misfire, boolean allowConcurrentExecutions,
            int maxConcurrentExecutions, int retries, @Nullable Duration timeout, @Nullable String retryPolicy,
            DefinitionSource source) {
        this(key, name, handlerType, schedule, runner, window, misfire, false, allowConcurrentExecutions,
                maxConcurrentExecutions, retries, timeout, retryPolicy, source);
    }

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
        requireNotBlankIfPresent(retryPolicy, "retryPolicy");
    }

    private static void requireNotBlankIfPresent(@Nullable String value, String field) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    /**
     * Monta uma definição {@code PROGRAMMATIC} via o builder staged
     * {@link JobSpec}, ex. {@code JobDefinition.of("id", Handler.class, spec ->
     * spec.cron(expr, zone).runner("io"))}.
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
