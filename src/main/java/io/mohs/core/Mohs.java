package io.mohs.core;

import java.util.function.Consumer;

import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import org.springframework.lang.CheckReturnValue;

import io.mohs.core.definition.JobDefinition;

/**
 * Fachada pública do Mohs — um verbo por operação, sempre sobre definição
 * existente. Corpo ainda não ligado ao motor: M1 é só o contrato; a
 * implementação real vive em {@code io.mohs.engine}, fiada por
 * {@code io.mohs.autoconfigure} (M3).
 */
public interface Mohs {

    @CheckReturnValue
    <T> ScheduleCommand schedule(JobRef<T> ref, T payload);

    /** Overload por string; tipo do payload é checado em runtime contra a definição (erro claro, não CCE). */
    @CheckReturnValue
    ScheduleCommand schedule(String jobId, Object payload);

    @CheckReturnValue
    Batch batch(String name, Consumer<BatchBuilder> configurer);

    void define(JobDefinition definition);

    /** Aposentadoria: cancela disparos futuros, preserva histórico. */
    void remove(JobKey jobKey);

    MohsLifecycle lifecycle();
}
