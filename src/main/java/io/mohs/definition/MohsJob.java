package io.mohs.definition;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.mohs.JobKey;
import io.mohs.Mohs;
import io.mohs.execution.JobContext;
import io.mohs.resource.ExecutionWindow;
import io.mohs.resource.JobQueue;
import io.mohs.resource.MohsRunner;
import io.mohs.schedule.Misfire;

/**
 * Declara um job num método de um bean gerenciado pelo Spring — sem
 * interface {@code Job}, sem {@code implements}. O starter transforma cada
 * método anotado em exatamente um {@link JobDefinition} no boot (source
 * {@link DefinitionSource#ANNOTATION}).
 *
 * <p>{@link #cron()}, {@link #every()} e {@link #everyAfterFinish()} são
 * mutuamente exclusivos; os três ausentes significam que o job só dispara
 * sob demanda (via {@link Mohs#schedule}, {@link Mohs#batch} ou o
 * dashboard). Os parâmetros do método seguem a mesma convenção
 * independente do gatilho: até um payload e um {@link JobContext},
 * opcionais, em qualquer ordem.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MohsJob {

    /** Identidade estável — vira o {@link JobKey}. Obrigatório, upsert no boot. */
    String id();

    /** Rótulo de exibição mutável. Default para o id quando deixado vazio. */
    String name() default "";

    /** Expressão cron estilo Quartz, seconds-first. Exige {@link #zone()}. */
    String cron() default "";

    /** Zone em que a expressão cron é avaliada. Obrigatório quando {@link #cron()} está definido. */
    String zone() default "";

    /** Intervalo fixed-rate (duração ISO-8601, ex. {@code "PT30S"}), ancorado no horário de disparo agendado. */
    String every() default "";

    /** Intervalo fixed-delay (duração ISO-8601), ancorado no fim da execução anterior. */
    String everyAfterFinish() default "";

    /** {@link MohsRunner} nomeado em que este job executa. */
    String runner() default "";

    /** {@link JobQueue} nomeada que limita a concorrência deste job. */
    String queue() default "";

    /** {@link ExecutionWindow} nomeada que exclui horários de disparo. */
    String window() default "";

    /** Política de misfire. Default {@link Misfire#IGNORE}. */
    Misfire misfire() default Misfire.IGNORE;

    /** Número máximo de tentativas de retry. */
    int retries() default 0;

    /** Timeout da tentativa (duração ISO-8601, ex. {@code "PT5M"}). */
    String timeout() default "";

    /** Nome do bean de uma política de retry customizada, para casos que {@link #retries()} não expressa. */
    String retryPolicy() default "";
}
