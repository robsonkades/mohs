package io.mohs.core.definition;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.mohs.core.job.JobKey;
import io.mohs.core.Mohs;
import io.mohs.core.execution.JobContext;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RateLimit;
import io.mohs.core.schedule.Misfire;

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
 *
 * <p>Componível como meta-anotação ({@code ANNOTATION_TYPE} no target,
 * mesmo desenho do {@code @Scheduled} do Spring): {@link RecurringJob} e
 * {@link OnDemandJob} são estereótipos sobre esta anotação (ADR-0038), e
 * o consumidor pode compor os próprios — o scanner resolve via
 * merged annotations, com {@code @AliasFor} honrado.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
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

    /** {@link ExecutionWindow} nomeada que exclui horários de disparo. */
    String window() default "";

    /**
     * {@link RateLimit} nomeado que limita a vazão de disparos deste job —
     * cluster-wide, não por nó (ADR-0042). Nome inexistente bloqueia o job,
     * de propósito: rodar sem o limite que alguém pediu é pior que parar.
     */
    String rateLimit() default "";

    /** Política de misfire. Default {@link Misfire#IGNORE}. */
    Misfire misfire() default Misfire.IGNORE;

    /**
     * Nasce pausado no PRIMEIRO registro da definição (ADR-0037): a agenda
     * fica declarada e desarmada até um {@code POST /jobs/{id}/resume} (ou
     * {@code Mohs.resume}); execução manual sob demanda continua valendo
     * mesmo pausado. Depois do nascimento, {@code paused} é decisão de
     * operador — redeploy nunca re-pausa.
     */
    boolean startPaused() default false;

    /**
     * Impede que mais de uma execução deste job fique {@code RUNNING} ao
     * mesmo tempo. Default: concorrência permitida (ver {@link
     * PolicySpec#preventOverlap()} pro raciocínio completo) — marque
     * {@code true} pro caso estreito de um job cron/interval cujo próprio
     * disparo seguinte pode ocorrer antes do anterior terminar.
     */
    boolean allowConcurrentExecutions() default true;

    /**
     * Teto de execuções concorrentes deste job — só lido quando {@link
     * #allowConcurrentExecutions()} é {@code false} (ver {@link
     * PolicySpec#maxConcurrentExecutions(int)}); nesse caso é obrigatório
     * ser pelo menos 1 (o default {@code 0} falha o boot com uma mensagem
     * clara em vez de assumir um valor).
     */
    int maxConcurrentExecutions() default 0;

    /** Número máximo de tentativas de retry. */
    int retries() default 0;

    /** Timeout da tentativa (duração ISO-8601, ex. {@code "PT5M"}). */
    String timeout() default "";

    /** Nome do bean de uma política de retry customizada, para casos que {@link #retries()} não expressa. */
    String retryPolicy() default "";
}
