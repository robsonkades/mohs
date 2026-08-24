package io.mohs.core.definition;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RateLimit;
import io.mohs.core.schedule.Misfire;

/**
 * Estereótipo de {@link MohsJob} para o job <b>automático</b> (ADR-0038):
 * agenda declarada, dispara sozinho, ocorrência sem payload — o handler
 * não pode exigir payload tipado (validação de boot; parâmetro
 * {@code Map}/{@code Object} é permitido: disparo automático entrega mapa
 * vazio, invocação manual avulsa pode entregar dados). Exige exatamente um
 * gatilho ({@link #cron()}+{@link #zone()}, {@link #every()} ou
 * {@link #everyAfterFinish()}) — job sem agenda é {@link OnDemandJob}.
 *
 * <p>Meta-anotada com {@code @MohsJob} (o padrão
 * {@code @Service}/{@code @Component} do Spring): cada atributo é
 * {@link AliasFor} do correspondente na anotação geral, e o scanner
 * enxerga através do estereótipo — uma única tradução, nenhuma mecânica
 * própria. Invocação manual avulsa continua valendo
 * ({@code POST /schedule}, {@code Mohs.schedule}) — o estereótipo nomeia o
 * papel primário, não uma exclusividade.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MohsJob(id = "")
public @interface RecurringJob {

    /** Alias de {@link #id()} — a forma concisa {@code @RecurringJob("sync")} com os demais atributos nomeados. */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String value() default "";

    /** Identidade estável — vira o {@code JobKey}; alias de {@link #value()}. Obrigatório (em branco falha o boot), upsert no boot. */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String id() default "";

    /** Rótulo de exibição mutável. Default para o id quando deixado vazio. */
    @AliasFor(annotation = MohsJob.class, attribute = "name")
    String name() default "";

    /** Expressão cron estilo Quartz, seconds-first. Exige {@link #zone()}. */
    @AliasFor(annotation = MohsJob.class, attribute = "cron")
    String cron() default "";

    /** Zone em que a expressão cron é avaliada. Obrigatório quando {@link #cron()} está definido. */
    @AliasFor(annotation = MohsJob.class, attribute = "zone")
    String zone() default "";

    /** Intervalo fixed-rate (duração ISO-8601, ex. {@code "PT30S"}), ancorado no horário de disparo agendado. */
    @AliasFor(annotation = MohsJob.class, attribute = "every")
    String every() default "";

    /** Intervalo fixed-delay (duração ISO-8601), ancorado no fim da execução anterior. */
    @AliasFor(annotation = MohsJob.class, attribute = "everyAfterFinish")
    String everyAfterFinish() default "";

    /** {@link MohsRunner} nomeado em que este job executa. */
    @AliasFor(annotation = MohsJob.class, attribute = "runner")
    String runner() default "";

    /** {@link ExecutionWindow} nomeada que exclui horários de disparo. */
    @AliasFor(annotation = MohsJob.class, attribute = "window")
    String window() default "";

    /** {@link RateLimit} nomeado que limita a vazão de disparos deste job, cluster-wide (ADR-0042). */
    @AliasFor(annotation = MohsJob.class, attribute = "rateLimit")
    String rateLimit() default "";

    /** Política de misfire. Default {@link Misfire#IGNORE}. */
    @AliasFor(annotation = MohsJob.class, attribute = "misfire")
    Misfire misfire() default Misfire.IGNORE;

    /** Nasce pausado no PRIMEIRO registro (ADR-0037) — ver {@link MohsJob#startPaused()}. */
    @AliasFor(annotation = MohsJob.class, attribute = "startPaused")
    boolean startPaused() default false;

    /** Ver {@link MohsJob#allowConcurrentExecutions()}. */
    @AliasFor(annotation = MohsJob.class, attribute = "allowConcurrentExecutions")
    boolean allowConcurrentExecutions() default true;

    /** Ver {@link MohsJob#maxConcurrentExecutions()}. */
    @AliasFor(annotation = MohsJob.class, attribute = "maxConcurrentExecutions")
    int maxConcurrentExecutions() default 0;

    /** Ver {@link MohsJob#retries()} — o default 1 é o que torna a entrega at-least-once sob falha de nó. */
    @AliasFor(annotation = MohsJob.class, attribute = "retries")
    int retries() default 1;

    /** Timeout da tentativa (duração ISO-8601, ex. {@code "PT5M"}). */
    @AliasFor(annotation = MohsJob.class, attribute = "timeout")
    String timeout() default "";

    /** Nome do bean de uma política de retry customizada, para casos que {@link #retries()} não expressa. */
    @AliasFor(annotation = MohsJob.class, attribute = "retryPolicy")
    String retryPolicy() default "";
}
