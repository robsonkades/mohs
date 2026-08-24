package io.mohs.core.definition;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

import io.mohs.core.Mohs;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RateLimit;

/**
 * Estereótipo de {@link MohsJob} para o job <b>sob demanda</b> (ADR-0038):
 * sem agenda — só roda quando invocado ({@link Mohs#schedule}, a API REST
 * ou o dashboard), com o payload fornecido na invocação. Por isso não
 * expõe atributos de gatilho, {@code misfire} (não há disparo a perder)
 * nem {@code startPaused} (pause não afeta invocação manual) — eles ficam
 * fixos nos defaults da meta-anotação.
 *
 * <p>Meta-anotada com {@code @MohsJob} (o padrão
 * {@code @Service}/{@code @Component} do Spring): açúcar exato para
 * {@code @MohsJob} sem {@code cron}/{@code every}, resolvida pelo scanner
 * via merged annotations — uma única tradução, nenhuma mecânica própria.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MohsJob(id = "")
public @interface OnDemandJob {

    /** Alias de {@link #id()} — a forma concisa {@code @OnDemandJob("import-file")}. */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String value() default "";

    /** Identidade estável — vira o {@code JobKey}; alias de {@link #value()}. Obrigatório (em branco falha o boot), upsert no boot. */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String id() default "";

    /** Rótulo de exibição mutável. Default para o id quando deixado vazio. */
    @AliasFor(annotation = MohsJob.class, attribute = "name")
    String name() default "";

    /** {@link MohsRunner} nomeado em que este job executa. */
    @AliasFor(annotation = MohsJob.class, attribute = "runner")
    String runner() default "";

    /** {@link ExecutionWindow} nomeada que exclui horários de disparo. */
    @AliasFor(annotation = MohsJob.class, attribute = "window")
    String window() default "";

    /** {@link RateLimit} nomeado que limita a vazão de disparos deste job, cluster-wide (ADR-0042). */
    @AliasFor(annotation = MohsJob.class, attribute = "rateLimit")
    String rateLimit() default "";

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
