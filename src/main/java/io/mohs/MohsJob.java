package io.mohs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a job on a method of a Spring-managed bean — no {@code Job}
 * interface, no {@code implements}. The starter turns each annotated method
 * into exactly one {@link JobDefinition} at boot (source
 * {@link DefinitionSource#ANNOTATION}).
 *
 * <p>{@link #cron()}, {@link #every()}, and {@link #everyAfterFinish()} are
 * mutually exclusive; all three absent means the job only fires on demand
 * (via {@link Mohs#schedule}, {@link Mohs#batch}, or the dashboard). Method
 * parameters follow the same convention regardless of trigger: up to one
 * payload and one {@link JobContext}, optional, any order.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MohsJob {

    /** Stable identity — becomes the {@link JobKey}. Required, boot-time upsert. */
    String id();

    /** Mutable display label. Defaults to the id when left empty. */
    String name() default "";

    /** Quartz-style seconds-first cron expression. Requires {@link #zone()}. */
    String cron() default "";

    /** Zone the cron expression is evaluated in. Required when {@link #cron()} is set. */
    String zone() default "";

    /** Fixed-rate interval (ISO-8601 duration, e.g. {@code "PT30S"}), anchored to the scheduled fire time. */
    String every() default "";

    /** Fixed-delay interval (ISO-8601 duration), anchored to the previous execution's end. */
    String everyAfterFinish() default "";

    /** Named {@link MohsRunner} this job executes on. */
    String runner() default "";

    /** Named {@link JobQueue} this job's concurrency is capped by. */
    String queue() default "";

    /** Named {@link ExecutionWindow} excluding fire times. */
    String window() default "";

    /** Misfire policy. Defaults to {@link Misfire#IGNORE}. */
    Misfire misfire() default Misfire.IGNORE;

    /** Maximum retry attempts. */
    int retries() default 0;

    /** Attempt timeout (ISO-8601 duration, e.g. {@code "PT5M"}). */
    String timeout() default "";

    /** Bean name of a custom retry policy, for cases {@link #retries()} can't express. */
    String retryPolicy() default "";
}
