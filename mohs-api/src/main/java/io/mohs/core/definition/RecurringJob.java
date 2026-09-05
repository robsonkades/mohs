/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.core.definition;

import java.lang.annotation.Documented;
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
 * A {@link MohsJob} stereotype for the <b>automatic</b> job: a declared schedule, firing by itself,
 * with an occurrence that carries no payload.
 *
 * <p>The handler therefore cannot demand a typed payload (validated at boot); a {@code Map} or
 * {@code Object} parameter is allowed, because an automatic firing delivers an empty map while a
 * one-off manual invocation may carry data. Exactly one trigger is required
 * ({@link #cron()} plus {@link #zone()}, {@link #every()}, or {@link #everyAfterFinish()}) — a job
 * with no schedule is an {@link OnDemandJob}.
 *
 * <p>Meta-annotated with {@code @MohsJob} (Spring's {@code @Service}/{@code @Component} pattern):
 * each attribute is an {@link AliasFor} of its counterpart on the general annotation, and the
 * scanner sees through the stereotype — one translation, no mechanics of its own. One-off manual
 * invocation still applies ({@code POST /schedule}, {@code Mohs.schedule}): the stereotype names
 * the primary role, not an exclusivity.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Documented
@Retention(RetentionPolicy.RUNTIME)
@MohsJob(id = "")
public @interface RecurringJob {

    /**
     * An alias of {@link #id()} — the concise form {@code @RecurringJob("sync")} with the remaining attributes named.
     *
     * @return the job ID alias
     */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String value() default "";

    /**
     * The stable identity — it becomes the {@code JobKey}; an alias of {@link #value()}. Mandatory (blank fails the boot), upserted at boot.
     *
     * @return the stable job identity
     */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String id() default "";

    /**
     * A mutable display label. Defaults to the id when left empty.
     *
     * @return the display label, or empty to use the ID
     */
    @AliasFor(annotation = MohsJob.class, attribute = "name")
    String name() default "";

    /**
     * A Quartz-style, seconds-first cron expression. Requires {@link #zone()}.
     *
     * @return the cron expression, or empty when another schedule is selected
     */
    @AliasFor(annotation = MohsJob.class, attribute = "cron")
    String cron() default "";

    /**
     * The zone the cron expression is evaluated in. Mandatory when {@link #cron()} is set.
     *
     * @return the cron time-zone ID
     */
    @AliasFor(annotation = MohsJob.class, attribute = "zone")
    String zone() default "";

    /**
     * A fixed-rate interval (an ISO-8601 duration, e.g. {@code "PT30S"}), anchored to the scheduled firing time.
     *
     * @return the fixed-rate ISO-8601 duration, or empty
     */
    @AliasFor(annotation = MohsJob.class, attribute = "every")
    String every() default "";

    /**
     * A fixed-delay interval (an ISO-8601 duration), anchored to the end of the previous execution.
     *
     * @return the fixed-delay ISO-8601 duration, or empty
     */
    @AliasFor(annotation = MohsJob.class, attribute = "everyAfterFinish")
    String everyAfterFinish() default "";

    /**
     * The named {@link MohsRunner} this job runs on.
     *
     * @return the runner name, or empty for the default runner
     */
    @AliasFor(annotation = MohsJob.class, attribute = "runner")
    String runner() default "";

    /**
     * The named {@link ExecutionWindow} that excludes firing times.
     *
     * @return the exclusion-window name, or empty for no exclusion
     */
    @AliasFor(annotation = MohsJob.class, attribute = "window")
    String window() default "";

    /**
     * The named {@link RateLimit} bounding this job's firing rate, cluster-wide.
     *
     * @return the rate-limit name, or empty for no rate limit
     */
    @AliasFor(annotation = MohsJob.class, attribute = "rateLimit")
    String rateLimit() default "";

    /**
     * The misfire policy. Defaults to {@link Misfire#IGNORE}.
     *
     * @return the missed-firing policy
     */
    @AliasFor(annotation = MohsJob.class, attribute = "misfire")
    Misfire misfire() default Misfire.IGNORE;

    /**
     * Born paused on the FIRST registration — see {@link MohsJob#startPaused()}.
     *
     * @return whether the first registration starts paused
     */
    @AliasFor(annotation = MohsJob.class, attribute = "startPaused")
    boolean startPaused() default false;

    /**
     * See {@link MohsJob#allowConcurrentExecutions()}.
     *
     * @return whether concurrent execution is unrestricted
     */
    @AliasFor(annotation = MohsJob.class, attribute = "allowConcurrentExecutions")
    boolean allowConcurrentExecutions() default true;

    /**
     * See {@link MohsJob#maxConcurrentExecutions()}.
     *
     * @return the concurrency ceiling when overlap is restricted
     */
    @AliasFor(annotation = MohsJob.class, attribute = "maxConcurrentExecutions")
    int maxConcurrentExecutions() default 0;

    /**
     * See {@link MohsJob#retries()} — the default of 1 is what makes delivery at-least-once under node failure.
     *
     * @return the number of retry attempts beyond the first
     */
    @AliasFor(annotation = MohsJob.class, attribute = "retries")
    int retries() default 1;

    /**
     * The attempt's timeout (an ISO-8601 duration, e.g. {@code "PT5M"}).
     *
     * @return the ISO-8601 attempt timeout, or empty for no job-specific timeout
     */
    @AliasFor(annotation = MohsJob.class, attribute = "timeout")
    String timeout() default "";

    /**
     * The bean name of a custom retry policy, for cases {@link #retries()} cannot express.
     *
     * @return the retry-policy bean name, or empty for the default policy
     */
    @AliasFor(annotation = MohsJob.class, attribute = "retryPolicy")
    String retryPolicy() default "";
}
