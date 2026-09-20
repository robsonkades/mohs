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

import io.mohs.core.Mohs;
import io.mohs.core.execution.JobContext;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RateLimit;
import io.mohs.core.schedule.Misfire;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a job on a method of a Spring-managed bean — no {@code Job} interface, no
 * {@code implements}. The starter turns each annotated method into exactly one
 * {@link JobDefinition} at boot, with source {@link DefinitionSource#ANNOTATION}.
 *
 * <p>{@link #cron()}, {@link #every()} and {@link #everyAfterFinish()} are mutually exclusive; all
 * three absent means the job fires only on demand (through {@link Mohs#enqueue},
 * {@link Mohs#batch} or the dashboard). The method's parameters follow the same convention
 * regardless of the trigger: at most one payload and one {@link JobContext}, both optional, in any
 * order.
 * The method must return {@code void} and finish synchronously. Async return types and Spring
 * {@code @Async} advice are rejected at startup because completion must include the handler's work.
 * Lazy singleton declarations are initialized before the executable catalog is frozen.
 *
 * <p>Composable as a meta-annotation ({@code ANNOTATION_TYPE} in its target, the same design as
 * Spring's {@code @Scheduled}): {@link RecurringJob} and {@link OnDemandJob} are stereotypes over
 * this annotation, and a consumer may compose their own — the scanner resolves them through merged
 * annotations, with {@code @AliasFor} honoured.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface MohsJob {

    /**
     * The stable identity — it becomes the {@link JobKey}. Mandatory, upserted at boot.
     *
     * @return the stable job identity
     */
    String id();

    /**
     * A mutable display label. Defaults to the id when left empty.
     *
     * @return the display label, or empty to use the ID
     */
    String name() default "";

    /**
     * A Quartz-style, seconds-first cron expression. Requires {@link #zone()}.
     *
     * @return the cron expression, or empty when another schedule is selected
     */
    String cron() default "";

    /**
     * The zone the cron expression is evaluated in. Mandatory when {@link #cron()} is set.
     *
     * @return the cron time-zone ID
     */
    String zone() default "";

    /**
     * A fixed-rate interval (an ISO-8601 duration, e.g. {@code "PT30S"}), anchored to the scheduled firing time.
     *
     * @return the fixed-rate ISO-8601 duration, or empty
     */
    String every() default "";

    /**
     * A fixed-delay interval (an ISO-8601 duration), anchored to the end of the previous execution.
     *
     * @return the fixed-delay ISO-8601 duration, or empty
     */
    String everyAfterFinish() default "";

    /**
     * The named {@link MohsRunner} this job runs on.
     *
     * @return the runner name, or empty for the default runner
     */
    String runner() default "";

    /**
     * The named {@link ExecutionWindow} that excludes firing times.
     *
     * @return the exclusion-window name, or empty for no exclusion
     */
    String window() default "";

    /**
     * The named {@link RateLimit} bounding this job's firing rate — cluster-wide, not per node. A
     * nonexistent name blocks the job on purpose: running without the limit somebody asked for is
     * worse than stopping.
     *
     * @return the rate-limit name, or empty for no rate limit
     */
    String rateLimit() default "";

    /**
     * The misfire policy. Defaults to {@link Misfire#IGNORE}.
     *
     * @return the missed-firing policy
     */
    Misfire misfire() default Misfire.IGNORE;

    /**
     * Born paused on the FIRST registration of the definition: the schedule is declared but
     * disarmed until a {@code POST /jobs/{id}/resume} (or {@code Mohs.resume}); manual on-demand
     * execution still works while paused. After birth, {@code paused} is an operator decision — a
     * redeploy never re-pauses.
     *
     * @return whether the first registration starts paused
     */
    boolean startPaused() default false;


    /**
     * Global admitted grants per job; zero is unlimited, positive values impose the cap.
     */
    int maxConcurrentExecutions() default 0;

    /**
     * Retry attempts BEYOND the first execution. The default is 1, not 0.
     *
     * <p>Finite retries authorize further attempts but do not guarantee successful delivery.
     * Repeated process failures may exhaust the budget before the handler succeeds. Zero disables
     * automatic retry; external effects may still need application idempotency.
     *
     * @return the number of retry attempts beyond the first
     */
    int retries() default 1;

    /**
     * The attempt's timeout (an ISO-8601 duration, e.g. {@code "PT5M"}).
     *
     * @return the ISO-8601 attempt timeout, or empty for no job-specific timeout
     */
    String timeout() default "";

    /**
     * The bean name of a custom retry policy, for cases {@link #retries()} cannot express.
     *
     * @return the retry-policy bean name, or empty for the default policy
     */
    String retryPolicy() default "";
}
