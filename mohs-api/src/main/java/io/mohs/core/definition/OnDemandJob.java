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

import io.mohs.core.Mohs;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.core.resource.RateLimit;

/**
 * A {@link MohsJob} stereotype for the <b>on-demand</b> job: no schedule — it runs only when
 * invoked ({@link Mohs#schedule}, the REST API or the dashboard), with the payload supplied at
 * invocation.
 *
 * <p>It therefore exposes no trigger attributes, no {@code misfire} (there is no firing to miss)
 * and no {@code startPaused} (pausing does not affect manual invocation) — those stay fixed at the
 * meta-annotation's defaults.
 *
 * <p>Meta-annotated with {@code @MohsJob} (Spring's {@code @Service}/{@code @Component} pattern):
 * exact sugar for {@code @MohsJob} without {@code cron}/{@code every}, resolved by the scanner
 * through merged annotations — one translation, no mechanics of its own.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Documented
@Retention(RetentionPolicy.RUNTIME)
@MohsJob(id = "")
public @interface OnDemandJob {

    /** Alias de {@link #id()} — a forma concisa {@code @OnDemandJob("import-file")}. */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String value() default "";

    /** The stable identity — it becomes the {@code JobKey}; an alias of {@link #value()}. Mandatory (blank fails the boot), upserted at boot. */
    @AliasFor(annotation = MohsJob.class, attribute = "id")
    String id() default "";

    /** A mutable display label. Defaults to the id when left empty. */
    @AliasFor(annotation = MohsJob.class, attribute = "name")
    String name() default "";

    /** The named {@link MohsRunner} this job runs on. */
    @AliasFor(annotation = MohsJob.class, attribute = "runner")
    String runner() default "";

    /** The named {@link ExecutionWindow} that excludes firing times. */
    @AliasFor(annotation = MohsJob.class, attribute = "window")
    String window() default "";

    /** The named {@link RateLimit} bounding this job's firing rate, cluster-wide. */
    @AliasFor(annotation = MohsJob.class, attribute = "rateLimit")
    String rateLimit() default "";

    /** Ver {@link MohsJob#allowConcurrentExecutions()}. */
    @AliasFor(annotation = MohsJob.class, attribute = "allowConcurrentExecutions")
    boolean allowConcurrentExecutions() default true;

    /** Ver {@link MohsJob#maxConcurrentExecutions()}. */
    @AliasFor(annotation = MohsJob.class, attribute = "maxConcurrentExecutions")
    int maxConcurrentExecutions() default 0;

    /** See {@link MohsJob#retries()} — the default of 1 is what makes delivery at-least-once under node failure. */
    @AliasFor(annotation = MohsJob.class, attribute = "retries")
    int retries() default 1;

    /** The attempt's timeout (an ISO-8601 duration, e.g. {@code "PT5M"}). */
    @AliasFor(annotation = MohsJob.class, attribute = "timeout")
    String timeout() default "";

    /** The bean name of a custom retry policy, for cases {@link #retries()} cannot express. */
    @AliasFor(annotation = MohsJob.class, attribute = "retryPolicy")
    String retryPolicy() default "";
}
