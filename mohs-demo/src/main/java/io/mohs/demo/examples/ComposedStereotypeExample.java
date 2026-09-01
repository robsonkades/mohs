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
package io.mohs.demo.examples;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import io.mohs.core.definition.MohsJob;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.definition.RecurringJob;
import io.mohs.core.schedule.Misfire;

/**
 * <b>Scenario 5 — your own annotation over {@code @MohsJob}.</b>
 *
 * <p>{@link RecurringJob} and {@link OnDemandJob} are not special: they are stereotypes
 * meta-annotated with {@code @MohsJob}, exactly like {@code @Service} over {@code @Component}. The
 * annotation targets {@code ANNOTATION_TYPE} as well as {@code METHOD}, so an application can
 * compose its own, and the scanner resolves it through Spring's merged annotations with
 * {@code @AliasFor} honoured.
 *
 * <h2>When this is worth doing</h2>
 *
 * <p>When a house rule keeps being repeated. If every nightly report in the system is "03:00 UTC,
 * three retries, fire the missed one, on the reports runner", then writing that five times is five
 * chances to get one attribute wrong — and the day the rule changes, it changes in five places.
 * {@link NightlyReport} states it once, and each job supplies only what is genuinely its own: the
 * id.
 *
 * <p>Do not compose an annotation that saves nothing. A stereotype whose only value is a shorter
 * name adds an indirection a reader has to open before they can tell what the job does.
 *
 * <h2>The rules the scanner enforces</h2>
 *
 * <ul>
 * <li>Each attribute you re-expose is an {@link AliasFor} of its counterpart on {@code @MohsJob}.
 *     Attributes you do not re-expose keep the value fixed in the meta-annotation, which is the
 *     point: {@link NightlyReport} does not let a caller change the schedule.</li>
 * <li>A method is exactly ONE job. Carrying both {@code @NightlyReport} and {@code @MohsJob} fails
 *     the boot, because the winner would otherwise be decided by declaration order in the source.</li>
 * <li>The id is still mandatory. A blank one fails the boot naming the method.</li>
 * </ul>
 */
@Component
public class ComposedStereotypeExample {

    private static final Logger log = LoggerFactory.getLogger(ComposedStereotypeExample.class);

    /**
     * The house rule for nightly reports, stated once. Everything except the id is fixed here, so
     * every job wearing this annotation is guaranteed to share the same schedule and policy.
     *
     * <p>{@code @MohsJob(id = "")} on the annotation itself is what makes it a job stereotype; the
     * empty id is overridden by {@link #value()} through {@code @AliasFor}.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @MohsJob(
            id = "",
            cron = "0 0 3 * * *",
            zone = "UTC",
            misfire = Misfire.FIRE_NOW,
            retries = 3,
            timeout = "PT1H"
    )
    public @interface NightlyReport {

        /** The job's id — the only thing a nightly report gets to choose. */
        @AliasFor(annotation = MohsJob.class, attribute = "id")
        String value() default "";
    }

    /** Reads as one line, and is a full {@code @MohsJob} definition after the merge. */
    @NightlyReport("example-revenue-report")
    void revenueReport() {
        log.info("yesterday's revenue");
    }

    /** A second one, guaranteed identical in policy to the first — that guarantee is the whole feature. */
    @NightlyReport("example-churn-report")
    void churnReport() {
        log.info("yesterday's churn");
    }
}
