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
package io.mohs.core.event;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.mohs.core.job.JobKey;

/**
 * Per-method sugar for {@link ExecutionListener}, filtered by job and event type — in the style of
 * Spring's {@code @EventListener}.
 *
 * <p>Delivery has the same contract as {@link ExecutionListener}, because that is literally what an
 * annotated method becomes: asynchronous, best-effort, unordered, and with its exception caught and
 * logged rather than reaching the job. A reaction that must not be lost is not a listener — the
 * handler enqueues its continuation inside its own transaction.
 *
 * <p>The method takes either no parameters or exactly one, of the event type it declares (or
 * {@link ExecutionEvent} itself). A signature that cannot receive the event fails the boot, and so
 * does a filter that cannot match — {@code BATCH_COMPLETED} scoped to a job, since a batch belongs
 * to no single job.
 */
@Target(ElementType.METHOD)
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface OnExecution {

    /**
     * The {@link JobKey#value()} to observe; empty (the default) observes every job.
     *
     * @return the observed job ID, or empty to observe all jobs
     */
    String job() default "";

    /**
     * Which {@link ExecutionEvent} variant triggers this method.
     *
     * @return the event variant observed by the method
     */
    ExecutionEventType event();
}
