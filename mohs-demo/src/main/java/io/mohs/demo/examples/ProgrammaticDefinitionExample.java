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

import java.time.Duration;
import java.time.ZoneId;

import org.springframework.stereotype.Component;

import io.mohs.core.Mohs;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.definition.MohsJob;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;

/**
 * <b>Scenario 6 — definitions built from data instead of from annotations.</b>
 *
 * <p>An annotation is a compile-time decision, so it cannot express "one synchronisation job per
 * tenant, with the cron each tenant chose". {@link JobDefinition#of} builds the same definition at
 * runtime, from whatever the application knows.
 *
 * <h2>The builder is staged, on purpose</h2>
 *
 * <p>{@link JobSpec} exposes only the four triggers, and each of them returns {@link PolicySpec},
 * which does not expose them again. "Cron and every" is therefore not a validation error at boot —
 * it does not compile. Both interfaces are sealed to one implementation, which is what lets them
 * gain methods in a minor release without breaking binary compatibility.
 *
 * <p>{@link PolicySpec} carries everything that is not the trigger: {@code runner}, {@code window},
 * {@code rateLimit}, {@code misfire}, {@code startPaused}, {@code preventOverlap},
 * {@code maxConcurrentExecutions}, {@code retries}, {@code timeout}, {@code retryPolicy} — the same
 * vocabulary as the annotation, in any order.
 *
 * <h2>Registration semantics</h2>
 *
 * <p>{@link Mohs#define} is an <b>upsert by {@code JobKey}</b>. It rewrites the definitional part
 * and never touches the operational part: a job an operator paused stays paused, and
 * {@code startPaused} applies at birth only. Redefining a job on every boot is therefore normal and
 * idempotent.
 *
 * <p>A definition built this way is {@code PROGRAMMATIC}, and that source is what decides
 * retirement: {@link Mohs#remove} works only on {@code PROGRAMMATIC} definitions (annotated jobs are
 * retired by deleting the annotation, which makes them {@code ORPHANED} on the next boot). An id
 * cannot be claimed by both sources — the collision fails the boot rather than picking a winner.
 *
 * <h2>Limitation, stated plainly</h2>
 *
 * <p>Defining a job is not the same as supplying its handler. Today the only public way to register
 * a handler is a {@code @MohsJob} method, and that produces an {@code ANNOTATION} definition — which
 * collides with a {@code PROGRAMMATIC} row of the same id. So this path currently registers,
 * reschedules and retires definitions; an execution of one would find no handler and fail through
 * the retry budget with {@code no handler registered for job ...}.
 *
 * <p>Nothing here calls {@link #defineTenantSync} at startup, for exactly that reason. Reach for
 * this API when you are managing definitions from data (an admin screen listing tenants, say), and
 * use {@link MohsJob @MohsJob} for the work itself until a public handler-registration seam exists.
 */
@Component
public class ProgrammaticDefinitionExample {

    private final Mohs mohs;

    /**
     * Creates a {@code ProgrammaticDefinitionExample} with the supplied values.
     *
     * @param mohs the scheduling and operations facade
     */
    public ProgrammaticDefinitionExample(Mohs mohs) {
        this.mohs = mohs;
    }

    /**
     * One definition per tenant, each with the tenant's own cron in the tenant's own zone. The id is
     * the stable identity — deriving it from the tenant id is what makes a redeploy an upsert
     * rather than a duplicate.
     *
     * @param tenantId the tenant identity used to namespace the job
     * @param cron the cron expression for the tenant job
     * @param zone the time zone used to evaluate the schedule
     */
    public void defineTenantSync(String tenantId, String cron, ZoneId zone) {
        mohs.define(JobDefinition.of("tenant-" + tenantId + "-sync", TenantSync.class,
                spec -> spec.cron(cron, zone)
                        .runner("io")
                        .misfire(Misfire.FIRE_NOW)
                        .retries(5)
                        .timeout(Duration.ofMinutes(10))
                        .preventOverlap()));
    }

    /**
     * The same definition without a trigger: it exists, it can be invoked, and it never fires by
     * itself — the programmatic equivalent of {@code @OnDemandJob}.
     *
     * @param tenantId the tenant identity used to namespace the job
     */
    public void defineTenantExport(String tenantId) {
        mohs.define(JobDefinition.of("tenant-" + tenantId + "-export", TenantSync.class,
                spec -> spec.onDemand().retries(2)));
    }

    /**
     * Retirement: enqueued executions become {@code CANCELLED}, the row is flagged retired and
     * disappears from the listings, and the history stays. It is reversible — defining the same key
     * again resurrects it. Calling this on an annotated job throws, telling you to remove the
     * annotation instead.
     *
     * @param tenantId the tenant identity used to namespace the job
     */
    public void retireTenant(String tenantId) {
        mohs.remove(JobKey.of("tenant-" + tenantId + "-sync"));
    }

    /** The class recorded as the definition's {@code handlerType}; it is persisted by name. */
    static final class TenantSync {
    }
}
