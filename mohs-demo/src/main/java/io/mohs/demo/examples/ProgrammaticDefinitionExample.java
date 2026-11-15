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
import java.time.Instant;
import java.time.ZoneId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import io.mohs.core.Mohs;
import io.mohs.core.SubmissionOptions;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.definition.JobRegistration;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.RecurringOptions;
import io.mohs.core.schedule.RecurringSchedule;

/**
 * One executable registration, any number of tenants and submissions.
 * Configuration runs once per application context; the methods below run on demand.
 * Input is persisted as JSON; the injected service and its callback stay in the application.
 */
@Component
public class ProgrammaticDefinitionExample {
    /** Shared executable signature. Tenant IDs are invocation data. */
    public static final JobRef<SyncInput> SYNC = JobRef.of("example-tenant-sync", SyncInput.class);

    /**
     * Serializable invocation data, independent of the executable registration.
     * @param tenantId the tenant whose data the application synchronizes
     */
    public record SyncInput(String tenantId) {}

    /** The application's executable logic; this demonstration records the requested work. */
    public static class TenantSynchronizer {
        private static final Logger log = LoggerFactory.getLogger(TenantSynchronizer.class);
        /** Creates the example application service. */
        public TenantSynchronizer() {}
        /**
         * Records the requested synchronization in this example.
         * @param input the independent input for this invocation
         */
        public void synchronize(SyncInput input) {
            log.info("synchronizing tenant {}", input.tenantId());
        }
    }

    /** One-time wiring. Spring supplies the service, including any advice on its proxy. */
    @Configuration(proxyBeanMethods = false)
    public static class Registration {
        /** Creates the example's one-time configuration. */
        public Registration() {}
        /**
         * Supplies the application logic through Spring.
         * @return the application service used by the job
         */
        @Bean
        TenantSynchronizer tenantSynchronizer() { return new TenantSynchronizer(); }

        /**
         * Declares one reusable executable.
         * @param synchronizer the Spring-managed application service
         * @return the reusable executable declaration; creating it submits no work
         */
        @Bean
        JobRegistration<SyncInput> tenantSync(TenantSynchronizer synchronizer) {
            return JobRegistration.of(SYNC, (input, context) -> synchronizer.synchronize(input))
                    .withOptions(JobOptions.defaults().withRetries(5).withTimeout(Duration.ofMinutes(10)));
        }
    }

    private final Mohs mohs;

    /**
     * Creates the application's submission service.
     * @param mohs the scheduling and operations facade
     */
    public ProgrammaticDefinitionExample(Mohs mohs) { this.mohs = mohs; }

    /**
     * Submits one independent synchronization for background execution.
     * @param tenantId the tenant supplied by the application
     * @return one execution receipt, eligible after commit
     */
    public Enqueued synchronizeNow(String tenantId) {
        return mohs.enqueue(SYNC, new SyncInput(tenantId));
    }

    /**
     * Persists a synchronization to become eligible at the supplied instant.
     * @param tenantId the tenant supplied by the application
     * @param when the earliest execution instant
     * @return one future execution receipt
     */
    public Enqueued synchronizeAt(String tenantId, Instant when) {
        return mohs.scheduleAt(SYNC, new SyncInput(tenantId), when);
    }

    /**
     * Submits priority work with an application-supplied deduplication identity.
     * @param tenantId the tenant supplied by the application
     * @param requestId the application's retry identity, scoped to this executable
     * @return the first accepted receipt for this request
     */
    public Enqueued synchronizeUrgently(String tenantId, String requestId) {
        return mohs.enqueue(SYNC, new SyncInput(tenantId), SubmissionOptions.defaults()
                .withPriority(Priority.HIGH).withIdempotencyKey(requestId));
    }

    /**
     * Same schedule ID updates the recurrence; tenant inputs never need another handler registration.
     * The explicit policy limits overlap independently for each tenant's recurring schedule.
     *
     * @param tenantId the tenant used to namespace the recurrence
     * @param cron the seconds-first recurring expression
     * @param zone the time zone in which to evaluate the expression
     * @return the recurring schedule identity and current trigger state
     */
    public RecurringSchedule defineTenantSync(String tenantId, String cron, ZoneId zone) {
        return mohs.scheduleRecurring("tenant-" + tenantId + "-sync", SYNC, new SyncInput(tenantId),
                new CronSpec(cron, zone), RecurringOptions.defaults().withMisfire(Misfire.FIRE_NOW)
                        .withJobOptions(JobOptions.defaults().withRetries(5)
                                .withTimeout(Duration.ofMinutes(10)).withMaxConcurrentExecutions(1)));
    }

    /**
     * Retires one tenant's recurring schedule and preserves its history.
     * @param tenantId the tenant whose recurrence should stop; other tenants remain scheduled
     */
    public void retireTenant(String tenantId) {
        mohs.remove(JobKey.of("tenant-" + tenantId + "-sync"));
    }
}
