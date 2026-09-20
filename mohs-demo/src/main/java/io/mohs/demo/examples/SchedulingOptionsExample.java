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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.mohs.core.Mohs;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobRef;

/** Examples of direct typed submission with immutable priority, actor and deduplication options. */
@Component
public class SchedulingOptionsExample {

    /** The job every method below schedules. */
    public static final JobRef<Charge> CHARGE = JobRef.of("example-invoice-charge", Charge.class);

    private static final Logger log = LoggerFactory.getLogger(SchedulingOptionsExample.class);

    private final Mohs mohs;

    /**
     * Creates a {@code SchedulingOptionsExample} with the supplied values.
     *
     * @param mohs the scheduling and operations facade
     */
    public SchedulingOptionsExample(Mohs mohs) {
        this.mohs = mohs;
    }

    /**
     * The payment payload used by the scheduling examples.
     *
     * @param invoiceId the invoice identity
     * @param cents the payment amount in cents
     */
    public record Charge(String invoiceId, long cents) {
    }

    /**
     * The plain case, and by far the most common line in any application using Mohs.
     *
     * @param charge the payment request to schedule
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    public Enqueued chargeNow(Charge charge) {
        return mohs.enqueue(CHARGE, charge);
    }

    /**
     * An absolute instant — a deadline the domain already knows (the end of a trial, a contract's
     * renewal date). The caller supplies it, because "now" belongs to the application's own
     * {@code Clock}, never to a static call buried in a library's example.
     *
     * @param charge the payment request to schedule
     * @param when the instant at which the execution becomes eligible
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    public Enqueued chargeAt(Charge charge, Instant when) {
        return mohs.scheduleAt(CHARGE, charge, when);
    }

    /**
     * A relative delay — a cool-off, a backoff the domain decides, a "try again in five minutes".
     *
     * @param charge the payment request to schedule
     * @param delay the delay before the scheduled instant
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    public Enqueued chargeAfter(Charge charge, Duration delay) {
        return mohs.scheduleAfter(CHARGE, charge, delay);
    }

    /**
     * Everything at once, which is what a real call site looks like when the work is money: it
     * jumps the queue, it records who asked, and a retried HTTP request cannot charge the customer
     * twice.
     *
     * @param charge the payment request to schedule
     * @param requestedBy the actor requesting the execution
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    public Enqueued chargeUrgently(Charge charge, String requestedBy) {
        return mohs.enqueue(CHARGE, charge, io.mohs.core.SubmissionOptions.defaults()
                .withPriority(Priority.CRITICAL).withActor(requestedBy)
                .withIdempotencyKey("charge-" + charge.invoiceId()));
    }

    /**
     * The string overload, for callers holding a name rather than a {@link JobRef} — a REST
     * controller, an admin screen, a dispatch table. The payload's type is checked at runtime
     * against the definition, so a mismatch is a clear error and not a surprise inside the handler.
     *
     * @param jobId the stable identity of the job
     * @param charge the payment request to schedule
     * @return the enqueue receipt, subject to commit of any enclosing transaction
     */
    public Enqueued chargeByName(String jobId, Charge charge) {
        return mohs.enqueue(JobRef.of(jobId, Charge.class), charge);
    }

    @OnDemandJob(id = "example-invoice-charge", name = "Charge an invoice", retries = 4, timeout = "PT2M")
    void charge(Charge charge) {
        log.info("charging {} cents for invoice {}", charge.cents(), charge.invoiceId());
    }
}
