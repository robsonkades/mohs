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
import io.mohs.core.ScheduleCommand;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobRef;

/**
 * <b>Scenario 7 — everything a single invocation can say.</b>
 *
 * <p>{@link Mohs#schedule} opens a {@link ScheduleCommand}: a chain over an ALREADY existing
 * definition. The pre-terminal steps adjust this one instance — priority, actor, idempotency key —
 * and the terminal persists it and closes the chain. Policy (retries, timeout, runner) belongs to
 * the definition and is deliberately not overridable here: a job's behaviour must not depend on
 * which call site scheduled it.
 *
 * <p>The three terminals:
 *
 * <table border="1">
 * <caption>When it becomes due</caption>
 * <tr><td>{@code now()}</td><td>as soon as a claim can take it</td></tr>
 * <tr><td>{@code at(instant)}</td><td>at an absolute point in time</td></tr>
 * <tr><td>{@code after(duration)}</td><td>relative to now</td></tr>
 * </table>
 *
 * <p>All three return an {@link Enqueued} receipt whose {@code ExecutionId} is already durable. The
 * non-terminal steps carry {@code @CheckReturnValue}, so a chain abandoned before its terminal is a
 * compiler warning rather than silence — the classic builder-without-build bug.
 *
 * <h2>The three modifiers, and what each is actually for</h2>
 *
 * <ul>
 * <li><b>{@code priority}</b> — five levels, used as the claim ordering. There is no aging in this
 *     version, so {@link Priority#BACKGROUND} can starve under sustained higher-priority load. Use
 *     it to separate classes of work, not to micro-manage a queue.</li>
 * <li><b>{@code as}</b> — the audit trail. It answers "who asked for this?" in the dashboard and in
 *     {@code Execution.actor}. The engine's own name is reserved: a manual schedule may not pass
 *     itself off as a trigger occurrence.</li>
 * <li><b>{@code idempotencyKey}</b> — deduplication by {@code (job, key)}, an Idempotent Receiver.
 *     A second call with the same key creates nothing and returns the ORIGINAL receipt. The key
 *     deduplicates for as long as the execution row exists, so derive it from the business event
 *     ({@code "charge-" + invoiceId}) and never from a timestamp.</li>
 * </ul>
 */
@Component
public class SchedulingOptionsExample {

    /** The job every method below schedules. */
    public static final JobRef<Charge> CHARGE = JobRef.of("example-invoice-charge", Charge.class);

    private static final Logger log = LoggerFactory.getLogger(SchedulingOptionsExample.class);

    private final Mohs mohs;

    public SchedulingOptionsExample(Mohs mohs) {
        this.mohs = mohs;
    }

    public record Charge(String invoiceId, long cents) {
    }

    /** The plain case, and by far the most common line in any application using Mohs. */
    public Enqueued chargeNow(Charge charge) {
        return mohs.schedule(CHARGE, charge).now();
    }

    /**
     * An absolute instant — a deadline the domain already knows (the end of a trial, a contract's
     * renewal date). The caller supplies it, because "now" belongs to the application's own
     * {@code Clock}, never to a static call buried in a library's example.
     */
    public Enqueued chargeAt(Charge charge, Instant when) {
        return mohs.schedule(CHARGE, charge).at(when);
    }

    /** A relative delay — a cool-off, a backoff the domain decides, a "try again in five minutes". */
    public Enqueued chargeAfter(Charge charge, Duration delay) {
        return mohs.schedule(CHARGE, charge).after(delay);
    }

    /**
     * Everything at once, which is what a real call site looks like when the work is money: it
     * jumps the queue, it records who asked, and a retried HTTP request cannot charge the customer
     * twice.
     */
    public Enqueued chargeUrgently(Charge charge, String requestedBy) {
        return mohs.schedule(CHARGE, charge)
                .priority(Priority.CRITICAL)
                .as(requestedBy)
                .idempotencyKey("charge-" + charge.invoiceId())
                .now();
    }

    /**
     * The string overload, for callers holding a name rather than a {@link JobRef} — a REST
     * controller, an admin screen, a dispatch table. The payload's type is checked at runtime
     * against the definition, so a mismatch is a clear error and not a surprise inside the handler.
     */
    public Enqueued chargeByName(String jobId, Charge charge) {
        return mohs.schedule(jobId, charge).now();
    }

    @OnDemandJob(id = "example-invoice-charge", name = "Charge an invoice", retries = 4, timeout = "PT2M")
    void charge(Charge charge) {
        log.info("charging {} cents for invoice {}", charge.cents(), charge.invoiceId());
    }
}
