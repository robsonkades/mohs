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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.definition.RecurringJob;
import io.mohs.core.execution.JobContext;

/**
 * <b>Scenario 4 — what a handler method may look like, and what the engine hands it.</b>
 *
 * <p>There is no {@code Job} interface to implement and nothing to extend. A handler is a method on
 * a Spring-managed bean, and its signature follows one rule: <b>at most one payload and at most one
 * {@link JobContext}, in any order</b>. Anything else fails at boot, naming the method and the
 * reason — never at the first occurrence, in production, at 03:00.
 *
 * <table border="1">
 * <caption>Every accepted shape</caption>
 * <tr><th>Signature</th><th>Accepted</th></tr>
 * <tr><td>{@code void run()}</td><td>yes — {@link #noParameters}</td></tr>
 * <tr><td>{@code void run(Payload p)}</td><td>yes — {@link #payloadOnly}</td></tr>
 * <tr><td>{@code void run(JobContext ctx)}</td><td>yes — {@link #contextOnly}</td></tr>
 * <tr><td>{@code void run(Payload p, JobContext ctx)}</td><td>yes — {@link #payloadAndContext}</td></tr>
 * <tr><td>{@code void run(JobContext ctx, Payload p)}</td><td>yes — the order does not matter</td></tr>
 * <tr><td>{@code void run(A a, B b)}</td><td><b>no</b> — two payloads</td></tr>
 * <tr><td>three or more parameters</td><td><b>no</b></td></tr>
 * </table>
 *
 * <p>The method may be package-private (all of these are) and may declare checked exceptions. An
 * exception thrown by the handler is the attempt's failure and travels the retry path with the
 * ORIGINAL exception preserved: the reflective {@code InvocationTargetException} is unwrapped, so
 * {@code Attempt.error} records what actually went wrong.
 *
 * <p>The parameter's declared type matters beyond this method — it is what the REST API converts a
 * JSON body into before scheduling.
 */
@Component
public class HandlerSignatureExample {

    /**
     * Creates a {@code HandlerSignatureExample} instance.
     */
    public HandlerSignatureExample() {
    }

    private static final Logger log = LoggerFactory.getLogger(HandlerSignatureExample.class);

    /** The whole job, when the work needs nothing from its invocation. */
    @RecurringJob(id = "example-signature-none", every = "PT6H")
    void noParameters() {
        log.info("nothing needed from the invocation");
    }

    /** The payload, typed. Only an on-demand job may demand one — an automatic occurrence has no data to give. */
    @OnDemandJob(id = "example-signature-payload")
    void payloadOnly(ExportRequest request) {
        log.info("exporting {} rows of {}", request.rowCount(), request.table());
    }

    /**
     * The context, without a payload. A recurring job is allowed to take one, because the context
     * is per-attempt information the engine always has — unlike a payload.
     */
    @RecurringJob(id = "example-signature-context", every = "PT6H")
    void contextOnly(JobContext ctx) {
        log.info("job {} execution {} attempt {}", ctx.jobKey().value(), ctx.executionId().value(), ctx.attempt());
    }

    /**
     * Both — and this is where {@link JobContext} earns its place:
     *
     * <ul>
     * <li>{@link JobContext#attempt()} is 1-based and increments on a retry, while
     *     {@link JobContext#executionId()} stays the same. "Am I a retry?" is {@code attempt() > 1},
     *     which is how a handler makes itself idempotent about the part it already did.</li>
     * <li>{@link JobContext#scheduledAt()} against {@link JobContext#firedAt()} is lateness: when
     *     this was due versus when it actually started. A growing gap is backlog.</li>
     * <li>{@link JobContext#cancellationRequested()} is <b>cooperative</b>: it turns true on a
     *     timeout, on shutdown once the drain grace expires, or when someone cancels the execution.
     *     Long work must poll it — nothing kills a thread on your behalf. Returning normally after
     *     seeing it records SUCCEEDED (finished work counts); throwing records CANCELLED.</li>
     * </ul>
     */
    @OnDemandJob(id = "example-signature-both", retries = 2, timeout = "PT10M")
    void payloadAndContext(ExportRequest request, JobContext ctx) {
        if (ctx.attempt() > 1) {
            log.info("attempt {} of execution {} — resuming, not restarting", ctx.attempt(), ctx.executionId().value());
        }
        for (int row = 0; row < request.rowCount(); row++) {
            if (ctx.cancellationRequested()) {
                log.info("cancellation observed at row {} — stopping cleanly", row);
                return;
            }
        }
    }

    /**
     * A payload may be any type Jackson can round-trip, including a {@code Map} when the shape is
     * genuinely dynamic — but a record documents the contract, and a typo in a key becomes a
     * compilation error instead of a {@code null}.
     *
     * @param table the database table name
     * @param rowCount the number of rows included in the statement
     * @param filters the enabled execution-page predicates
     */
    public record ExportRequest(String table, int rowCount, Map<String, String> filters) {
    }
}
