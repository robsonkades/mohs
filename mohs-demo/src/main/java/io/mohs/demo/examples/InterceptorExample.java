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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.execution.JobContext;

/**
 * <b>Scenario 15 — wrapping the handler on its own thread.</b>
 *
 * <p>An interceptor is a Chain of Responsibility around every attempt, running on the attempt's
 * thread, before and after the handler. That is what makes it the right place for anything that has
 * to be present WHILE the work runs: the logging MDC, a tracing span, a tenant bound to the call,
 * a stopwatch that measures the handler and nothing else.
 *
 * <table border="1">
 * <caption>Interceptor against {@link ExecutionListener}</caption>
 * <tr><th></th><th>{@code ExecutionInterceptor}</th><th>{@code ExecutionListener}</th></tr>
 * <tr><td>Thread</td><td>the attempt's own</td><td>the engine's event executor</td></tr>
 * <tr><td>Timing</td><td>around the handler</td><td>after the fact</td></tr>
 * <tr><td>An exception it throws</td><td><b>fails the attempt</b> and retries normally</td><td>caught and logged</td></tr>
 * <tr><td>Can it slow the job down?</td><td>yes — it is on the critical path</td><td>no</td></tr>
 * </table>
 *
 * <p>That last row is the whole trade: whatever sits on the critical path takes part in the outcome.
 * An interceptor that calls a slow service adds that latency to every execution, and one that throws
 * burns a retry.
 *
 * <h2>Rules that are not optional</h2>
 *
 * <ul>
 * <li><b>Always call {@code chain.proceed()}</b>, exactly once. Not calling it silently skips the
 *     handler while the execution still records SUCCEEDED.</li>
 * <li><b>Clean up in a {@code finally}.</b> The thread is reused for other work; anything left in
 *     an MDC or a thread-bound context leaks into the next job — the classic "why does this log line
 *     carry another tenant's id" bug.</li>
 * <li><b>Order is by {@code @Order}</b>, the standard Spring mechanism. Several interceptors nest
 *     in that order, outermost first.</li>
 * </ul>
 *
 * <p>Prefer a {@code ScopedValue} over a {@code ThreadLocal} for context you set here: it is bounded
 * by the {@code proceed()} call by construction, so there is nothing to forget to clean up. MDC is
 * the exception below only because SLF4J's API is what it is.
 */
@Component
@Order(0)
public class InterceptorExample implements ExecutionInterceptor {

    /**
     * Creates a {@code InterceptorExample} instance.
     */
    public InterceptorExample() {
    }

    private static final Logger log = LoggerFactory.getLogger(InterceptorExample.class);

    /**
     * Every log line the handler writes now carries the job, the execution and the attempt — which
     * is the difference between an operator finding one execution among a million and grepping.
     *
     * <p>The {@code finally} is not defensive style: it is what stops this attempt's identity from
     * following the thread into the next one.
     */
    @Override
    public void intercept(JobContext ctx, Chain chain) throws Exception {
        MDC.put("mohs.job", ctx.jobKey().value());
        MDC.put("mohs.execution", ctx.executionId().value());
        MDC.put("mohs.attempt", Integer.toString(ctx.attempt()));
        long startedAt = System.nanoTime();
        try {
            chain.proceed();
        } finally {
            log.debug("attempt finished in {} ms", (System.nanoTime() - startedAt) / 1_000_000);
            MDC.remove("mohs.attempt");
            MDC.remove("mohs.execution");
            MDC.remove("mohs.job");
        }
    }
}
