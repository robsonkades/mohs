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
import org.springframework.stereotype.Component;

import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.Enqueued;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionEventType;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.OnExecution;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;

/**
 * <b>Scenario 14 — observing executions without interfering with them.</b>
 *
 * <p>Two forms of the same thing. A bean implementing {@link ExecutionListener} sees every event of
 * every job; a method annotated {@link OnExecution} sees one event type, optionally of one job, in
 * the style of Spring's {@code @EventListener}. The annotated method literally becomes a listener —
 * same delivery, same contract, no second pipeline.
 *
 * <h2>The contract, and the line it draws</h2>
 *
 * <ul>
 * <li><b>Asynchronous</b> — delivered on the engine's event executor, not on the attempt's thread.
 *     Nothing a listener does can slow the job down.</li>
 * <li><b>Best-effort</b> — an exception thrown by a listener is caught and logged, and never
 *     reaches the job.</li>
 * <li><b>Unordered</b> — there is no ordering guarantee even between events of one execution:
 *     {@code RetryScheduled} may arrive before the {@code AttemptFailed} that caused it.</li>
 * <li><b>Complete</b> — every outcome publishes, including the reaper reclaiming a dead node's
 *     work. The same pipeline, whichever node observed the failure.</li>
 * </ul>
 *
 * <p>So: metrics, logs, notifications, dashboards — yes. Anything that must not be lost — no. A
 * guaranteed reaction is a job enqueued inside the handler's own transaction (the Transactional
 * Outbox), because the two then commit together and the listener's fate stops mattering.
 *
 * <p>{@link ExecutionEvent} is sealed, which is what makes the switch below exhaustive: a variant
 * added in a future release becomes a compiler warning here rather than a silently skipped
 * {@code default} branch.
 */
@Component
public class EventListenerExample implements ExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(EventListenerExample.class);

    /**
     * The broad form: every event, every job. Pattern matching over the sealed hierarchy gives each
     * variant its own fields — no casts, no {@code instanceof} ladder, and no way to forget one.
     */
    @Override
    public void on(ExecutionEvent event) {
        switch (event) {
            case Enqueued e -> log.debug("enqueued {} of {} by {}", e.executionId().value(), e.jobKey().value(), e.actor());
            case Started e -> log.debug("started {} attempt {}", e.executionId().value(), e.attempt());
            case AttemptFailed e -> log.warn("attempt {} of {} failed: {}", e.attempt(), e.executionId().value(),
                    e.error().toString());
            case RetryScheduled e -> log.info("retry {} of {} at {}", e.nextAttempt(), e.executionId().value(), e.retryAt());
            case Succeeded e -> log.debug("succeeded {} on attempt {}", e.executionId().value(), e.attempt());
            case Failed e -> log.error("terminally failed {} (attempts exhausted: {})", e.executionId().value(),
                    e.attemptsExhausted());
            case Cancelled e -> log.info("cancelled {} on attempt {}", e.executionId().value(), e.attempt());
            case BatchCompleted e -> log.info("batch {} done: {}/{} succeeded", e.name(), e.succeeded(), e.total());
        }
    }

    /**
     * The narrow form: one event type, one job. This is the shape most applications actually want —
     * "page somebody when the nightly invoice run finally gives up" — and the filter means the
     * method is not called for anything else.
     *
     * <p>The method takes either no parameters or exactly one, of the type that event is delivered
     * as. A signature that could not receive the event fails the boot, and so does a filter that
     * could never match — both would otherwise be invisible: a method that never fires looks exactly
     * like one whose event never happened.
     */
    @OnExecution(job = "example-nightly-invoices", event = ExecutionEventType.FAILED)
    void invoiceRunGaveUp(Failed event) {
        log.error("the nightly invoice run failed terminally: {}", event.executionId().value());
    }

    /** No job filter: every terminal failure in the system, which is the usual anchor for an alert. */
    @OnExecution(event = ExecutionEventType.FAILED)
    void anyTerminalFailure(Failed event) {
        log.error("terminal failure of {} on {}", event.jobKey().value(), event.executionId().value());
    }

    /**
     * A batch belongs to no single job, so {@code BATCH_COMPLETED} may not carry a job filter — the
     * boot rejects that combination rather than leaving a method that can never be called.
     */
    @OnExecution(event = ExecutionEventType.BATCH_COMPLETED)
    void batchClosed(BatchCompleted event) {
        log.info("batch {} closed with {} failures", event.name(), event.failed());
    }
}
