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
package io.mohs.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.ExecutionInterceptor;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Started;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.JobContext;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;

/**
 * Invokes a claimed execution's handler — the "dispatch" half of the job flow.
 *
 * <p>Completion is a {@link LeaseStore.CompletionResult} fenced by the {@link Grant}'s ownership
 * {@code (node_id, epoch, attempt_number)} — the fencing token — delivered either directly or through the
 * {@link CompletionBatcher} (group commit).
 *
 * <p>An attempt failure with budget left ({@code JobDefinition.retries}) is reborn in the queue with
 * backoff ({@link RetrySchedule}) — the retry entry travels INSIDE the result and lands in the same
 * transaction as the completion (see {@code CompletionResult.retry}); an exhausted budget is a
 * terminal {@code FAILED} — or, when the definition names one, whatever its {@code retryPolicy}
 * decides ({@link RetryPolicyRegistry}).
 *
 * <p>Synchronous and with no pool of its own: what decides how many {@code dispatch} calls stay in
 * flight, and on what kind of thread, is the {@link Engine}'s poll loop.
 */
public final class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private static final String NO_HANDLER_ERROR = "no handler registered for job ";

    /**
     * The ceiling on {@code Attempt.error}, in chars: the payload's documented ceiling (256 KB), well
     * above any stack trace — the history column was widened past 64 KB precisely so a trace never
     * truncates — and low enough that a handler echoing a response body into its exception cannot
     * make one attempt row megabytes wide. Package-private for the test.
     */
    static final int MAX_ERROR_LENGTH = 256 * 1024;

    private final LeaseStore leaseStore;
    private final JobStore jobStore;
    private final HandlerRegistry handlerRegistry;
    private final Clock clock;
    private final List<ExecutionInterceptor> interceptors;
    private final ExecutionEventPublisher events;
    private final EngineMetrics metrics;
    private final RetryPolicyRegistry retryPolicies;
    private final @Nullable CompletionBatcher completionBatcher;

    /**
     * The ownership the claim handed over: {@code (nodeId, epoch, attemptNumber)} is the fence on every completion;
     * {@code attemptNumber} came from the queue entry (nothing counts attempts on the hot path); and
     * {@code claimedAt} anchors the watchdog's synthetic attempt.
     *
     * <p>It used to carry the execution's {@code created_at} as well, all the way from the payload
     * read to the completion, for no reason other than matching a primary key that led with time.
     * With the key normalised to {@code execution_id}, the completion matches by id and the value
     * has nowhere left to be used.
     */
    public record Grant(String nodeId, long epoch, int attemptNumber, Instant claimedAt) {
        public Grant {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(claimedAt, "claimedAt");
            if (attemptNumber < 1) {
                throw new IllegalArgumentException("attemptNumber must be >= 1");
            }
        }
    }

    /** No custom retry policy and no group commit (synchronous completion per result) — the convenience for tests and one-off callers. */
    public Dispatcher(LeaseStore leaseStore, JobStore jobStore, HandlerRegistry handlerRegistry, Clock clock,
            List<ExecutionInterceptor> interceptors, List<ExecutionListener> listeners, AsyncTaskExecutor eventExecutor,
            EngineMetrics metrics) {
        this(leaseStore, jobStore, handlerRegistry, clock, interceptors, listeners, eventExecutor, metrics, null,
                RetryPolicyRegistry.empty());
    }

    public Dispatcher(LeaseStore leaseStore, JobStore jobStore, HandlerRegistry handlerRegistry, Clock clock,
            List<ExecutionInterceptor> interceptors, List<ExecutionListener> listeners, AsyncTaskExecutor eventExecutor,
            EngineMetrics metrics, @Nullable CompletionBatcher completionBatcher) {
        this(leaseStore, jobStore, handlerRegistry, clock, interceptors, listeners, eventExecutor, metrics,
                completionBatcher, RetryPolicyRegistry.empty());
    }

    public Dispatcher(LeaseStore leaseStore, JobStore jobStore, HandlerRegistry handlerRegistry, Clock clock,
            List<ExecutionInterceptor> interceptors, List<ExecutionListener> listeners, AsyncTaskExecutor eventExecutor,
            EngineMetrics metrics, @Nullable CompletionBatcher completionBatcher, RetryPolicyRegistry retryPolicies) {
        this.retryPolicies = Objects.requireNonNull(retryPolicies, "retryPolicies");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.interceptors = List.copyOf(Objects.requireNonNull(interceptors, "interceptors"));
        this.events = new ExecutionEventPublisher(Objects.requireNonNull(listeners, "listeners"), Objects.requireNonNull(eventExecutor, "eventExecutor"));
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.completionBatcher = completionBatcher;
    }

    /** The form with no external cancellation source — its own signal, which nothing raises. A convenience for tests and one-off callers. */
    public void dispatch(Execution execution, JobDefinition definition, Object payload, Grant grant) {
        dispatch(execution, definition, payload, new CancellationSignal(), grant);
    }

    public void dispatch(Execution execution, JobDefinition definition, Object payload, CancellationSignal signal,
            Grant grant) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(signal, "signal");
        Objects.requireNonNull(grant, "grant");

        Instant firedAt = clock.instant();
        metrics.dispatchLatency(execution.jobKey(), Duration.between(execution.scheduledAt(), firedAt));

        int attemptNumber = grant.attemptNumber();
        JobContext ctx = new DefaultJobContext(execution.jobKey(), execution.id(), attemptNumber, execution.scheduledAt(), firedAt, signal);

        Optional<JobHandler> handler = handlerRegistry.find(execution.jobKey());
        if (handler.isEmpty()) {
            // Goes through the retry budget on purpose: during a rolling update another node (running
            // the version that still registers the handler) may claim the retry
            fail(execution, definition, firedAt, new IllegalStateException(NO_HANDLER_ERROR + execution.jobKey().value()), grant);
            return;
        }

        CancellationSignal.Reason preStart = signal.reason();
        if (preStart != null) {
            failBeforeStart(execution, definition, firedAt, preStart, grant);
            return;
        }

        // Started means "the interceptor chain, and through it the handler, is about to run" —
        // published only once the two exits above are behind: an attempt that fails for want of a
        // handler, or is cancelled before starting, ran nothing, and a Started followed by a Failed
        // would tell a listener that work began
        events.publish(new Started(execution.id(), execution.jobKey(), attemptNumber, firedAt));
        // succeed() sits outside the try on purpose: the catch maps by the signal's reason, and its
        // precondition is "the HANDLER exited abnormally" — a failure of the success write is not a
        // handler failure, so it propagates (the Engine logs it, the lease stays up and the reaper
        // decides — indistinguishable from a crash before completion, which is the honest path)
        try {
            invokeWithinInterruptWindow(handler.orElseThrow(), payload, ctx, signal);
        } catch (Exception e) {
            failSignalAware(execution, definition, firedAt, e, signal, grant);
            return;
        }
        succeed(execution, definition, firedAt, grant);
    }

    /**
     * A pre-start check: a signal raised while the task is still queued in the runner is honoured
     * BEFORE invoking the handler — not starting new work is the first step of a graceful shutdown
     * (Burns).
     *
     * <p>{@code MANUAL} becomes {@code CANCELLED} without running; {@code SHUTDOWN} becomes a
     * NodeShutdown failure without running (work not done, so a clean retry on another node). A
     * pre-start {@code TIMEOUT} is unreachable — mapped through the same path for exhaustiveness,
     * never ignored.
     */
    private void failBeforeStart(Execution execution, JobDefinition definition, Instant firedAt,
            CancellationSignal.Reason reason, Grant grant) {
        switch (reason) {
            case MANUAL -> cancelled(execution, definition, firedAt,
                    new IllegalStateException("cancel requested before the handler started"), grant);
            case SHUTDOWN -> fail(execution, definition, firedAt, new IllegalStateException(
                    "node shutdown: drain grace elapsed before attempt " + grant.attemptNumber() + " started"), grant);
            case TIMEOUT -> fail(execution, definition, firedAt, timeoutError(definition, grant.attemptNumber(),
                    new IllegalStateException("timeout signalled before the handler started — should be unreachable")), grant);
            // A statement switch over an enum does not enforce exhaustiveness — a new reason with no
            // case lands here, never in silence (the Javadoc's promise)
            default -> throw new IllegalStateException("unmapped cancellation reason: " + reason);
        }
    }

    /**
     * The interrupt window: it opens immediately before the chain and closes in a {@code finally},
     * BEFORE any completion write — JDBC never runs interrupted and a CPU runner's thread returns
     * clean to the pool.
     */
    private void invokeWithinInterruptWindow(JobHandler handler, Object payload, JobContext ctx, CancellationSignal signal) throws Exception {
        signal.registerHandlerThread();
        try {
            runInterceptorChain(handler, payload, ctx);
        } finally {
            signal.unregisterHandlerThreadAndClearInterrupt();
        }
    }

    /**
     * A raised signal reclassifies ONLY an abnormal exit — a normal return is {@code SUCCEEDED} even
     * with the signal up (the work finished; recording anything else would lie and would schedule a
     * duplicate).
     *
     * <p>{@code TIMEOUT}/{@code SHUTDOWN} follow the retry budget like any failure; {@code MANUAL} is
     * a terminal {@code CANCELLED} — a cancel beats the budget.
     */
    private void failSignalAware(Execution execution, JobDefinition definition, Instant firedAt,
            Exception error, CancellationSignal signal, Grant grant) {
        CancellationSignal.Reason reason = signal.reason();
        if (reason == null) {
            fail(execution, definition, firedAt, error, grant);
            return;
        }
        switch (reason) {
            case TIMEOUT -> fail(execution, definition, firedAt, timeoutError(definition, grant.attemptNumber(), error), grant);
            case SHUTDOWN -> fail(execution, definition, firedAt, new IllegalStateException(
                    "node shutdown: drain grace elapsed before attempt " + grant.attemptNumber() + " finished", error), grant);
            case MANUAL -> cancelled(execution, definition, firedAt, error, grant);
            // The same guard as failBeforeStart's: a statement switch does not enforce exhaustiveness,
            // and a reason with no arm would return with NO completion written — the one outcome the
            // completion path must never produce
            default -> throw new IllegalStateException("unmapped cancellation reason: " + reason);
        }
    }

    private static TimeoutException timeoutError(JobDefinition definition, int attemptNumber, Exception cause) {
        TimeoutException error = new TimeoutException("attempt " + attemptNumber + " exceeded job timeout " + definition.timeout());
        error.initCause(cause);
        return error;
    }

    /**
     * INFO, not WARN, and without a stack trace: an honoured cancellation is the system doing what
     * the operator asked, not a failure. The {@code CANCELLED} attempt carries a null {@code error}
     * ({@code Attempt}'s invariant) — the exception the handler exited with goes to the log.
     */
    private void cancelled(Execution execution, JobDefinition definition, Instant firedAt, Exception error, Grant grant) {
        log.info("execution {} of job '{}' cancelled on attempt {} — cooperative cancellation honoured (handler exited with: {})",
                execution.id().value(), execution.jobKey().value(), grant.attemptNumber(), error.toString());
        completeOrDiscard(terminalResult(execution, definition, grant, firedAt, clock.instant(), ExecutionState.CANCELLED, null),
                () -> events.publish(new Cancelled(execution.id(), execution.jobKey(), grant.attemptNumber())));
    }

    /**
     * A terminal completion: the outcome IS the terminal state, no retry entry travels with it, and a
     * fixed-delay occurrence rearms its chain from {@code finishedAt}. {@code error} is what
     * {@code Attempt.error} keeps — {@code null} for a success and for a cancellation.
     */
    private static LeaseStore.CompletionResult terminalResult(Execution execution, @Nullable JobDefinition definition,
            Grant grant, Instant firedAt, Instant finishedAt, ExecutionState outcome, @Nullable Exception error) {
        return new LeaseStore.CompletionResult(execution.id(), execution.jobKey(), grant.nodeId(), grant.epoch(),
                grant.attemptNumber(), firedAt, finishedAt, outcome,
                error == null ? null : error.getClass().getName(), error == null ? null : errorMessage(error),
                outcome, null, execution.batchId(), rearmNextFireAt(execution, definition, finishedAt));
    }

    /**
     * The event pipeline, so the {@link Engine} can publish the reaper's outcomes through the same
     * path as a dispatch — package-private: the publisher stays internal to the package, only who may
     * speak through it changes.
     */
    ExecutionEventPublisher events() {
        return events;
    }

    /**
     * The stray-lease reconcile's state-based guard: is this id's completion in transit inside the
     * {@link CompletionBatcher}? With no batcher (synchronous completion) there is no transit —
     * {@code false}.
     */
    boolean completionInTransit(ExecutionId executionId) {
        return completionBatcher != null && completionBatcher.completionInTransit(executionId);
    }

    /**
     * Drains the group commit. It exists for {@code stop}: with the batcher on, a dispatch future
     * completes at the SUBMIT rather than at the commit, so {@code awaitInFlight} returns with results
     * still in the queue. Announcing the lease free before that lets a peer reclaim an execution that
     * ALREADY SUCCEEDED — and with {@code retries=0} the recorded outcome is a terminal FAILED, not a
     * duplicate. Idempotent: Spring's {@code close} runs afterwards and finds nothing.
     */
    void drainCompletions(Duration drainBudget) {
        if (completionBatcher != null) {
            completionBatcher.close(drainBudget);
        }
    }

    /**
     * A classic Chain of Responsibility (GoF): each interceptor wraps the next, and the innermost
     * calls the real handler. It runs on the dispatch's own thread; an interceptor's exception IS an
     * attempt failure.
     */
    private void runInterceptorChain(JobHandler handler, Object payload, JobContext ctx) throws Exception {
        ExecutionInterceptor.Chain chain = () -> handler.invoke(payload, ctx);
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            ExecutionInterceptor interceptor = interceptors.get(i);
            ExecutionInterceptor.Chain next = chain;
            chain = () -> interceptor.intercept(ctx, next);
        }
        chain.proceed();
    }

    private void succeed(Execution execution, JobDefinition definition, Instant firedAt, Grant grant) {
        completeOrDiscard(terminalResult(execution, definition, grant, firedAt, clock.instant(), ExecutionState.SUCCEEDED, null),
                () -> events.publish(new Succeeded(execution.id(), execution.jobKey(), grant.attemptNumber())));
    }

    /**
     * The terminal completion of a scheduler occurrence on a fixed-delay schedule rearms the chain —
     * {@code end + interval}, "anchored to the end of the previous execution" taken literally.
     *
     * <p>{@code null} on the other schedules, on a manual execution, and when no definition is at hand
     * ({@link #failBeforeDispatch} with the definition removed — a retired job does not fire, and the
     * resurrection through an upsert cures it).
     */
    private static @Nullable Instant rearmNextFireAt(Execution execution, @Nullable JobDefinition definition, Instant finishedAt) {
        return Execution.SCHEDULER_ACTOR.equals(execution.actor())
                && definition != null && definition.schedule() instanceof IntervalSpec interval && interval.afterFinish()
                ? finishedAt.plus(interval.interval())
                : null;
    }

    /** The remaining budget ({@link RetrySchedule}) decides: reborn in the queue with backoff (in the SAME transaction as the completion) or a terminal failure ({@link #failTerminally}). */
    private void fail(Execution execution, JobDefinition definition, Instant firedAt, Exception error, Grant grant) {
        fail(execution, definition, firedAt, error, grant, true);
    }

    private void fail(Execution execution, JobDefinition definition, Instant firedAt, Exception error, Grant grant,
            boolean throughBatcher) {
        int attemptNumber = grant.attemptNumber();
        Optional<Instant> nextRetry = retryPolicies.nextRetryAt(definition, attemptNumber, error, clock.instant());
        if (nextRetry.isEmpty()) {
            failTerminally(execution, definition, firedAt, error, true, grant, throughBatcher);
            return;
        }
        Instant retryAt = nextRetry.orElseThrow();
        int nextAttempt = attemptNumber + 1;
        log.warn("execution {} of job '{}' failed on attempt {} — retry {} scheduled for {}", execution.id().value(),
                execution.jobKey().value(), attemptNumber, nextAttempt, retryAt, error);
        completeOrDiscard(new LeaseStore.CompletionResult(execution.id(), execution.jobKey(), grant.nodeId(), grant.epoch(),
                attemptNumber, firedAt, clock.instant(), ExecutionState.FAILED, error.getClass().getName(), errorMessage(error),
                null,
                new WorkQueue.ReadyEntry(execution.id(), execution.jobKey(), Shards.of(execution.id()),
                        execution.priority().value(), nextAttempt, retryAt),
                execution.batchId(), null),
                () -> {
                    events.publish(new AttemptFailed(execution.id(), execution.jobKey(), attemptNumber, error));
                    events.publish(new RetryScheduled(execution.id(), execution.jobKey(), nextAttempt, retryAt));
                },
                throughBatcher);
    }

    /**
     * The WARN carrying the full exception is the only place a failure's stack trace appears by
     * default — {@code Attempt.error} keeps only the message, and the {@code Failed} event depends on
     * a registered {@code ExecutionListener}; without this log, the cause of a job broken at 3 a.m.
     * would be nowhere.
     *
     * <p>{@code attemptsExhausted} answers the question {@link Failed}'s Javadoc asks: {@code true}
     * only when {@link RetrySchedule} said "no budget left"; a failure terminal by nature
     * (pre-dispatch) publishes {@code false} — an intact budget is not an exhausted one.
     */
    private void failTerminally(Execution execution, @Nullable JobDefinition definition, Instant firedAt,
            Exception error, boolean attemptsExhausted, Grant grant) {
        failTerminally(execution, definition, firedAt, error, attemptsExhausted, grant, true);
    }

    private void failTerminally(Execution execution, @Nullable JobDefinition definition, Instant firedAt,
            Exception error, boolean attemptsExhausted, Grant grant, boolean throughBatcher) {
        log.warn("execution {} of job '{}' failed on attempt {}", execution.id().value(),
                execution.jobKey().value(), grant.attemptNumber(), error);
        completeOrDiscard(terminalResult(execution, definition, grant, firedAt, clock.instant(), ExecutionState.FAILED, error),
                () -> events.publish(new Failed(execution.id(), execution.jobKey(), grant.attemptNumber(), error, attemptsExhausted)),
                throughBatcher);
    }

    /**
     * Publishes the events only if the completion's fence held — a lost incarnation (the reaper or a
     * requeue got there first) discards the result with a WARN, and never publishes an event for a
     * transition that did not happen.
     *
     * <p>With the {@link CompletionBatcher} the write becomes a group commit and the outcome (metrics
     * and events) runs on the flusher's thread, AFTER the batch commits — the same guarantee, with a
     * durability window of at most the flush interval.
     */
    private void completeOrDiscard(LeaseStore.CompletionResult result, Runnable publishEvents) {
        completeOrDiscard(result, publishEvents, true);
    }

    /**
     * {@code throughBatcher = false} forces synchronous completion even with the batcher on: the cold
     * paths ({@link #failBeforeDispatch}, {@link #abandonOwnership}) depend on their callers'
     * "it threw, so it did not happen" contract (the watchdog's retry on the next tick, the Engine's
     * guard). Routing them through the queue would trade the synchronous exception for a log line in
     * the flusher that nobody retries (JCIP §6.3.2 — going asynchronous changes the caller's error
     * contract along with it).
     */
    private void completeOrDiscard(LeaseStore.CompletionResult result, Runnable publishEvents, boolean throughBatcher) {
        if (completionBatcher == null || !throughBatcher) {
            handleOutcome(result, leaseStore.complete(List.of(result), jobStore)
                    .getOrDefault(result.executionId(), LeaseStore.Completion.FENCED_OUT), publishEvents);
            return;
        }
        completionBatcher.submit(result, completion -> handleOutcome(result, completion, publishEvents));
    }

    private void handleOutcome(LeaseStore.CompletionResult result, LeaseStore.Completion completion, Runnable publishEvents) {
        if (completion.owned()) {
            metrics.attemptFinished(result.jobKey(),
                    new Attempt(result.attemptNumber(), result.startedAt(), result.finishedAt(), result.outcome(), result.error()),
                    result.terminalState() != null ? result.terminalState() : ExecutionState.RETRY_WAITING);
            publishEvents.run();
            publishBatchCompletedIfClosed(completion);
        } else {
            log.warn("attempt {} of execution {} finished {} but the incarnation was no longer ours (reaper/requeue passed first) — result discarded",
                    result.attemptNumber(), result.executionId().value(), result.outcome());
        }
    }

    /**
     * Who closed the batch was elected by the database, inside the completion's transaction; the
     * publication happens AFTER it, alongside the completion's other events, because an event does
     * not roll back if the transaction aborts.
     */
    private void publishBatchCompletedIfClosed(LeaseStore.Completion completion) {
        BatchCounters closed = completion.closedBatch();
        if (closed != null) {
            events.publish(new BatchCompleted(closed.batchId(), closed.name(), closed.total(), closed.succeeded(), closed.failed()));
        }
    }

    /**
     * What {@code Attempt.error} keeps of the exception: its message, never the stack trace (that
     * goes to the WARN log), and at most {@link #MAX_ERROR_LENGTH} of it. A handler that throws with
     * a request body or a report in the message would otherwise store that much per attempt, and
     * {@code GET /executions/{id}} would return all of it; past the ceiling it is data, not an
     * error, and the tail says how much was cut.
     */
    private static String errorMessage(Exception error) {
        String message = Objects.requireNonNullElse(error.getMessage(), error.toString());
        return message.length() <= MAX_ERROR_LENGTH ? message : truncated(message);
    }

    /**
     * The head up to the ceiling, then how many chars were dropped. When the last char kept would be
     * the high half of a surrogate pair the cut steps back one char rather than split the pair: a
     * lone surrogate is not valid text for any dialect.
     */
    private static String truncated(String message) {
        int kept = Character.isHighSurrogate(message.charAt(MAX_ERROR_LENGTH - 1)) ? MAX_ERROR_LENGTH - 1 : MAX_ERROR_LENGTH;
        return message.substring(0, kept) + "… [truncated " + (message.length() - kept) + " chars]";
    }

    /**
     * Fails an execution terminally without having gone through {@link #dispatch} — for when the
     * caller ({@link Engine}) already knows the execution failed before the handler could even run
     * (an unreadable payload, a definition removed between claim and dispatch).
     *
     * <p>Always terminal, without consulting the budget: an unreadable payload does not heal by
     * repeating the read, and a removed definition has no trustworthy {@code retries}.
     * {@code Failed.attemptsExhausted} comes out {@code false} — terminal by nature, not by budget.
     */
    void failBeforeDispatch(Execution execution, @Nullable JobDefinition definition, Exception cause, Grant grant) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(cause, "cause");
        // Synchronous on purpose: the Engine's guard handles this write's exception ("log it and leave
        // ownership to the reconcile") — through the batcher's queue, a flush failure would never
        // reach it
        failTerminally(execution, definition, clock.instant(), cause, false, grant, false);
    }

    /**
     * The Watchdog Bound over the new ownership: the node RELEASES ownership of an incarnation whose
     * runtime exceeded the bound — a synthetic attempt FAILED through the retry budget, fenced by the
     * released ownership.
     *
     * <p>The local zombie keeps running until it finishes on its own; its completion carries this SAME
     * ownership, but the lease has already dropped, and a re-claim — even by this same node, whose
     * epoch only moves when its own lease expires — writes the next attempt number, so its fence
     * loses by construction.
     */
    void abandonOwnership(Execution execution, JobDefinition definition, Grant grant, String reason) {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(grant, "grant");
        // Synchronous on purpose: the watchdog only marks ownership as released if this call returns —
        // "a failed release does not mark, and the next tick retries" requires a synchronous exception,
        // not an enqueue that always returns (JCIP §6.3.2)
        fail(execution, definition, grant.claimedAt(), new IllegalStateException(reason), grant, false);
    }

    /**
     * {@link #cancellationRequested()} reads the incarnation's {@link CancellationSignal} — the sources
     * are the job's timeout, the drain grace expiring, and a {@code POST /executions/{id}/cancel}
     * observed by the tick.
     */
    private record DefaultJobContext(JobKey jobKey, ExecutionId executionId, int attempt, Instant scheduledAt, Instant firedAt,
            CancellationSignal signal) implements JobContext {

        @Override
        public boolean cancellationRequested() {
            return signal.cancellationRequested();
        }
    }
}
