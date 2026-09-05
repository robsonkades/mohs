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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.execution.ExecutionId;
import io.mohs.engine.LeaseStore.Completion;
import io.mohs.engine.LeaseStore.CompletionResult;

/**
 * Group commit for completions: dispatch results enter a bounded queue and are flushed in a single
 * {@link LeaseStore#complete} transaction — {@code flushSize} results or {@code flushInterval}
 * elapsed since the first pending one, whichever comes first. It was this or a synchronous commit
 * per execution at the top of the measured wait profile ({@code LWLock:WALWrite}).
 *
 * <p><b>The semantic cost, declared:</b> the window between "the handler finished" and "the result
 * is durable" grows from about 1 ms to at most {@code flushInterval}; a crash in that window
 * re-executes up to {@code flushSize} results more than those in flight. The contract was already
 * at-least-once — this changes the exposure to duplicates, not the guarantee. Anyone needing the old
 * behaviour turns on {@code mohs.engine.completion-flush-on-every-result}.
 *
 * <p>Structural backpressure: a full queue blocks the {@code submit} on the handler's thread — the
 * dispatch stays in flight, the claim sees the reduced headroom, and the node stops claiming beyond
 * what it can persist.
 *
 * <p>A batch flush failure falls back to individual completion per result (the same transaction as
 * ever); an individual failure leaves the execution {@code RUNNING} for the reaper — it never kills
 * the flusher thread nor the batch's neighbours (no internal back-off; the recovery path is the
 * usual one).
 *
 * <p>A single flush thread, virtual and named; {@code close()} (Spring, after the engine's stop —
 * the {@code SmartLifecycle} stops first) drains what is left; a {@code submit} after the close
 * completes synchronously through the old path — a zombie finishing after shutdown does not lose its
 * result.
 */
public final class CompletionBatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CompletionBatcher.class);

    /** The idle poll's wait — it only sets how quickly a close is noticed, not the flush trigger. */
    private static final Duration IDLE_POLL = Duration.ofMillis(50);

    private record Pending(CompletionResult result, Consumer<Completion> onOutcome) {
    }

    private final LeaseStore leaseStore;
    private final JobStore jobStore;
    private final int flushSize;
    private final Duration flushInterval;
    private final LinkedBlockingQueue<Pending> queue;
    /**
     * Ids whose completion is IN TRANSIT (between the {@code submit} and the verdict) — the
     * state-based guard the Engine's stray-lease reconcile consults: the grace's temporal heuristic
     * does not cover a job running longer than the grace, and its lease would exist with no entry in
     * any map during the flush window.
     *
     * <p>The entry is removed on EVERY outcome, including a failed individual completion — leaving it
     * would hide the lease from the reconcile forever, the opposite of the purpose.
     */
    private final Set<ExecutionId> inTransit = ConcurrentHashMap.newKeySet();
    private final Thread flusher;
    private volatile boolean closed;

    /** The drain already happened — Spring's {@code close}, after the {@code stop}, does not pay for the join twice. */
    private volatile boolean drained;

    /**
     * Creates a {@code CompletionBatcher} with the supplied values.
     *
     * @param leaseStore the persistence port for execution ownership
     * @param jobStore the persistence port for job definitions and triggers
     * @param flushSize the maximum number of completion results per flush
     * @param flushInterval the maximum interval between completion flushes
     */
    public CompletionBatcher(LeaseStore leaseStore, JobStore jobStore, int flushSize, Duration flushInterval) {
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        if (flushSize <= 0) {
            throw new IllegalArgumentException("flushSize must be positive, got " + flushSize);
        }
        if (!Objects.requireNonNull(flushInterval, "flushInterval").isPositive()) {
            throw new IllegalArgumentException("flushInterval must be positive, got " + flushInterval);
        }
        this.flushSize = flushSize;
        this.flushInterval = flushInterval;
        // 4x the batch: room for the queue to absorb a burst without blocking needlessly, and a
        // ceiling low enough for backpressure to reach the handler threads
        this.queue = new LinkedBlockingQueue<>(flushSize * 4);
        this.flusher = Thread.ofVirtual().name("mohs-completion-flusher").unstarted(this::flushLoop);
    }

    /** Idempotent — the thread starts outside the constructor (JCIP §3.2: {@code this} must not escape before it is built). */
    public void start() {
        if (flusher.getState() == Thread.State.NEW) {
            flusher.start();
        }
    }

    /**
     * Enqueues the result; {@code onOutcome} runs on the flusher's thread with the CAS verdict, AFTER
     * the batch commits — the same "publish only what became durable" guarantee as the synchronous
     * path. After {@code close()}, it completes synchronously on the calling thread.
     *
     * @param result the completion result to persist
     * @param onOutcome the callback receiving the durable completion outcome
     */
    public void submit(CompletionResult result, Consumer<Completion> onOutcome) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(onOutcome, "onOutcome");
        if (closed) {
            onOutcome.accept(completeOne(result));
            return;
        }
        inTransit.add(result.executionId());
        Pending pending = new Pending(result, onOutcome);
        try {
            queue.put(pending);
        } catch (InterruptedException e) {
            // An interrupted put did NOT insert — the transit never began
            inTransit.remove(result.executionId());
            // Complete BEFORE re-arming the flag: with it raised, the JDBC acquire would throw (the
            // same rationale as flushLoop) and the fallback would defeat itself; the restored status
            // is for the CALLER to observe afterwards (JCIP §7.1.3)
            onOutcome.accept(completeOne(result));
            Thread.currentThread().interrupt();
            return;
        }
        // The flusher may have died between the closed check above and the put (check-then-act,
        // JCIP §2.2): its final sweep would then have run before this entry landed, leaving it
        // queued with its marker set and nobody left to complete it. Whoever removes the entry
        // completes it — the sweep by polling, this thread by removing; the queue makes the two
        // atomic against each other
        if (closed && queue.remove(pending)) {
            completeIndividually(pending);
        }
    }

    /** The reconcile's state-based guard: is this id's completion between the submit and the verdict? */
    boolean completionInTransit(ExecutionId id) {
        return inTransit.contains(id);
    }

    /**
     * The {@code Throwable} guard around the {@code flush} and the {@code finally} that degrades to
     * the synchronous path are this service thread's failure policy (JCIP §7.3): without them an
     * {@code Error} (an OOME, a listener with a broken initialiser) would silently kill the flusher
     * with {@code closed} false — the queue would fill, every submit would block forever, and the
     * in-flight executions would be stuck with their lease being renewed, out of the reaper's reach.
     */
    private void flushLoop() {
        List<Pending> buffer = new ArrayList<>(flushSize);
        try {
            while (!(closed && queue.isEmpty())) {
                try {
                    Pending first = queue.poll(IDLE_POLL.toNanos(), TimeUnit.NANOSECONDS);
                    if (first == null) {
                        continue;
                    }
                    buffer.add(first);
                    // Accumulate in slices of at most IDLE_POLL rather than one wait of flushInterval:
                    // the close has to be observed mid-window — a long interval (a test, a future
                    // configuration) would stall the shutdown's drain for the whole window
                    long deadlineNanos = System.nanoTime() + flushInterval.toNanos();
                    while (buffer.size() < flushSize && !closed) {
                        long remainingNanos = deadlineNanos - System.nanoTime();
                        if (remainingNanos <= 0) {
                            break;
                        }
                        Pending next = queue.poll(Math.min(remainingNanos, IDLE_POLL.toNanos()), TimeUnit.NANOSECONDS);
                        if (next != null) {
                            buffer.add(next);
                        }
                    }
                } catch (InterruptedException e) {
                    // A dedicated thread of its own: an interrupt here can only mean "shut down", so it
                    // becomes the close protocol (flush the buffer, drain, exit). It does NOT re-arm
                    // the flag: that would poison the flush's JDBC (Hikari's acquire throws with the
                    // flag raised) and resuming the poll would become a busy-spin.
                    closed = true;
                }
                try {
                    flush(buffer);
                } catch (Throwable t) {
                    log.error("completion flush cycle failed unexpectedly — {} result(s) fall to this node's "
                            + "stray-lease reconcile (or a peer's reaper if this node dies)", buffer.size(), t);
                } finally {
                    // Idempotent with the removes in deliverOutcome/completeIndividually: NO outcome —
                    // an Error included — leaves a live marker, otherwise the lease would stay hidden
                    // from the reconcile forever
                    buffer.forEach(pending -> inTransit.remove(pending.result().executionId()));
                    buffer.clear();
                }
            }
        } finally {
            // If this thread exits by ANY route, submit degrades to the synchronous path rather than
            // blocking on a dead queue — and what was still queued is completed here, one by one, so
            // no result keeps its in-transit marker with nobody left to clear it (a marker that
            // outlives its flusher hides the lease from the reconcile forever). A submit that raced
            // this sweep re-checks after its put and completes its own entry
            closed = true;
            completeQueuedIndividually();
        }
    }

    /**
     * One transaction for the whole batch; if THAT fails, it falls back to individual completion — a
     * result is never discarded because of its neighbours. {@code onOutcome} is guarded per result: a
     * listener that throws takes down neither the flusher nor its batch siblings.
     */
    private void flush(List<Pending> buffer) {
        if (buffer.isEmpty()) {
            return;
        }
        Map<ExecutionId, Completion> verdicts;
        try {
            verdicts = leaseStore.complete(buffer.stream().map(Pending::result).toList(), jobStore);
        } catch (RuntimeException e) {
            log.warn("group completion flush of {} result(s) failed — falling back to one completion per result", buffer.size(), e);
            for (Pending pending : buffer) {
                completeIndividually(pending);
            }
            return;
        }
        for (Pending pending : buffer) {
            deliverOutcome(pending, verdicts.getOrDefault(pending.result().executionId(), Completion.FENCED_OUT));
        }
    }

    private void completeIndividually(Pending pending) {
        try {
            deliverOutcome(pending, completeOne(pending.result()));
        } catch (RuntimeException e) {
            // The transit ended even without a verdict — the lease becomes visible to the
            // reconcile/reaper instead of hiding behind a dead marker
            inTransit.remove(pending.result().executionId());
            log.error("could not record the completion of execution {} — its lease stands until a reaper reclaims it",
                    pending.result().executionId(), e);
        }
    }

    private Completion completeOne(CompletionResult result) {
        return leaseStore.complete(List.of(result), jobStore)
                .getOrDefault(result.executionId(), Completion.FENCED_OUT);
    }

    private void deliverOutcome(Pending pending, Completion completion) {
        inTransit.remove(pending.result().executionId());
        try {
            pending.onOutcome().accept(completion);
        } catch (RuntimeException e) {
            log.error("completion outcome callback of execution {} threw — the result IS durable, only its follow-up (events/metrics) was lost",
                    pending.result().executionId(), e);
        }
    }

    /**
     * Drains and stops: it waits for the flusher (up to 10s) and then sweeps the queue synchronously.
     *
     * <p>The final sweep is not redundancy — a {@code put} that passed the {@code closed} check in
     * {@code submit} may land AFTER the flusher exits (check-then-act is not atomic, JCIP §2.2), and
     * the sweep is what catches that straggler; it also covers a flusher that never started and a
     * join that timed out. Called by Spring after the engine's stop.
     */
    @Override
    public void close() {
        close(Duration.ofSeconds(10));
    }

    /**
     * A drain with the caller's deadline. The engine's {@code stop} passes whatever is left of the
     * {@code grace}: without it, the drain added a FIXED 10s after the {@code DrainDeadline} had
     * already been spent, overrunning the Boot shutdown phase that {@code stop}'s Javadoc promises to
     * respect.
     *
     * @param drainBudget the time allowed for pending writes to finish on shutdown
     */
    public void close(Duration drainBudget) {
        closed = true;
        // The JOIN is what is not paid for twice: the engine's stop already waited with its own
        // deadline, and a second join would land after the final heartbeat, with the cluster already
        // considering us dead. The SWEEP is different — it is what catches the put that landed after
        // the flusher exited (check-then-act is not atomic, JCIP §2.2), and that straggler can arrive
        // BETWEEN the stop's drain and Spring's bean destruction. Guarding the sweep together with
        // the join lost that result silently
        if (!drained && flusher.getState() != Thread.State.NEW) {
            try {
                if (!flusher.join(drainBudget)) {
                    log.warn("completion flusher did not drain within {} — draining the remaining {} result(s)"
                            + " synchronously; result(s) already inside the flusher buffer stay at the mercy of"
                            + " the reaper", drainBudget, queue.size());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        completeQueuedIndividually();
        drained = true;
    }

    /** The synchronous sweep of whatever is still queued — the flusher's last act and the close's straggler catch. */
    private void completeQueuedIndividually() {
        for (Pending pending; (pending = queue.poll()) != null; ) {
            completeIndividually(pending);
        }
    }
}
