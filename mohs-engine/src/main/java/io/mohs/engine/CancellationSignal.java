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
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

/**
 * The cancellation signal of ONE dispatch incarnation: a cooperative flag, a reason, and delivery of
 * an interrupt to the handler's thread.
 *
 * <p>It is the meeting point of the three sources — a job timeout ({@code TIMEOUT}), the drain grace
 * expiring ({@code SHUTDOWN}) and {@code POST /executions/{id}/cancel} ({@code MANUAL}) — and the
 * source {@code JobContext.cancellationRequested()} reads.
 *
 * <p>Registration, interrupt and deregistration share one lock (the {@code FutureTask.cancel}
 * problem, JCIP ch. 7): the interrupt is only delivered while the thread is registered, and
 * {@link #unregisterHandlerThreadAndClearInterrupt()} clears the pending status AFTER leaving the
 * registry — no new interrupt can arrive, so the completion write (JDBC) never runs interrupted and
 * a CPU runner's platform thread never returns poisoned to the pool.
 *
 * <p>{@code ReentrantLock} rather than {@code synchronized} — the project's discipline on a
 * concurrent path (explicit capabilities, JCIP ch. 13).
 */
final class CancellationSignal {

    /** The first reason registered wins — it decides the outcome mapping in the {@code Dispatcher}. */
    enum Reason { TIMEOUT, SHUTDOWN, MANUAL }

    private final ReentrantLock lock = new ReentrantLock();

    private volatile @Nullable Reason reason;
    /** A monotonic stamp of the handler's REAL start — time queued in a runner does not count; read by the tick without a lock. */
    private volatile long handlerStartedNanos;
    private volatile boolean handlerStarted;
    private volatile boolean handlerFinished;

    /** Guarded by {@code lock}; {@code null} outside the register-to-deregister window. */
    private @Nullable Thread handlerThread;

    boolean cancellationRequested() {
        return reason != null;
    }

    @Nullable Reason reason() {
        return reason;
    }

    /**
     * Raises the signal. The first REASON wins, since it decides the outcome mapping — but interrupt
     * DELIVERY is not first-wins: a handler that caught the timeout's interrupt, cleaned up and
     * blocked again (a common pattern) was immune to the shutdown's interrupt, while the log promised
     * "signalling cancellation and interrupting them". Shutdown's lever cannot depend on who
     * signalled first.
     *
     * <p>{@code interrupt} delivers {@code Thread.interrupt()} if — and only if — the handler's
     * thread is registered; before registration or after deregistration it is a pure flag. There is
     * no risk of an interrupt loop: {@code signalJobTimeouts} guards with
     * {@code !cancellationRequested()}, and the drain escalation runs at most twice per {@code stop}.
     */
    void requestCancellation(Reason newReason, boolean interrupt) {
        lock.lock();
        try {
            if (reason == null) {
                reason = newReason;
            }
            if (interrupt && handlerThread != null) {
                handlerThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Called by the handler's own thread immediately before invoking the chain — it opens the interrupt window and starts the timeout clock. */
    void registerHandlerThread() {
        lock.lock();
        try {
            handlerThread = Thread.currentThread();
            handlerStartedNanos = System.nanoTime();
            handlerStarted = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the interrupt window and clears the pending status — always in a {@code finally}, before
     * any completion write. The clearing sits outside the lock on purpose: with deregistration
     * committed, no concurrent {@link #requestCancellation} can interrupt any more, so
     * {@code Thread.interrupted()} erases whatever may have arrived.
     */
    void unregisterHandlerThreadAndClearInterrupt() {
        lock.lock();
        try {
            handlerThread = null;
            handlerFinished = true;
        } finally {
            lock.unlock();
        }
        Thread.interrupted();
    }

    /**
     * The timeout clock: {@code true} if the handler has started, has not finished, and has been
     * running for longer than {@code duration} — monotonic time ({@code System.nanoTime}), never the
     * injected {@code Clock} (a duration must not jump on a resync, a project invariant).
     */
    boolean handlerRunningLongerThan(Duration duration) {
        return handlerStarted && !handlerFinished
                && System.nanoTime() - handlerStartedNanos >= duration.toNanos();
    }
}
