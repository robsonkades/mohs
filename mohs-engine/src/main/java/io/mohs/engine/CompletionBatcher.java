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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import io.mohs.core.execution.ExecutionId;
import io.mohs.engine.LeaseStore.Completion;
import io.mohs.engine.LeaseStore.CompletionResult;

/**
 * Bounded group completion. A transient failure retries the same group once after jittered backoff.
 * Unknown outcomes receive no fabricated verdict; unresolved grants become visible to recovery.
 * Close drains only within its budget. Later submissions leave their grants for recovery.
 * User effects may already have happened when persistence fails or the process stops.
 */
public final class CompletionBatcher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CompletionBatcher.class);
    private static final Duration IDLE_POLL = Duration.ofMillis(50);
    private record Pending(CompletionResult result, Consumer<Completion> onOutcome) {}
    private record Claim(ExecutionId executionId, String token) {
        static Claim of(CompletionResult result) { return new Claim(result.executionId(), result.claimToken()); }
    }
    private final LeaseStore leaseStore;
    private final int flushSize;
    private final Duration flushInterval;
    private final LinkedBlockingQueue<Pending> queue;
    private final Set<Claim> inTransit = ConcurrentHashMap.newKeySet();
    private final Thread flusher;
    private volatile boolean closed;
    private volatile boolean aborted;
    private volatile long stopAt = Long.MAX_VALUE;

    public CompletionBatcher(LeaseStore leaseStore, int flushSize, Duration flushInterval) {
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        if (flushSize < 1 || flushSize > 1000) throw new IllegalArgumentException("flushSize must be between 1 and 1000");
        if (!Objects.requireNonNull(flushInterval, "flushInterval").isPositive()) {
            throw new IllegalArgumentException("flushInterval must be positive");
        }
        flushInterval.toNanos();
        this.flushSize = flushSize;
        this.flushInterval = flushInterval;
        this.queue = new LinkedBlockingQueue<>(flushSize * 4);
        this.flusher = Thread.ofVirtual().name("mohs-completion-flusher").unstarted(this::flushLoop);
    }

    /** Safe to call repeatedly; construction never starts a thread. */
    public synchronized void start() {
        if (flusher.getState() == Thread.State.NEW) flusher.start();
    }

    /** Blocks on a full queue while running. The callback receives only a database verdict. */
    public void submit(CompletionResult result, Consumer<Completion> onOutcome) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(onOutcome, "onOutcome");
        if (closed || !inTransit.add(Claim.of(result))) return;
        var pending = new Pending(result, onOutcome);
        try {
            queue.put(pending);
        } catch (InterruptedException interrupted) {
            inTransit.remove(Claim.of(result));
            Thread.currentThread().interrupt();
            return;
        }
        // A close can drain between the first check and put. The queue decides who removes it.
        if (closed && queue.remove(pending)) inTransit.remove(Claim.of(result));
    }

    boolean completionInTransit(ExecutionId id, String claimToken) {
        return inTransit.contains(new Claim(id, claimToken));
    }

    private void flushLoop() {
        var buffer = new ArrayList<Pending>(flushSize);
        try {
            while (!aborted && !(closed && queue.isEmpty())) {
                try {
                    var first = queue.poll(IDLE_POLL.toNanos(), TimeUnit.NANOSECONDS);
                    if (first == null) continue;
                    buffer.add(first);
                    long deadline = System.nanoTime() + flushInterval.toNanos();
                    while (buffer.size() < flushSize && !aborted) {
                        Pending next = queue.poll();
                        if (next != null) { buffer.add(next); continue; }
                        long remaining = deadline - System.nanoTime();
                        if (closed || remaining <= 0) break;
                        next = queue.poll(Math.min(remaining, IDLE_POLL.toNanos()), TimeUnit.NANOSECONDS);
                        if (next != null) buffer.add(next);
                    }
                    flush(buffer);
                } catch (InterruptedException interrupted) {
                    aborted = true;
                } catch (Throwable failure) {
                    log.error("completion group unresolved; grants will be recovered", failure);
                } finally {
                    buffer.forEach(p -> inTransit.remove(Claim.of(p.result())));
                    buffer.clear();
                }
            }
        } finally {
            closed = true;
            abandonQueued();
        }
    }

    private void flush(List<Pending> buffer) throws InterruptedException {
        // Different grants of one execution need separate verdict maps. Preserve their input order.
        var group = new ArrayList<Pending>(buffer.size());
        var ids = new HashSet<ExecutionId>();
        for (var pending : buffer) {
            if (!ids.add(pending.result().executionId())) {
                if (!persist(group)) return;
                group.clear();
                ids.clear();
                ids.add(pending.result().executionId());
            }
            group.add(pending);
        }
        persist(group);
    }

    private boolean persist(List<Pending> group) throws InterruptedException {
        if (group.isEmpty()) return true;
        var results = group.stream().map(Pending::result).toList();
        for (int attempt = 0; attempt < 2 && withinBudget(); attempt++) {
            try {
                Map<ExecutionId, Completion> verdicts = leaseStore.complete(results);
                for (var pending : group) {
                    inTransit.remove(Claim.of(pending.result()));
                    var verdict = verdicts.get(pending.result().executionId());
                    if (verdict == null) throw new IllegalStateException("store omitted a completion verdict");
                    try {
                        pending.onOutcome().accept(verdict);
                    } catch (RuntimeException failure) {
                        log.error("completion is durable but its local observer failed", failure);
                    }
                }
                return true;
            } catch (RuntimeException failure) {
                boolean retryable = failure instanceof TransientDataAccessException
                        || failure instanceof DataAccessResourceFailureException
                        || failure instanceof RecoverableDataAccessException;
                if (!retryable || attempt == 1) {
                    log.warn("{} completions unresolved; no per-result fallback, recovery retains authority", group.size(), failure);
                    // Protect subsequent groups too: an unavailable database must not produce a busy loop.
                    if (retryable) backoff();
                    return false;
                }
                backoff();
            }
        }
        return false;
    }

    private boolean withinBudget() {
        return !aborted && (stopAt == Long.MAX_VALUE || stopAt - System.nanoTime() > 0);
    }

    private void backoff() throws InterruptedException {
        long nanos = TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(50, 151));
        if (stopAt != Long.MAX_VALUE) nanos = Math.min(nanos, Math.max(0, stopAt - System.nanoTime()));
        if (nanos > 0) TimeUnit.NANOSECONDS.sleep(nanos);
    }

    @Override
    public void close() { close(Duration.ofSeconds(10)); }

    /** No synchronous sweep after the deadline: an unresponsive driver cannot multiply the wait by queue size. */
    public void close(Duration drainBudget) {
        Objects.requireNonNull(drainBudget, "drainBudget");
        if (drainBudget.isNegative()) throw new IllegalArgumentException("drainBudget must be nonnegative");
        long budget = drainBudget.toNanos();
        long remaining;
        synchronized (this) {
            long now = System.nanoTime();
            // Concurrent/repeated close may shorten the common drain deadline, never extend it.
            if (stopAt == Long.MAX_VALUE || stopAt - now > budget) stopAt = now + budget;
            closed = true;
            if (aborted || flusher.getState() == Thread.State.TERMINATED) { abandonQueued(); return; }
            start();
            remaining = Math.max(0, stopAt - System.nanoTime());
        }
        try {
            if (!flusher.join(Duration.ofNanos(remaining))) {
                aborted = true;
                flusher.interrupt();
                log.warn("completion drain deadline reached; unresolved grants remain recoverable");
            }
        } catch (InterruptedException interrupted) {
            aborted = true;
            flusher.interrupt();
            Thread.currentThread().interrupt();
        } finally {
            abandonQueued();
        }
    }

    private void abandonQueued() {
        for (Pending pending; (pending = queue.poll()) != null;) inTransit.remove(Claim.of(pending.result()));
    }
}
