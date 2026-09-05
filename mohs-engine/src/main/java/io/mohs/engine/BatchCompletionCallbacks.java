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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionListener;

/**
 * The continuations registered through {@code Batch.onCompletion} — an {@link ExecutionListener}
 * like any other, not a parallel path: the {@code BatchCompleted} that fires the callbacks is
 * exactly the event the {@code Dispatcher} publishes when a completion closes the batch, so delivery
 * inherits the listener contract — asynchronous, best-effort, with no ordering guarantee.
 *
 * <p>Genuinely best-effort: the registration lives only in this JVM. A batch closed by ANOTHER node
 * publishes the event there, not here, and the callback does not run. Anyone needing a guaranteed
 * reaction enqueues a job inside the transaction — that is in {@code Batch#onCompletion}'s Javadoc,
 * and it is why this registry can be an in-memory map rather than persisted state.
 *
 * <p>A callback leaves the map when its batch closes — but only on THIS node: a batch closed
 * elsewhere publishes the event there, and the entry here would never be removed. In an N-node
 * cluster, roughly (N-1)/N of the registrations would stay resident forever in a singleton bean.
 * Hence the LRU ceiling: delivery is already best-effort and this-JVM-only by contract, so
 * discarding the oldest registration is honest degradation rather than new loss.
 *
 * <p>A callback that throws is logged and swallowed: one broken observer must not take down delivery
 * to the others, nor the event for the other listeners.
 */
public final class BatchCompletionCallbacks implements ExecutionListener {

    /**
     * Creates an empty batch callback registry.
     */
    public BatchCompletionCallbacks() {
    }

    private static final Logger log = LoggerFactory.getLogger(BatchCompletionCallbacks.class);

    /** The LRU's ceiling — live batches observed in this JVM; above it, the oldest is evicted. */
    static final int MAX_TRACKED_BATCHES = 10_000;

    private final ReentrantLock lock = new ReentrantLock();

    /** A LinkedHashMap in LRU mode under a lock (JCIP ch. 13) — ConcurrentHashMap has no eviction. */
    private final Map<String, List<Consumer<BatchCompleted>>> byBatchId =
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, List<Consumer<BatchCompleted>>> eldest) {
                    return size() > MAX_TRACKED_BATCHES;
                }
            };

    void register(String batchId, Consumer<BatchCompleted> callback) {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(callback, "callback");
        lock.lock();
        try {
            byBatchId.computeIfAbsent(batchId, _ -> new CopyOnWriteArrayList<>()).add(callback);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void on(ExecutionEvent event) {
        if (!(event instanceof BatchCompleted completed)) {
            return;
        }
        List<Consumer<BatchCompleted>> callbacks;
        lock.lock();
        try {
            callbacks = byBatchId.remove(completed.batchId());
        } finally {
            lock.unlock();
        }
        if (callbacks == null) {
            return;
        }
        for (Consumer<BatchCompleted> callback : callbacks) {
            try {
                callback.accept(completed);
            } catch (RuntimeException e) {
                log.error("onCompletion callback for batch {} threw — the batch is complete regardless, "
                        + "and the remaining callbacks still run", completed.batchId(), e);
            }
        }
    }
}
