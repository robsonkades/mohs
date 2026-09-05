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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;

import org.springframework.core.task.AsyncTaskExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.core.event.ExecutionEvent;
import io.mohs.core.event.ExecutionListener;

/**
 * Delivers an {@link ExecutionEvent} to the registered {@link ExecutionListener}s — best-effort and
 * asynchronous, on a dedicated virtual thread per publication (the contract already documented on
 * {@link ExecutionListener}: "it observes, never interferes"). A listener's exception is caught and
 * logged, never propagated — a broken listener must not affect the job's result.
 *
 * <p>{@code executor} is injected rather than created here — {@link MohsExecutors} is what knows how
 * to build a virtual-thread executor with a real concurrency ceiling; this class only consumes it.
 * It does not close the executor it receives: the lifecycle belongs to whoever built it.
 *
 * <p>Package-private: its consumers are {@link Dispatcher}, for everything an attempt goes through,
 * and the facade ({@link MohsImpl}/{@link ScheduleCommandImpl}), for the {@code Enqueued} born on
 * the scheduling side. It opens up to {@code public} the day code outside this package (a manual
 * retry through REST, say) needs to publish events too — YAGNI until then.
 */
final class ExecutionEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEventPublisher.class);

    private final List<ExecutionListener> listeners;
    private final AsyncTaskExecutor executor;

    ExecutionEventPublisher(List<ExecutionListener> listeners, AsyncTaskExecutor executor) {
        this.listeners = List.copyOf(Objects.requireNonNull(listeners, "listeners"));
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /**
     * The observation pipeline never exerts backpressure on the control pipeline: a saturated
     * executor drops the event with a WARN here — the boundary that drops is the publisher, never an
     * exception climbing up through the poll loop (a mass reclaim publishing 2x500 events must not
     * hijack the tick) nor through the dispatch's completion path.
     */
    void publish(ExecutionEvent event) {
        Objects.requireNonNull(event, "event");
        for (ExecutionListener listener : listeners) {
            try {
                executor.execute(() -> {
                    try {
                        listener.on(event);
                    } catch (RuntimeException e) {
                        log.warn("execution listener {} threw for event {} — ignored, listener exceptions never affect the job", listener.getClass().getName(), event, e);
                    }
                });
            } catch (RejectedExecutionException e) {
                log.warn("event executor saturated — dropping {} for listener {} (delivery is best-effort by contract)",
                        event.getClass().getSimpleName(), listener.getClass().getName());
            }
        }
    }
}
