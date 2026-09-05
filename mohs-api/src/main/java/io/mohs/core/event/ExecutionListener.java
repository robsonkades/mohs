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
package io.mohs.core.event;

/**
 * An Observer (GoF) of execution events — it observes, never interferes.
 *
 * <p>A listener's exception is caught and logged by the engine and never affects the job. Delivery
 * is best-effort and asynchronous; a guaranteed reaction does not use a listener — the handler
 * enqueues the continuation inside its own transaction (the transactional outbox clause of the
 * async contract).
 *
 * <p>Every outcome publishes: local dispatch and the reaper's reclaim (a dead node) go through the
 * same pipeline. Documented limitation: there is no ordering guarantee between events of the same
 * execution, delivery being asynchronous per listener — {@code RetryScheduled} may arrive before
 * the {@code AttemptFailed} that causally precedes it.
 */
@FunctionalInterface
public interface ExecutionListener {
    /**
     * Observes an execution event delivered by the event executor.
     *
     * @param event the execution event to observe
     */
    void on(ExecutionEvent event);
}
