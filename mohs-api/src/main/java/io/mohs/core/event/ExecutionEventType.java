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
 * Mirrors the {@link ExecutionEvent} variants one for one. It exists only because
 * {@link OnExecution#event()}, being an annotation attribute, cannot reference the permitted sealed
 * record it wants to filter on directly.
 *
 * <p>The names derive from the RECORD, not from {@code ExecutionState}: the
 * {@code RETRY_SCHEDULED} to {@code RETRY_WAITING} rename belonged to the STATE ("the execution is
 * waiting") and bled through to here, letting a user write
 * {@code @OnExecution(event = RETRY_WAITING)} and match {@code case RetryScheduled} in a pattern —
 * two names for the same thing. An event and a state are different things; here the event governs.
 */
public enum ExecutionEventType {
    /**
     * Matches scheduling receipts published after durable enqueue.
     */
    ENQUEUED,
    /**
     * Matches the start of an execution attempt.
     */
    STARTED,
    /**
     * Matches an attempt failure before its retry outcome.
     */
    ATTEMPT_FAILED,
    /**
     * Matches a retry becoming scheduled.
     */
    RETRY_SCHEDULED,
    /**
     * Matches terminal success.
     */
    SUCCEEDED,
    /**
     * Matches terminal failure.
     */
    FAILED,
    /**
     * Matches cooperative cancellation.
     */
    CANCELLED,
    /**
     * Matches a batch whose members have all finished.
     */
    BATCH_COMPLETED
}
