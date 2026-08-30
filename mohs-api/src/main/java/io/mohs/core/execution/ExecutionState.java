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
package io.mohs.core.execution;

import io.mohs.core.event.ExecutionEvent;

/**
 * The lifecycle state of an {@link Execution}. Not enumerated explicitly in the design documents;
 * derived here from the {@link ExecutionEvent} variants and the {@code GET /executions?status=}
 * filter of the REST design.
 *
 * <p>{@code RETRY_WAITING} (formerly {@code RETRY_SCHEDULED}) is an execution waiting out a retry's
 * backoff, and it is <b>not claimable</b>: when the backoff expires it returns to {@link #ENQUEUED},
 * and the queue has a single admission rule ({@code visible_at <= now}).
 *
 * <p>The rename carries the change in meaning: "scheduled" promised a state the claim read, while
 * "waiting" says what it is — a wait.
 */
public enum ExecutionState {
    ENQUEUED,
    RUNNING,
    RETRY_WAITING,
    SUCCEEDED,
    FAILED,
    CANCELLED
}
