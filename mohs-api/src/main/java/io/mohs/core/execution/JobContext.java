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

import java.time.Instant;

import io.mohs.core.job.JobKey;

/**
 * The handler's optional parameter, carrying per-attempt context: identity, timing, cooperative
 * cancellation and progress reporting.
 *
 * <p>Deliberately a plain interface rather than a fluent one — this lives on the hot path, and
 * beyond the Effective Java discipline a DSL here would only pollute stack traces.
 */
public interface JobContext {

    JobKey jobKey();

    ExecutionId executionId();

    /** 1-based; a retry increments this, while the execution's id stays the same. */
    int attempt();

    Instant scheduledAt();

    /**
     * The instant THIS attempt began being dispatched — not {@code Execution}'s {@code fired_at}
     * column, which records the claim (tens of milliseconds earlier, under load).
     */
    Instant firedAt();

    /**
     * Cooperative cancellation: becomes {@code true} when the job's timeout fires, when shutdown
     * escalates after the drain grace expires, or when a {@code POST /executions/{id}/cancel} is
     * observed by the owning node (staleness of at most one loop interval — from
     * {@code mohs.engine.poll-interval} to {@code mohs.engine.max-poll-interval}, depending on the
     * backoff).
     *
     * <p>The handler decides when and how to stop: throwing after observing a manual cancel ends the
     * execution as {@code CANCELLED}, while completing normally records {@code SUCCEEDED} —
     * finished work counts, even with a request pending. Timeout and shutdown additionally deliver
     * {@code Thread.interrupt()}; a manual cancel does not, being a pure flag.
     */
    boolean cancellationRequested();

    /** Opcional, voltado ao dashboard. No-op se nada observar. */
    void progress(int done, int total);
}
