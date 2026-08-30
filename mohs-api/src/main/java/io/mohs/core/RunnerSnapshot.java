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
package io.mohs.core;

import java.util.Objects;

import io.mohs.core.resource.RunnerMode;

/**
 * A runner on this node: its declared configuration plus how much work it is carrying right now.
 *
 * <p><strong>Node-local by nature.</strong> A runner is a thread pool, and a thread pool does not
 * exist in the cluster — it exists in a process. Two instances of the same runner on different
 * nodes have independent occupancy, and there is no sum that would mean anything: whoever reads
 * this is looking at the node that answered, not at the system. That is the difference between this
 * list and {@link Mohs#nodes() nodes}/{@link Mohs#jobs() jobs}, which are shared state in the
 * database.
 *
 * <p>{@code max} is the declared ceiling, derived from the mode: {@code maxConcurrent} for
 * {@link RunnerMode#IO}, {@code maxSize} for {@link RunnerMode#CPU}.
 *
 * <p>{@code running} counts what has been <em>accepted and has not finished</em>. In {@code IO} mode
 * that is effectively what is executing: there is no queue between accepting and executing — the
 * ceiling is a semaphore, and above it the executor REJECTS ({@code TaskRejectedException}) instead
 * of queueing or making the submitter wait. In {@code CPU} mode the queue sits between accepting and
 * executing, and the number includes what waits in it — measuring only what occupies a thread would
 * hide exactly the backlog the operator needs to see, which is why {@code running} can exceed
 * {@code max} in that mode.
 *
 * <p>A separate {@code queued} component was considered and refused: two numbers for the same fact
 * ({@code running} is already the sum) would force every consumer to know that only one of them
 * applies per mode, and adding a component to a public record breaks the canonical constructor and
 * deconstruction patterns — backing out later costs more than explaining now. Anyone needing the
 * backlog computes {@code running - max} in {@code CPU} mode, where it is the difference's only
 * possible meaning.
 */
public record RunnerSnapshot(String name, RunnerMode mode, int max, int running) {

    public RunnerSnapshot {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(mode, "mode");
        if (max <= 0) {
            throw new IllegalArgumentException("max must be positive: " + max);
        }
        if (running < 0) {
            throw new IllegalArgumentException("running must not be negative: " + running);
        }
    }
}
