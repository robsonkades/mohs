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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.mohs.core.execution.ExecutionState;

/**
 * The aggregate view behind {@link Mohs#overview} — the dashboard's polling anchor
 * ({@code GET /overview}): live-work counts and terminal throughput.
 *
 * <p>Terminal states deliberately have no all-time count: history grows without bound and this is
 * queried by polling, so the cost has to be proportional to the live work and to the window, never
 * to the table's size — cheap by construction.
 *
 * <p>{@code executionCountsByState} always carries the three live states ({@code ENQUEUED},
 * {@code RUNNING}, {@code RETRY_WAITING}), with zero when there is no row — an absent key and a
 * zero are the same information, and a polling contract should not force the consumer to tell them
 * apart.
 *
 * <h2>Why TWO throughput readings</h2>
 *
 * <p>The live counts are instantaneous gauges, and by Little's Law ({@code L = lambda x W}) they
 * are about zero for any fast job: a 1ms job fired every second has an average concurrency of
 * 0.001, so sampling every 2s finds it in one reading out of a thousand. Measured on the dashboard:
 * 4 samples with live work out of 60, peaking at 1, while 39 executions completed per minute.
 * Anyone receiving only {@code L} concludes the system is idle while it works.
 *
 * <p>Hence {@link #recent}: a SHORT window of fixed size whose only purpose is to be divided —
 * {@link ThroughputReading#perSecond()} yields the missing lambda. It cannot be derived from
 * {@link #throughput}, which is a long sliding window: differencing two consecutive readings gives
 * {@code (what entered) - (what left through the other end)}, which is zero in steady state. The
 * two coexist because they answer different questions: {@code throughput} is "how much was done in
 * the last hour", {@code recent} is "is anything happening right now".
 *
 * <p><b>The two are NOT nested</b> — {@code recent} is not a slice of {@code throughput}, and that
 * is contract, not detail. Two independent reasons. The long window is chosen by the caller and may
 * be SHORTER than the short one's 10s ({@code ?window=1s} is valid over REST). And the counts are
 * separate round trips, in distinct snapshots: whatever finishes between them enters one and not
 * the other — measured, about 19 rows of asymmetry per call at the 4k/s operating point. Adding,
 * subtracting or stacking one on the other produces a negative number sooner or later; each reading
 * answers its own question, on its own.
 */
public record OverviewSnapshot(Map<ExecutionState, Long> executionCountsByState, ThroughputReading throughput,
        ThroughputReading recent) {

    /** The states the map always carries — the live work. */
    private static final Set<ExecutionState> ACTIVE_STATES =
            Set.of(ExecutionState.ENQUEUED, ExecutionState.RUNNING, ExecutionState.RETRY_WAITING);

    public OverviewSnapshot {
        Objects.requireNonNull(executionCountsByState, "executionCountsByState");
        Objects.requireNonNull(throughput, "throughput");
        Objects.requireNonNull(recent, "recent");
        for (ExecutionState state : executionCountsByState.keySet()) {
            if (!ACTIVE_STATES.contains(state)) {
                throw new IllegalArgumentException("executionCountsByState only carries live work, got " + state
                        + " — all-time terminal counts are excluded by contract (unbounded history on a polling anchor)");
            }
        }
        // EnumMap plus unmodifiableMap rather than Map.copyOf: the enum's declaration order
        // (ENQUEUED before RUNNING before RETRY_WAITING) is the lifecycle order, and an order
        // exposed through an API is contract.
        Map<ExecutionState, Long> normalized = new EnumMap<>(ExecutionState.class);
        for (ExecutionState state : ACTIVE_STATES) {
            normalized.put(state, executionCountsByState.getOrDefault(state, 0L));
        }
        executionCountsByState = Collections.unmodifiableMap(normalized);
    }
}
