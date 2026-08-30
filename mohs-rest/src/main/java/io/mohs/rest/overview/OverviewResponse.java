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
package io.mohs.rest.overview;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import io.mohs.core.OverviewSnapshot;
import io.mohs.core.execution.ExecutionState;

/**
 * The dashboard's polling anchor: cheap by construction — counts by status plus the recent window's
 * throughput ({@code GET /overview}).
 *
 * <p>It is the flat form of {@link OverviewSnapshot}, which is what defines the contract: only live
 * states appear in the map (always all three, zeros included), while recent terminal activity is in
 * {@link ThroughputView}.
 */
public record OverviewResponse(Map<ExecutionState, Long> executionCountsByStatus, ThroughputView throughput,
        ThroughputView recent) {

    public OverviewResponse {
        Objects.requireNonNull(executionCountsByStatus, "executionCountsByStatus");
        Objects.requireNonNull(throughput, "throughput");
        Objects.requireNonNull(recent, "recent");
        // EnumMap plus unmodifiableMap rather than Map.copyOf: it preserves the enum's declaration
        // order in the JSON — an order exposed through an API is contract.
        Map<ExecutionState, Long> ordered = new EnumMap<>(ExecutionState.class);
        ordered.putAll(executionCountsByStatus);
        executionCountsByStatus = Collections.unmodifiableMap(ordered);
    }

    static OverviewResponse from(OverviewSnapshot snapshot) {
        return new OverviewResponse(snapshot.executionCountsByState(),
                ThroughputView.from(snapshot.throughput()), ThroughputView.from(snapshot.recent()));
    }
}
