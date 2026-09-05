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

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.mohs.core.execution.ExecutionState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * The contract of {@code GET /overview} lives here: live work only, always the three states (zero
 * included), in lifecycle order — plus the {@link ThroughputReading} accompanying it, whose point is
 * to give the RATE the live counts cannot.
 */
class OverviewSnapshotTest {

    private static final ThroughputReading MINUTE = new ThroughputReading(Duration.ofSeconds(60), 0L, 0L);
    private static final ThroughputReading RECENT = new ThroughputReading(Duration.ofSeconds(10), 0L, 0L);

    @Test
    void normalizesTheThreeLiveStatesWithZeroForAbsentKeys() {
        OverviewSnapshot snapshot = new OverviewSnapshot(Map.of(ExecutionState.RUNNING, 3L), MINUTE, RECENT);

        assertThat(snapshot.executionCountsByState()).containsExactly(
                entry(ExecutionState.ENQUEUED, 0L),
                entry(ExecutionState.RUNNING, 3L),
                entry(ExecutionState.RETRY_WAITING, 0L));
    }

    /** The decision defended in code: an all-time terminal count does not enter — unbounded history on a polling anchor. */
    @Test
    void rejectsTerminalStateKeys() {
        assertThatThrownBy(() -> new OverviewSnapshot(Map.of(ExecutionState.SUCCEEDED, 1L), MINUTE, RECENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SUCCEEDED");
    }

    @Test
    void rejectsNonPositiveWindow() {
        assertThatThrownBy(() -> new ThroughputReading(Duration.ZERO, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ThroughputReading(Duration.ofSeconds(-1), 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void exposesAnImmutableMap() {
        OverviewSnapshot snapshot = new OverviewSnapshot(Map.of(), MINUTE, RECENT);

        assertThatThrownBy(() -> snapshot.executionCountsByState().put(ExecutionState.ENQUEUED, 9L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * The point of the short reading: it exists to be DIVIDED. Without it the dashboard has only
     * instantaneous gauges, which by Little's Law sit at zero for a fast job — so "idle" and
     * "working quickly" become the same screen.
     */
    @Test
    void theRecentReadingYieldsARate() {
        ThroughputReading recent = new ThroughputReading(Duration.ofSeconds(10), 47L, 3L);

        assertThat(recent.perSecond()).isEqualTo(5.0);
    }

    /** Throughput is successes AND failures: a system that only fails is not idle. */
    @Test
    void theRateCountsFailuresAsWork() {
        assertThat(new ThroughputReading(Duration.ofSeconds(2), 0L, 8L).perSecond()).isEqualTo(4.0);
    }
}
