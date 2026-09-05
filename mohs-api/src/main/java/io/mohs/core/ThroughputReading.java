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
import java.util.Objects;

/**
 * Terminal executions counted within a window — the pair of "what finished" and "over how long".
 *
 * <p>It exists because {@link OverviewSnapshot} carries TWO readings of the same shape for different
 * purposes, and three loose components repeated twice would be a Data Clump (Fowler).
 *
 * <p>The window travels with the count on purpose: a count without a window is not interpretable,
 * and it is the window that lets the consumer derive a RATE
 * ({@code (succeeded + failed) / window}) instead of receiving an absolute number that only means
 * something if they know how long it was measured over.
 *
 * @param window the positive duration over which completions were counted
 * @param succeeded the number of successful terminal executions
 * @param failed the number of failed terminal executions
 */
public record ThroughputReading(Duration window, long succeeded, long failed) {

    /**
     * Creates a {@code ThroughputReading} with the supplied values.
     *
     * @param window the positive duration over which completions were counted
     * @param succeeded the number of successful terminal executions
     * @param failed the number of failed terminal executions
     */
    public ThroughputReading {
        Objects.requireNonNull(window, "window");
        if (!window.isPositive()) {
            throw new IllegalArgumentException("window must be positive, got " + window);
        }
        if (succeeded < 0 || failed < 0) {
            throw new IllegalArgumentException("counters must not be negative");
        }
    }

    /**
     * Terminal executions per second in this window — successes and failures added together: this is
     * throughput, not a success rate.
     *
     * <p>Those two are the only terminal states that exist today. If {@code CANCELLED} appears, it
     * stays OUT of a historical throughput (throughput is completed work, and cancelling is an
     * operator's decision) but must come IN here — the short window's question is "is anything
     * happening", and a drain cancelling 100k executions is a great deal happening.
     *
     * <p>{@code toSeconds() + toNanosPart()} rather than {@code toNanos()}: the latter overflows
     * above roughly 292 years of window, and the constructor only requires a positive one. Through
     * REST that is unreachable (clamped to PT1S-PT1H), but {@code Mohs#overview} is a public facade
     * and accepts any duration — a record that promises to validate at construction cannot have a
     * partial method.
     *
     * @return successful plus failed completions divided by the window in seconds
     */
    public double perSecond() {
        double seconds = window.toSeconds() + window.toNanosPart() / 1_000_000_000.0;
        return (succeeded + failed) / seconds;
    }
}
