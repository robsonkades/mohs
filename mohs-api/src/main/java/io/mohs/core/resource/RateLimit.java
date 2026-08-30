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
package io.mohs.core.resource;

import java.time.Duration;
import java.util.Objects;

/**
 * The maximum throughput allowed over a shared resource within a time window — a cluster-wide cap,
 * but bounding rate rather than simultaneous concurrency.
 *
 * <p>A bean defines the structure and a property adjusts the numbers
 * ({@code mohs.rate-limits.<name>.max}/{@code .window}). This is a spec, never an actual limiter —
 * the engine is what enforces it.
 */
public record RateLimit(String name, int max, Duration window) {

    public RateLimit {
        Fields.requireNotBlank(name, "name");
        requireRefillable(max, window);
    }

    /**
     * The throughput rule isolated from the name, for callers that validate before having one
     * ({@code PATCH /rate-limits/{name}} takes the name from the path, not the body). A single
     * source: the wire and the record both enforce THIS, never two copies that diverge by the third
     * edit.
     *
     * <p>The ceiling on {@code max} relative to the window in nanoseconds is what stops
     * {@code window.dividedBy(max)} from truncating to zero: the bucket's refill interval must be
     * representable, and a division by {@code Duration.ZERO} in there would bring down the ENTIRE
     * claim round — including jobs with no limit at all.
     */
    public static void requireRefillable(int max, Duration window) {
        if (max < 1) {
            throw new IllegalArgumentException("max must be at least 1");
        }
        Objects.requireNonNull(window, "window");
        if (!window.isPositive()) {
            throw new IllegalArgumentException("window must be positive");
        }
        if (window.compareTo(Duration.ofNanos(max)) < 0) {
            throw new IllegalArgumentException("window " + window + " is too short for max " + max
                    + " — one token is issued every window/max, which needs at least " + max + "ns of window");
        }
    }
}
