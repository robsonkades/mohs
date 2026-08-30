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

import java.time.Duration;
import java.util.Objects;

import io.mohs.core.ThroughputReading;

/**
 * One window's throughput — part of {@link OverviewResponse}, which carries TWO: the long one ("how
 * much was done") and the short one ("is anything happening right now").
 */
public record ThroughputView(Duration window, long succeeded, long failed, double ratePerSecond) {

    public ThroughputView {
        Objects.requireNonNull(window, "window");
    }

    /**
     * The domain reading in wire format. {@code ratePerSecond} travels already computed: the division
     * is trivial, but a JSON consumer would have to parse the ISO-8601 duration to do it — and a
     * client that gets that arithmetic wrong draws a wrong chart with nothing to flag it. The
     * denominator stays in the response for the consumer to check.
     */
    static ThroughputView from(ThroughputReading reading) {
        return new ThroughputView(reading.window(), reading.succeeded(), reading.failed(), reading.perSecond());
    }
}
