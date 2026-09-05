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
package io.mohs.core.schedule;

import java.time.Duration;
import java.util.Objects;

/**
 * An interval-based schedule: fixed-rate when {@code afterFinish} is {@code false} (anchored to the
 * scheduled firing time), fixed-delay when {@code true} (anchored to the end of the previous
 * execution).
 *
 * @param interval the positive interval between firings
 * @param afterFinish whether the interval starts at completion rather than the scheduled firing instant
 */
public record IntervalSpec(Duration interval, boolean afterFinish) implements Schedule {

    /**
     * Creates a {@code IntervalSpec} with the supplied values.
     *
     * @param interval the positive interval between firings
     * @param afterFinish whether the interval starts at completion rather than the scheduled firing instant
     */
    public IntervalSpec {
        Objects.requireNonNull(interval, "interval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }
}
