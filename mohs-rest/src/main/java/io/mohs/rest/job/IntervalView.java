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
package io.mohs.rest.job;

import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;

/** The wire form of {@link io.mohs.core.schedule.IntervalSpec}. */
public record IntervalView(ScheduleType type, Duration interval, boolean afterFinish) implements ScheduleView {

    /** See {@link CronView} — the same reason for the explicit {@code @JsonCreator} on the canonical constructor. */
    @JsonCreator
    public IntervalView {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(interval, "interval");
    }

    public IntervalView(Duration interval, boolean afterFinish) {
        this(ScheduleType.INTERVAL, interval, afterFinish);
    }
}
