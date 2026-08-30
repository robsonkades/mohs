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

import java.time.ZoneId;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Forma de wire de {@link io.mohs.core.schedule.CronSpec}. */
public record CronView(ScheduleType type, String expression, ZoneId zone) implements ScheduleView {

    /**
     * Marks the canonical constructor explicitly as Jackson's creator — without it, the ambiguity
     * with the two-argument convenience constructor below makes Jackson pick the wrong one when
     * deserialising (failing while trying to populate {@code type}, which records do not expose
     * through a setter).
     */
    @JsonCreator
    public CronView {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(zone, "zone");
    }

    public CronView(String expression, ZoneId zone) {
        this(ScheduleType.CRON, expression, zone);
    }
}
