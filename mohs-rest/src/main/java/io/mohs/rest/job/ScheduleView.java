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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;

/**
 * The wire form of {@link Schedule} — sealed 1:1 with the domain, in the same spirit: an exhaustive
 * switch in {@link #from(Schedule)}, and a fourth type is a compilation error until it is handled.
 *
 * <p>{@code type} is an explicit discriminator in the JSON rather than something inferred from the
 * class name, which keeps it portable across client languages.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CronView.class, name = "CRON"),
        @JsonSubTypes.Type(value = IntervalView.class, name = "INTERVAL"),
        @JsonSubTypes.Type(value = OnDemandView.class, name = "ON_DEMAND")
})
public sealed interface ScheduleView permits CronView, IntervalView, OnDemandView {

    /**
     * Returns the discriminator of this schedule variant.
     *
     * @return the schedule variant discriminator
     */
    ScheduleType type();

    /**
     * Converts the supplied snapshot to its REST representation.
     *
     * @param schedule the firing schedule to evaluate
     * @return the corresponding REST representation
     */
    static ScheduleView from(Schedule schedule) {
        return switch (schedule) {
            case CronSpec cron -> new CronView(cron.expression(), cron.zone());
            case IntervalSpec interval -> new IntervalView(interval.interval(), interval.afterFinish());
            case OnDemandSpec _ -> new OnDemandView();
        };
    }

    /**
     * The inverse of {@link #from} — the request body of {@code PATCH .../schedule} translated back into the domain.
     *
     * @return the corresponding scheduling specification
     */
    default Schedule toSchedule() {
        return switch (this) {
            case CronView cron -> new CronSpec(cron.expression(), cron.zone());
            case IntervalView interval -> new IntervalSpec(interval.interval(), interval.afterFinish());
            case OnDemandView _ -> new OnDemandSpec();
        };
    }
}
