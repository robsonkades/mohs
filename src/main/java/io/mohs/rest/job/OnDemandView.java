package io.mohs.rest.job;

import java.util.Objects;

/** Forma de wire de {@link io.mohs.core.schedule.OnDemandSpec}. */
public record OnDemandView(ScheduleType type) implements ScheduleView {

    public OnDemandView {
        Objects.requireNonNull(type, "type");
    }

    public OnDemandView() {
        this(ScheduleType.ON_DEMAND);
    }
}
