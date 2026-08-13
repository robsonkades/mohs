package io.mohs.rest.job;

import java.time.Duration;
import java.util.Objects;

/** Forma de wire de {@link io.mohs.core.schedule.IntervalSpec}. */
public record IntervalView(ScheduleType type, Duration interval, boolean afterFinish) implements ScheduleView {

    public IntervalView {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(interval, "interval");
    }

    public IntervalView(Duration interval, boolean afterFinish) {
        this(ScheduleType.INTERVAL, interval, afterFinish);
    }
}
