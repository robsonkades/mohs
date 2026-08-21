package io.mohs.rest.job;

import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Forma de wire de {@link io.mohs.core.schedule.IntervalSpec}. */
public record IntervalView(ScheduleType type, Duration interval, boolean afterFinish) implements ScheduleView {

    /** Ver {@link CronView} — mesma razão para o {@code @JsonCreator} explícito no construtor canônico. */
    @JsonCreator
    public IntervalView {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(interval, "interval");
    }

    public IntervalView(Duration interval, boolean afterFinish) {
        this(ScheduleType.INTERVAL, interval, afterFinish);
    }
}
