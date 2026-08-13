package io.mohs.rest.job;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Forma de wire de {@link io.mohs.core.schedule.OnDemandSpec}. */
public record OnDemandView(ScheduleType type) implements ScheduleView {

    /** Ver {@link CronView} — mesma razão para o {@code @JsonCreator} explícito no construtor canônico. */
    @JsonCreator
    public OnDemandView {
        Objects.requireNonNull(type, "type");
    }

    public OnDemandView() {
        this(ScheduleType.ON_DEMAND);
    }
}
