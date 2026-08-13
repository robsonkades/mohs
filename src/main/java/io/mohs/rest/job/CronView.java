package io.mohs.rest.job;

import java.time.ZoneId;
import java.util.Objects;

/** Forma de wire de {@link io.mohs.core.schedule.CronSpec}. */
public record CronView(ScheduleType type, String expression, ZoneId zone) implements ScheduleView {

    public CronView {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(zone, "zone");
    }

    public CronView(String expression, ZoneId zone) {
        this(ScheduleType.CRON, expression, zone);
    }
}
