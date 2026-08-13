package io.mohs.rest.job;

/** Discriminador de {@link ScheduleView} na borda JSON. */
public enum ScheduleType {
    CRON,
    INTERVAL,
    ON_DEMAND
}
