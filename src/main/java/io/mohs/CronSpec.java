package io.mohs;

import java.time.ZoneId;
import java.util.Objects;

/**
 * Cron-based schedule: Quartz-style, seconds-first expression, evaluated in
 * the given {@link ZoneId} — never the JVM default zone.
 */
public record CronSpec(String expression, ZoneId zone) implements Schedule {

    public CronSpec {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(zone, "zone");
        if (expression.isBlank()) {
            throw new IllegalArgumentException("expression must not be blank");
        }
    }
}
