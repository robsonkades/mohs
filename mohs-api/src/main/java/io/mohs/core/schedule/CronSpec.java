package io.mohs.core.schedule;

import java.time.ZoneId;
import java.util.Objects;

/**
 * Agenda baseada em cron: sintaxe estilo Quartz, seconds-first, avaliada no
 * {@link ZoneId} informado — nunca no zone default da JVM.
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
