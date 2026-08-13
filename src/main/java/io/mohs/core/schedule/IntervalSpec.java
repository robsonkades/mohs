package io.mohs.core.schedule;

import java.time.Duration;
import java.util.Objects;

/**
 * Agenda baseada em intervalo: fixed-rate quando {@code afterFinish} é
 * {@code false} (ancorado no horário de disparo agendado), fixed-delay
 * quando {@code true} (ancorado no fim da execução anterior).
 */
public record IntervalSpec(Duration interval, boolean afterFinish) implements Schedule {

    public IntervalSpec {
        Objects.requireNonNull(interval, "interval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }
}
