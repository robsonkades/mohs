package io.mohs;

import java.time.Duration;
import java.util.Objects;

/**
 * Interval-based schedule: fixed-rate when {@code afterFinish} is
 * {@code false} (anchored to the scheduled fire time), fixed-delay when
 * {@code true} (anchored to the previous execution's end).
 */
public record IntervalSpec(Duration interval, boolean afterFinish) implements Schedule {

    public IntervalSpec {
        Objects.requireNonNull(interval, "interval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }
}
