package io.mohs.core;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Temporal contract shared by submissions, receipts and relational persistence.
 */
public final class TimePrecision {
    private static final Instant MIN = Instant.parse("1000-01-01T00:00:00Z");
    private static final Instant MAX = Instant.parse("9999-12-31T23:59:59.999999Z");

    private TimePrecision() {
    }

    /**
     * Earliest representable instant that does not precede the requested eligibility time.
     */
    public static Instant eligible(Instant instant) {
        Objects.requireNonNull(instant, "instant");
        Instant rounded = instant.truncatedTo(ChronoUnit.MICROS);
        if (!rounded.equals(instant)) rounded = rounded.plus(1, ChronoUnit.MICROS);
        return inRange(rounded);
    }

    /**
     * An observed time is rounded down so a query cannot make future work eligible.
     */
    public static Instant observed(Instant instant) {
        return inRange(Objects.requireNonNull(instant, "instant").truncatedTo(ChronoUnit.MICROS));
    }

    /**
     * Exact signed duration representation. Each caller defines whether zero/negative is allowed.
     */
    public static long micros(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        if (duration.getNano() % 1000 != 0)
            throw new IllegalArgumentException("duration requires sub-microsecond precision");
        try {
            long seconds = duration.getSeconds();
            int fraction = duration.getNano() / 1000;
            return seconds < 0 && fraction > 0
                    ? Math.addExact(Math.multiplyExact(seconds + 1, 1_000_000L), fraction - 1_000_000L)
                    : Math.addExact(Math.multiplyExact(seconds, 1_000_000L), fraction);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("duration exceeds the signed microsecond range", overflow);
        }
    }

    /**
     * Inverse of {@link #micros(Duration)} without an intermediate nanosecond overflow.
     */
    public static Duration duration(long micros) {
        return Duration.ofSeconds(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000);
    }

    private static Instant inRange(Instant instant) {
        if (instant.isBefore(MIN) || instant.isAfter(MAX)) {
            throw new IllegalArgumentException("instant must be within UTC years 1000 through 9999 at microsecond precision");
        }
        return instant;
    }
}
