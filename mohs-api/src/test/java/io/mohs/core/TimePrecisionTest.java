package io.mohs.core;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimePrecisionTest {
    @Test
    void eligibleRoundsUpAndObservationDownIncludingBeforeEpoch() {
        for (var instant : new Instant[]{Instant.parse("2026-09-09T00:00:00.123456001Z"), Instant.parse("1960-01-01T00:00:00.123456001Z")}) {
            assertThat(TimePrecision.eligible(instant)).isAfter(instant);
            assertThat(TimePrecision.observed(instant)).isBefore(instant);
            assertThat(Duration.between(TimePrecision.observed(instant), TimePrecision.eligible(instant))).isEqualTo(Duration.ofNanos(1000));
        }
    }

    @Test
    void signedMicrosecondsRoundTripAtBothLongLimits() {
        for (long value : new long[]{Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
            assertThat(TimePrecision.micros(TimePrecision.duration(value))).isEqualTo(value);
        }
        assertThatThrownBy(() -> TimePrecision.micros(Duration.ofNanos(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TimePrecision.micros(Duration.ofSeconds(Long.MAX_VALUE))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void commonDatabaseRangeIsEnforcedAfterRounding() {
        assertThat(TimePrecision.eligible(Instant.parse("9999-12-31T23:59:59.999999Z"))).isEqualTo(Instant.parse("9999-12-31T23:59:59.999999Z"));
        assertThatThrownBy(() -> TimePrecision.eligible(Instant.parse("9999-12-31T23:59:59.999999001Z"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TimePrecision.observed(Instant.parse("0999-12-31T23:59:59Z"))).isInstanceOf(IllegalArgumentException.class);
    }
}
