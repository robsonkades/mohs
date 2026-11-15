package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.resource.RateLimit;
import static org.assertj.core.api.Assertions.assertThat;

class JdbcRatePrecisionTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void fractionsSurviveChargesWithoutIssuingEarly(JdbcJobBindingTest.Backend backend) {
        var start = Instant.parse("2026-09-09T12:00:00Z");
        var clock = io.mohs.test.MutableClock.startingAt(start);
        var store = new JdbcRateLimitStore(backend.dataSource(), TestDatabaseClock.at(backend.delegate(), clock));
        store.upsert(new RateLimit("fractional", 3, Duration.ofNanos(10_000)));
        assertThat(store.charge("fractional", 3)).isTrue();
        clock.setTo(start.plusNanos(3_000));
        assertThat(store.available("fractional")).isZero();
        clock.setTo(start.plusNanos(4_000));
        assertThat(store.charge("fractional", 1)).isTrue();
        clock.setTo(start.plusNanos(7_000));
        assertThat(store.charge("fractional", 1)).isTrue();
        clock.setTo(start.plusNanos(10_000));
        assertThat(store.charge("fractional", 1)).isTrue();
        clock.setTo(start.plusNanos(13_000));
        assertThat(store.available("fractional")).isZero();
        clock.setTo(start.plusNanos(14_000));
        assertThat(store.available("fractional")).isOne();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void nonDivisibleRateDoesNotRoundItsIntervalDown(JdbcJobBindingTest.Backend backend) {
        var start = Instant.parse("2026-09-09T12:00:00Z");
        var clock = io.mohs.test.MutableClock.startingAt(start);
        var store = new JdbcRateLimitStore(backend.dataSource(), TestDatabaseClock.at(backend.delegate(), clock));
        store.upsert(new RateLimit("high-rate", 333_333, Duration.ofSeconds(1)));
        assertThat(store.charge("high-rate", 333_333)).isTrue();
        clock.setTo(start.plusNanos(999_999_000));
        assertThat(store.available("high-rate")).isEqualTo(333_332);
        clock.setTo(start.plusSeconds(1));
        assertThat(store.available("high-rate")).isEqualTo(333_333);
    }
}
