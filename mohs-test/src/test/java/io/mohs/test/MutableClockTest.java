package io.mohs.test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MutableClockTest {

    @Test
    void startsAtTheGivenInstant() {
        Instant start = Instant.parse("2026-08-13T00:00:00Z");

        MutableClock clock = MutableClock.startingAt(start);

        assertThat(clock.instant()).isEqualTo(start);
    }

    @Test
    void setToMovesTheClock() {
        MutableClock clock = MutableClock.startingAt(Instant.parse("2026-08-13T00:00:00Z"));
        Instant target = Instant.parse("2030-01-01T00:00:00Z");

        clock.setTo(target);

        assertThat(clock.instant()).isEqualTo(target);
    }

    @Test
    void advanceMovesTheClockForward() {
        Instant start = Instant.parse("2026-08-13T00:00:00Z");
        MutableClock clock = MutableClock.startingAt(start);

        clock.advance(Duration.ofMinutes(30));

        assertThat(clock.instant()).isEqualTo(start.plus(Duration.ofMinutes(30)));
    }

    @Test
    void withZoneKeepsTheSameInstantUnderADifferentZone() {
        Instant start = Instant.parse("2026-08-13T00:00:00Z");
        MutableClock clock = new MutableClock(start, ZoneId.of("UTC"));

        var saoPaulo = clock.withZone(ZoneId.of("America/Sao_Paulo"));

        assertThat(saoPaulo.getZone()).isEqualTo(ZoneId.of("America/Sao_Paulo"));
        assertThat(saoPaulo.instant()).isEqualTo(start);
    }
}
