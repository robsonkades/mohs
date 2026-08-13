package io.mohs.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

/**
 * {@link Clock} de teste — a implementação "test" das três da ADR-0008.
 * {@link #setTo(Instant)}/{@link #advance(Duration)} tornam agenda e
 * misfire determinísticos sem {@code Thread.sleep} (§CLAUDE.md "O que NÃO
 * fazer"); é o relógio por trás de {@code mohs.clock()} no test kit
 * (§5.14 do documento mestre).
 */
public final class MutableClock extends Clock {

    private final ZoneId zone;
    private volatile Instant now;

    public MutableClock(Instant initial, ZoneId zone) {
        this.now = Objects.requireNonNull(initial, "initial");
        this.zone = Objects.requireNonNull(zone, "zone");
    }

    public static MutableClock startingAt(Instant initial) {
        return new MutableClock(initial, ZoneId.of("UTC"));
    }

    public void setTo(Instant instant) {
        this.now = Objects.requireNonNull(instant, "instant");
    }

    public void advance(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        this.now = this.now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        Objects.requireNonNull(zone, "zone");
        return new MutableClock(now, zone);
    }

    @Override
    public Instant instant() {
        return now;
    }
}
