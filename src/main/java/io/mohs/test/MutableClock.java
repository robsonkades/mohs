package io.mohs.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link Clock} de teste — a implementação "test" das três da ADR-0008.
 * {@link #setTo(Instant)}/{@link #advance(Duration)} tornam agenda e
 * misfire determinísticos sem {@code Thread.sleep} (§CLAUDE.md "O que NÃO
 * fazer"); é o relógio por trás de {@code mohs.clock()} no test kit
 * (§5.14 do documento mestre).
 */
public final class MutableClock extends Clock {

    private final ZoneId zone;
    private final AtomicReference<Instant> now;

    public MutableClock(Instant initial, ZoneId zone) {
        this.now = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
        this.zone = Objects.requireNonNull(zone, "zone");
    }

    public static MutableClock startingAt(Instant initial) {
        return new MutableClock(initial, ZoneId.of("UTC"));
    }

    public void setTo(Instant instant) {
        now.set(Objects.requireNonNull(instant, "instant"));
    }

    /** JAVA-9: {@code updateAndGet} torna o avanço atômico — dois {@code advance} concorrentes não perdem incremento (JCIP §2.2, mesma correção do CONC-3 no {@code DatabaseClock}). */
    public void advance(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        now.updateAndGet(current -> current.plus(duration));
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        Objects.requireNonNull(zone, "zone");
        return new MutableClock(now.get(), zone);
    }

    @Override
    public Instant instant() {
        return now.get();
    }
}
