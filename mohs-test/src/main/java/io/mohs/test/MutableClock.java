/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test {@link Clock}: the implementation a test injects wherever the engine reads "now".
 *
 * <p>Every point in time the engine observes comes from an injected clock, never from
 * {@code Instant.now()}, precisely so that a test can drive it. {@link #setTo(Instant)} and
 * {@link #advance(Duration)} make scheduling and misfire deterministic without
 * {@code Thread.sleep}, which is timing-dependent and therefore flaky. This is the clock behind
 * {@code mohs.clock()} in the test kit.
 *
 * <p>Thread-safe: the instant is held in an {@link AtomicReference}, so readers never observe a
 * torn value and concurrent advances compose.
 */
public final class MutableClock extends Clock {

    private final ZoneId zone;
    private final AtomicReference<Instant> now;

    public MutableClock(Instant initial, ZoneId zone) {
        this(new AtomicReference<>(Objects.requireNonNull(initial, "initial")), zone);
    }

    /** The time source is passed by reference so that {@link #withZone} yields a view, not a copy. */
    private MutableClock(AtomicReference<Instant> now, ZoneId zone) {
        this.now = now;
        this.zone = Objects.requireNonNull(zone, "zone");
    }

    public static MutableClock startingAt(Instant initial) {
        return new MutableClock(initial, ZoneId.of("UTC"));
    }

    public void setTo(Instant instant) {
        now.set(Objects.requireNonNull(instant, "instant"));
    }

    /**
     * Advances the clock by {@code duration}.
     *
     * <p>{@code updateAndGet} keeps the advance atomic, so two concurrent calls cannot lose an
     * increment (JCIP §2.2 — the same fix applied as CONC-3 in {@code DatabaseClock}).
     */
    public void advance(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        now.updateAndGet(current -> current.plus(duration));
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    /**
     * A view in another zone that SHARES the same time source: {@code advance} on the original
     * clock moves both.
     *
     * <p>This is what {@link Clock#withZone} promises ("a copy of this clock with a different
     * time-zone"). An earlier version returned a SNAPSHOT instead, so the derived clock was born
     * frozen and a library user's test passed — or failed — for the wrong reason, silently.
     * {@code DatabaseClock} has always behaved this way.
     */
    @Override
    public Clock withZone(ZoneId zone) {
        Objects.requireNonNull(zone, "zone");
        return new MutableClock(now, zone);
    }

    @Override
    public Instant instant() {
        return now.get();
    }
}
