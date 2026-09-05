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
package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.engine.SyncableClock;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * The "database" time source: the database is the cluster's time authority — {@link #instant()} never
 * does I/O, being O(1) over the offset already sampled by {@link #sync()}.
 *
 * <p>It implements {@link SyncableClock} (a port in {@code io.mohs.engine}) rather than exposing
 * {@code sync()}/{@code currentOffset()} only as concrete methods — the same adapter pattern as
 * {@code JdbcJobStore}/{@code JobStore}.
 *
 * <p>Only the offset is this class's responsibility — "how often to resample" is a decision for its
 * user (the scheduling belongs to {@code io.mohs.autoconfigure}, alongside the rest of
 * {@code mohs.time.*}'s property binding).
 *
 * <p>It reads the host clock to estimate the offset against the database. Returned instants are
 * clamped independently of the offset: correcting an ahead sample pauses time until the database
 * catches up with the last observation, rather than preserving that erroneous offset forever.
 */
public final class DatabaseClock extends Clock implements SyncableClock {

    private static final Logger log = LoggerFactory.getLogger(DatabaseClock.class);

    /** At most 500 ms of midpoint uncertainty; a stalled connection is not a clock measurement. */
    private static final Duration MAX_SAMPLE_ROUND_TRIP = Duration.ofSeconds(1);

    private final JdbcTemplate jdbcTemplate;
    /**
     * Both halves of a sample — the statement and its crossing back to an instant — belong to the
     * dialect, and the crossing is never a {@code java.sql.Timestamp}: the driver builds one by
     * interpreting a zoneless server answer in the JVM's zone, so a node three hours from the server
     * read a three-hour "clock offset" that was only the distance between two zones.
     */
    private final JdbcDelegate delegate;
    private final Duration skewWarnThreshold;
    private final ZoneId zone;
    private final Clock systemClock;
    private final LongSupplier nanoTime;
    private final AtomicReference<Sample> sample = new AtomicReference<>(Sample.UNSYNCED);

    /** The accepted offset and last returned instant move atomically, including across a resample. */
    private record Sample(Duration offset, boolean measured, Instant lastReturned) {

        static final Sample UNSYNCED = new Sample(Duration.ZERO, false, Instant.MIN);

        Sample at(Instant localNow) {
            Instant candidate = localNow.plus(offset);
            return candidate.isAfter(lastReturned) ? new Sample(offset, measured, candidate) : this;
        }
    }

    /**
     * Creates a {@code DatabaseClock} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param skewWarnThreshold the offset magnitude above which a warning is logged
     * @param delegate the database-specific time query and timestamp adapter
     */
    public DatabaseClock(DataSource dataSource, Duration skewWarnThreshold, JdbcDelegate delegate) {
        this(dataSource, skewWarnThreshold, delegate, ZoneId.of("UTC"), Clock.systemUTC());
    }

    DatabaseClock(DataSource dataSource, Duration skewWarnThreshold, JdbcDelegate delegate, ZoneId zone, Clock systemClock) {
        this(dataSource, skewWarnThreshold, delegate, zone, systemClock, System::nanoTime);
    }

    DatabaseClock(DataSource dataSource, Duration skewWarnThreshold, JdbcDelegate delegate, ZoneId zone,
            Clock systemClock, LongSupplier nanoTime) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.jdbcTemplate.setQueryTimeout((int) MAX_SAMPLE_ROUND_TRIP.toSeconds());
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.skewWarnThreshold = Objects.requireNonNull(skewWarnThreshold, "skewWarnThreshold");
        this.zone = Objects.requireNonNull(zone, "zone");
        this.systemClock = Objects.requireNonNull(systemClock, "systemClock");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    /** A view in another zone, delegating {@link #instant()} to this same clock. */
    @Override
    public Clock withZone(ZoneId zone) {
        Objects.requireNonNull(zone, "zone");
        return new Clock() {
            @Override
            public ZoneId getZone() {
                return zone;
            }

            @Override
            public Clock withZone(ZoneId newZone) {
                return DatabaseClock.this.withZone(newZone);
            }

            @Override
            public Instant instant() {
                return DatabaseClock.this.instant();
            }
        };
    }

    @Override
    public Instant instant() {
        Instant localNow = systemClock.instant();
        return sample.updateAndGet(current -> current.at(localNow)).lastReturned();
    }

    @Override
    public Duration currentOffset() {
        return sample.get().offset();
    }

    /**
     * Whether a sample was ever applied. Refusing to run on an unsynchronised clock is the caller's
     * decision, not this class's: a failed resample must not stop an engine that is already running,
     * while a failed FIRST sample means the engine would start on the very clock the operator said not
     * to trust.
     *
     * @return whether at least one database time sample has been accepted
     */
    public boolean isSynchronised() {
        return sample.get().measured();
    }

    /** Serialises measurements, not reads: a slow older query cannot overwrite a newer sample. */
    @Override
    public synchronized void sync() {
        try {
            Instant beforeQuery = systemClock.instant();
            long startNanos = nanoTime.getAsLong();
            String nowQuery = delegate.nowQuery();
            Instant databaseNow = jdbcTemplate.queryForObject(nowQuery, (rs, _) -> delegate.readNow(rs));
            Duration roundTrip = Duration.ofNanos(nanoTime.getAsLong() - startNanos);

            if (roundTrip.compareTo(MAX_SAMPLE_ROUND_TRIP) > 0) {
                log.warn("clock sample discarded: round trip {} exceeds {}; keeping last known offset {}",
                        roundTrip, MAX_SAMPLE_ROUND_TRIP, currentOffset());
                return;
            }

            if (databaseNow == null) {
                log.warn("'{}' returned no result, keeping last known offset {}", nowQuery, currentOffset());
                return;
            }

            Instant appNowAtMidpoint = beforeQuery.plus(roundTrip.dividedBy(2));
            Duration sampledOffset = Duration.between(appNowAtMidpoint, databaseNow);
            warnOnExcessiveSkew(sampledOffset);
            // An offset may decrease. Clamping OFFSETS would retain a fast sample forever; clamp
            // only instants already returned, letting database time catch up with that finite floor.
            sample.updateAndGet(current -> new Sample(sampledOffset, true, current.lastReturned()));
        } catch (DataAccessException e) {
            log.warn("failed to sync clock with database, keeping last known offset {}", currentOffset(), e);
        }
    }

    private void warnOnExcessiveSkew(Duration sampledOffset) {
        if (sampledOffset.abs().compareTo(skewWarnThreshold) > 0) {
            log.warn("clock skew {} exceeds threshold {}", sampledOffset, skewWarnThreshold);
        }
    }

}
