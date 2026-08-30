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

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.engine.SyncableClock;

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
 * <p>It is the one place in the engine where reading the real clock ({@link Clock#systemUTC()}) is the
 * class's purpose rather than a violation of "every now comes from the injected Clock" —
 * {@code ArchitectureTest} makes an exception for this class alone.
 */
public final class DatabaseClock extends Clock implements SyncableClock {

    private static final Logger log = LoggerFactory.getLogger(DatabaseClock.class);
    private static final String NOW_QUERY = "SELECT CURRENT_TIMESTAMP";

    private final JdbcTemplate jdbcTemplate;
    private final Duration skewWarnThreshold;
    private final ZoneId zone;
    private final Clock systemClock;
    private final AtomicReference<Duration> offset = new AtomicReference<>(Duration.ZERO);

    public DatabaseClock(DataSource dataSource, Duration skewWarnThreshold) {
        this(dataSource, skewWarnThreshold, ZoneId.of("UTC"), Clock.systemUTC());
    }

    DatabaseClock(DataSource dataSource, Duration skewWarnThreshold, ZoneId zone, Clock systemClock) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.skewWarnThreshold = Objects.requireNonNull(skewWarnThreshold, "skewWarnThreshold");
        this.zone = Objects.requireNonNull(zone, "zone");
        this.systemClock = Objects.requireNonNull(systemClock, "systemClock");
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
        return systemClock.instant().plus(offset.get());
    }

    @Override
    public Duration currentOffset() {
        return offset.get();
    }

    /** Measures the database-to-application offset with round-trip compensation and applies the monotonic clamp. */
    @Override
    public void sync() {
        try {
            Instant beforeQuery = systemClock.instant();
            long t0 = System.nanoTime();
            // A deliberate Timestamp (not the usual crossing): here the source is the database's clock,
            // not a zoneless column — tz-aware on PG and H2. On SQL Server and MySQL it would NOT be,
            // which is why the auto-configuration refuses mohs.time.mode=database on both rather than
            // sampling a difference between two zones and calling it a clock offset
            Timestamp databaseTimestamp = jdbcTemplate.queryForObject(NOW_QUERY, Timestamp.class);
            long t1 = System.nanoTime();

            if (databaseTimestamp == null) {
                log.warn("'{}' returned no result, keeping last known offset {}", NOW_QUERY, offset.get());
                return;
            }

            Duration roundTrip = Duration.ofNanos(t1 - t0);
            Instant appNowAtMidpoint = beforeQuery.plus(roundTrip.dividedBy(2));
            Duration sampledOffset = Duration.between(appNowAtMidpoint, databaseTimestamp.toInstant());

            if (sampledOffset.abs().compareTo(skewWarnThreshold) > 0) {
                log.warn("clock skew {} exceeds threshold {}", sampledOffset, skewWarnThreshold);
            }

            applyIfMonotonic(sampledOffset);
        } catch (DataAccessException e) {
            log.warn("failed to sync clock with database, keeping last known offset {}", offset.get(), e);
        }
    }

    /**
     * "now + sampledOffset < now + current offset" simplifies to "sampledOffset < current offset" — the
     * {@code now} is the same on both sides, so there is no need to read the clock again to compare. A
     * resample that would go backwards in time is discarded — not adjusted to some minimum safe value —
     * and is retried on the next call, by which point real time will have advanced enough.
     *
     * <p>{@code accumulateAndGet} makes the clamp atomic regardless of who calls {@link #sync()} —
     * nothing assumes a single writer; if two concurrent samplings arrive here, the comparison and the
     * write happen as one unit, with no window for an older write to overwrite a newer one already
     * published.
     */
    private void applyIfMonotonic(Duration sampledOffset) {
        offset.accumulateAndGet(sampledOffset, (current, sampled) -> sampled.compareTo(current) < 0 ? current : sampled);
    }
}
