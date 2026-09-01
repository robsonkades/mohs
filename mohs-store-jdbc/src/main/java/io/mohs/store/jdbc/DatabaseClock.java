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
 * <p>It is the one place in the engine where reading the real clock ({@link Clock#systemUTC()}) is the
 * class's purpose rather than a violation of "every now comes from the injected Clock" —
 * {@code ArchitectureTest} makes an exception for this class alone.
 */
public final class DatabaseClock extends Clock implements SyncableClock {

    private static final Logger log = LoggerFactory.getLogger(DatabaseClock.class);

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
    private final AtomicReference<Sample> sample = new AtomicReference<>(Sample.UNSYNCED);

    /**
     * The offset and whether it was ever measured, in one reference so the clamp below stays a single
     * atomic unit.
     *
     * <p>The flag is what keeps the clamp from anchoring on a value nobody sampled. A monotonic clamp
     * is an invariant over the SEQUENCE of samples; the starting value is not a sample, and treating
     * it as one silently inverted the feature for half the cluster — a node whose local clock runs
     * AHEAD of the database has a negative offset, every sample of it compares below zero, and every
     * one was discarded. That node kept its own fast clock forever, which is the node that reaps a
     * live peer's lease.
     */
    private record Sample(Duration offset, boolean measured) {

        static final Sample UNSYNCED = new Sample(Duration.ZERO, false);

        Sample keepingTheLaterOf(Sample sampled) {
            return !measured || sampled.offset().compareTo(offset) >= 0 ? sampled : this;
        }
    }

    public DatabaseClock(DataSource dataSource, Duration skewWarnThreshold, JdbcDelegate delegate) {
        this(dataSource, skewWarnThreshold, delegate, ZoneId.of("UTC"), Clock.systemUTC());
    }

    DatabaseClock(DataSource dataSource, Duration skewWarnThreshold, JdbcDelegate delegate, ZoneId zone, Clock systemClock) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.delegate = Objects.requireNonNull(delegate, "delegate");
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
        return systemClock.instant().plus(sample.get().offset());
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
     */
    public boolean isSynchronised() {
        return sample.get().measured();
    }

    /** Measures the database-to-application offset with round-trip compensation and applies the monotonic clamp. */
    @Override
    public void sync() {
        try {
            Instant beforeQuery = systemClock.instant();
            long startNanos = System.nanoTime();
            String nowQuery = delegate.nowQuery();
            Instant databaseNow = jdbcTemplate.queryForObject(nowQuery, (rs, rowNum) -> delegate.readNow(rs));
            Duration roundTrip = Duration.ofNanos(System.nanoTime() - startNanos);

            if (databaseNow == null) {
                log.warn("'{}' returned no result, keeping last known offset {}", nowQuery, currentOffset());
                return;
            }

            Instant appNowAtMidpoint = beforeQuery.plus(roundTrip.dividedBy(2));
            Duration sampledOffset = Duration.between(appNowAtMidpoint, databaseNow);
            warnOnExcessiveSkew(sampledOffset);
            applyIfMonotonic(sampledOffset);
        } catch (DataAccessException e) {
            log.warn("failed to sync clock with database, keeping last known offset {}", currentOffset(), e);
        }
    }

    private void warnOnExcessiveSkew(Duration sampledOffset) {
        if (sampledOffset.abs().compareTo(skewWarnThreshold) > 0) {
            log.warn("clock skew {} exceeds threshold {}", sampledOffset, skewWarnThreshold);
        }
    }

    /**
     * "now + sampledOffset < now + current offset" simplifies to "sampledOffset < current offset" — the
     * {@code now} is the same on both sides, so there is no need to read the clock again to compare. A
     * resample that would go backwards in time is discarded — not adjusted to some minimum safe value —
     * and is retried on the next call.
     *
     * <p>The FIRST measurement is always applied, whatever its sign: there is no earlier sample for it
     * to move backwards from. The clamp only ever compares two things the database actually said.
     *
     * <p>A discard is logged, because it is invisible otherwise. The clamp is the right answer to a
     * transient measurement error and the wrong one to a persistent disagreement — the samples keep
     * arriving, keep being dropped, and the offset silently stops converging. The line says which node
     * is refusing to move and by how much.
     *
     * <p>{@code getAndAccumulate} makes the clamp atomic regardless of who calls {@link #sync()} —
     * nothing assumes a single writer; if two concurrent samplings arrive here, the comparison and the
     * write happen as one unit, with no window for an older write to overwrite a newer one already
     * published.
     */
    private void applyIfMonotonic(Duration sampledOffset) {
        Sample previous = sample.getAndAccumulate(new Sample(sampledOffset, true), Sample::keepingTheLaterOf);
        if (previous.measured() && sampledOffset.compareTo(previous.offset()) < 0) {
            log.warn("clock sample {} discarded: it would move this node's time backwards from offset {}",
                    sampledOffset, previous.offset());
        }
    }
}
