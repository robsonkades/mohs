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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.jspecify.annotations.Nullable;

/**
 * Binding and reading instants against zoneless temporal columns (H2's and Postgres's
 * {@code TIMESTAMP}, MySQL's {@code DATETIME}, SQL Server's {@code DATETIME2}): the crossing is
 * {@link LocalDateTime} through JDBC 4.2 ({@code setObject}/{@code getObject}), NEVER
 * {@code java.sql.Timestamp}.
 *
 * <p>The legacy path converted through the <b>JVM's default zone</b> at both ends; the constant offset
 * cancelled out within one JVM, but the daylight-saving gap did not — {@code Timestamp.valueOf} resolves
 * a nonexistent {@code LocalDateTime} by pushing it forward, and during the gap hour every instant
 * written came out an hour wrong (reported 2026-08-19; in {@code refilled_at} that means an apparently
 * empty token bucket — a burst above the limit, the failure mode the rate limit exists to prevent).
 * {@link LocalDateTime} consults no zone at all: the UTC wall clock crosses verbatim in all four
 * databases, and the two functions are inverses at EVERY instant, gap included.
 *
 * <p>The project's contract, unchanged: every temporal column stores the wall clock in UTC. Why not
 * {@code timestamptz}/{@code datetimeoffset} yet: MySQL makes uniformity impossible (its
 * {@code TIMESTAMP} ends in 2038 — unacceptable for a scheduler's {@code next_fire_at}) and converting
 * the type on the current tables buys nothing this crossing does not already provide; the newer tables
 * are born tz-aware.
 */
public final class JdbcTimestamps {

    private JdbcTimestamps() {
    }

    /** Public: also used by {@code io.mohs.store.jdbc.delegate} (each delegate's {@code splitTimestamp}) — both are internal packages, so this never becomes the module's public API. */
    public static LocalDateTime toUtcLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    public static Instant fromUtcLocalDateTime(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC);
    }

    /** The binding of a nullable column ({@code next_fire_at} of a disarmed trigger): {@code null} crosses as SQL {@code NULL}. */
    static @Nullable LocalDateTime toUtcLocalDateTimeOrNull(@Nullable Instant instant) {
        return instant == null ? null : toUtcLocalDateTime(instant);
    }

    /** The read of a nullable column: an SQL {@code NULL} comes back as {@code null}, never as an epoch. Public for the delegates' {@code readSplitTimestamp}. */
    public static @Nullable Instant fromUtcLocalDateTimeOrNull(@Nullable LocalDateTime dateTime) {
        return dateTime == null ? null : fromUtcLocalDateTime(dateTime);
    }

    /**
     * The crossing for the TZ-AWARE tables (Postgres's {@code TIMESTAMPTZ}): {@link OffsetDateTime} in
     * UTC through JDBC 4.2.
     *
     * <p>A {@link LocalDateTime} would be interpreted in the SESSION's zone against a tz-aware column —
     * exactly the class of bug the zoneless crossing killed; an explicit offset crosses verbatim
     * regardless of the session's {@code TimeZone}. The databases with no tz-aware column (H2, MySQL, SQL
     * Server, the split's functional equivalents) stay on the LocalDateTime crossing — the choice belongs
     * to {@code JdbcDelegate}.
     */
    public static OffsetDateTime toUtcOffsetDateTime(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    public static Instant fromUtcOffsetDateTime(OffsetDateTime dateTime) {
        return dateTime.toInstant();
    }
}
