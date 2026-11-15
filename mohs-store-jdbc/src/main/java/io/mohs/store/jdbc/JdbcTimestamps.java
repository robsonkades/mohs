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
 * Operational instants cross JDBC 4.2 as UTC LocalDateTime values at microsecond precision.
 * No JVM or connection time zone participates in persistence. OffsetDateTime is used only
 * to read database clock expressions that return an explicit offset.
 */
public final class JdbcTimestamps {

    private JdbcTimestamps() {
    }

    /**
     * Public: also used by {@code io.mohs.store.jdbc.delegate} (each delegate's {@code splitTimestamp}) — both are internal packages, so this never becomes the module's public API.
     *
     * @param instant the instant to evaluate
     * @return the instant expressed as a UTC local timestamp
     */
    public static LocalDateTime toUtcLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(io.mohs.core.TimePrecision.observed(instant), ZoneOffset.UTC);
    }

    /**
     * Interprets a JDBC local timestamp in UTC.
     *
     * @param dateTime the local date and time to interpret in UTC
     * @return the instant represented by the UTC timestamp
     */
    public static Instant fromUtcLocalDateTime(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC);
    }

    /** The binding of a nullable column ({@code next_fire_at} of a disarmed trigger): {@code null} crosses as SQL {@code NULL}. */
    static @Nullable LocalDateTime toUtcLocalDateTimeOrNull(@Nullable Instant instant) {
        return instant == null ? null : toUtcLocalDateTime(instant);
    }

    /**
     * The read of a nullable column: an SQL {@code NULL} comes back as {@code null}, never as an epoch. Public for the delegates' {@code readSplitTimestamp}.
     *
     * @param dateTime the date and time to interpret in UTC
     * @return the UTC instant, or {@code null} for a null timestamp
     */
    public static @Nullable Instant fromUtcLocalDateTimeOrNull(@Nullable LocalDateTime dateTime) {
        return dateTime == null ? null : fromUtcLocalDateTime(dateTime);
    }

    /**
     * Converts a JDBC offset timestamp to an instant.
     *
     * @param dateTime the date and time with its explicit offset
     * @return the instant represented by the offset timestamp
     */
    public static Instant fromUtcOffsetDateTime(OffsetDateTime dateTime) {
        return dateTime.toInstant();
    }
}
