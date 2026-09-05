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
import java.util.TimeZone;
import java.util.UUID;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The daylight-saving gap regression, the bug reported on 2026-08-19: the legacy path
 * ({@code Timestamp.valueOf}) resolved a nonexistent {@code LocalDateTime} through the JVM's DEFAULT
 * zone by pushing it an hour forward — during the gap hour, every instant written came out wrong.
 *
 * <p>The {@link LocalDateTime} crossing (JDBC 4.2) consults no zone at all; these tests pin that by
 * switching the JVM's default to a zone with DST and crossing an instant INSIDE the gap.
 */
class JdbcTimestampsTest {

    /**
     * What crosses the legacy conversion is the UTC WALL CLOCK — 02:30 — and it is THAT which has to be a
     * nonexistent time in the default zone (America/New_York's 2026-03-08 gap is 02:00-03:00 local): with
     * the old code, {@code Timestamp.valueOf(LDT 02:30)} was pushed to 03:30 and the round trip returned
     * 03:30Z, not 02:30Z.
     *
     * <p>The first version of this test used 07:30Z (the INSTANT whose local time is 02:30) — vacuous: it
     * passed with the bug present.
     */
    private static final Instant INSIDE_THE_GAP = Instant.parse("2026-03-08T02:30:00Z");

    private static void withJvmZone(String zoneId, Runnable body) {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
            body.run();
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void roundTripsAnInstantInsideTheDstGapRegardlessOfTheJvmZone() {
        withJvmZone("America/New_York", () -> assertThat(
                JdbcTimestamps.fromUtcLocalDateTime(JdbcTimestamps.toUtcLocalDateTime(INSIDE_THE_GAP)))
                .isEqualTo(INSIDE_THE_GAP));
    }

    /** The same gap through a real JDBC round trip — it is the driver that must not consult the default zone, not only the in-memory conversion. */
    @Test
    void survivesTheGapThroughARealJdbcRoundTrip() {
        withJvmZone("America/New_York", () -> {
            JdbcDataSource h2 = new JdbcDataSource();
            h2.setURL("jdbc:h2:mem:jdbc-timestamps-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            h2.setUser("sa");
            h2.setPassword("");
            JdbcTemplate jdbc = new JdbcTemplate(h2);
            jdbc.execute("CREATE TABLE gap_probe (ts TIMESTAMP NOT NULL)");

            jdbc.update("INSERT INTO gap_probe (ts) VALUES (?)", JdbcTimestamps.toUtcLocalDateTime(INSIDE_THE_GAP));
            LocalDateTime read = jdbc.queryForObject("SELECT ts FROM gap_probe", LocalDateTime.class);

            assertThat(read).isNotNull();
            assertThat(JdbcTimestamps.fromUtcLocalDateTime(read)).isEqualTo(INSIDE_THE_GAP);
        });
    }
}
