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
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.store.jdbc.delegate.JdbcDelegate;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one property {@link DatabaseClockTest} cannot show, because it runs in whatever zone the build
 * happens to be in: that the sampled offset is the distance between two CLOCKS and not between two
 * ZONES.
 *
 * <p>Every case runs with the JVM three hours from UTC against a server on UTC, and with the
 * application clock started a known {@link #DATABASE_AHEAD_BY} behind — so the expected offset is a
 * value only a real sample produces. It is worth stating what the counterfactual looks like, because
 * it is what these assertions were written against: reading the answer as a {@code java.sql.Timestamp}
 * — the driver interpreting a zoneless value in the JVM's zone — measured +10,799,407 ms on MySQL and
 * +10,800,059 ms on SQL Server. Three hours, to the millisecond, of a "clock offset" that was nothing
 * but this test's own {@code setDefault}.
 *
 * <p>PostgreSQL and H2 cannot fail that particular counterfactual — they answer WITH a zone, so even a
 * {@code java.sql.Timestamp} lands on the right instant. They are here for a different one, and it is
 * the likelier future edit: a delegate that "simplifies" all four onto a single crossing. Read as a
 * zoneless {@code LocalDateTime}, these two are the pair that breaks, and they break here.
 */
@ResourceLock(Resources.TIME_ZONE)
class DatabaseClockZoneTest {

    private static final TimeZone THREE_HOURS_FROM_UTC = TimeZone.getTimeZone("America/Sao_Paulo");
    private static final Duration SKEW_WARN_THRESHOLD = Duration.ofMinutes(5);
    /**
     * The application clock starts this far BEHIND the server, so the expected offset is a value only a
     * real sample can produce. Asserting "close to zero" instead would have been hollow twice over: a
     * {@code sync()} that failed outright also leaves zero, and the sign of the bug depends on which
     * side of UTC the test's zone is — from Sao_Paulo (west) a zone distance reads +3h and is caught,
     * but from a zone east of UTC it reads -3h, which the monotonic clamp would discard, leaving zero
     * and a green test over a live defect.
     */
    private static final Duration DATABASE_AHEAD_BY = Duration.ofSeconds(30);
    /** Generous next to a container's round trip, and far from both zero and the three hours it catches. */
    private static final Duration TOLERANCE = Duration.ofSeconds(5);

    @Test
    void postgresSamplesTheServerClockRatherThanTheZoneDistance() {
        assertOffsetIsNotTheZoneDistance(PostgresTestSupport.freshSchema(), new PostgresJdbcDelegate());
    }

    @Test
    void mysqlSamplesTheServerClockRatherThanTheZoneDistance() {
        assertOffsetIsNotTheZoneDistance(MySqlTestSupport.freshSchema(), new MySqlJdbcDelegate());
    }

    @Test
    void sqlServerSamplesTheServerClockRatherThanTheZoneDistance() {
        assertOffsetIsNotTheZoneDistance(SqlServerTestSupport.freshSchema(), new SqlServerJdbcDelegate());
    }

    @Test
    void h2SamplesTheServerClockRatherThanTheZoneDistance() {
        assertOffsetIsNotTheZoneDistance(h2DataSource(), new H2JdbcDelegate());
    }

    private static DataSource h2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:clock-zone-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        return h2;
    }

    private static void assertOffsetIsNotTheZoneDistance(DataSource dataSource, JdbcDelegate delegate) {
        TimeZone original = TimeZone.getDefault();
        TimeZone.setDefault(THREE_HOURS_FROM_UTC);
        try {
            Clock appClock = new MutableClock(Instant.now().minus(DATABASE_AHEAD_BY), ZoneOffset.UTC);
            DatabaseClock clock = new DatabaseClock(dataSource, SKEW_WARN_THRESHOLD, delegate, ZoneOffset.UTC, appClock);

            clock.sync();

            assertThat(clock.currentOffset())
                    .isBetween(DATABASE_AHEAD_BY.minus(TOLERANCE), DATABASE_AHEAD_BY.plus(TOLERANCE));
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
