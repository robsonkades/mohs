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

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Against a real H2 (embedded, same JVM) rather than mocking
 * {@link org.springframework.jdbc.core.JdbcTemplate}'s internal chain — mocking down to the
 * {@code ResultSet} would be fragile (it would depend on a Spring implementation detail, not on a stable
 * contract). The controlled scenarios (a positive offset, the clamp, skew) are built by manipulating the
 * application-side {@link MutableClock}, not by faking the database's time.
 */
class DatabaseClockTest {

    private static final Duration SKEW_WARN_THRESHOLD = Duration.ofSeconds(1);
    private static final Duration TOLERANCE = Duration.ofSeconds(2);

    private final DataSource dataSource = h2DataSource();
    private final ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(DatabaseClock.class);
    private final ListAppender<ILoggingEvent> logAppender = new ListAppender<>();

    @BeforeEach
    void attachLogAppender() {
        logAppender.start();
        logger.addAppender(logAppender);
    }

    @AfterEach
    void detachLogAppender() {
        logger.detachAppender(logAppender);
    }

    private static DataSource h2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:clock-sync-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        return h2;
    }

    @Test
    void appliesTheSampledOffsetWhenDatabaseAndAppClockAgree() {
        MutableClock appClock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        DatabaseClock clock = new DatabaseClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sync();

        assertThat(clock.currentOffset().abs()).isLessThan(TOLERANCE);
    }

    @Test
    void appliesAPositiveOffsetWhenTheDatabaseIsAhead() {
        Duration expected = Duration.ofSeconds(5);
        MutableClock appClock = new MutableClock(Instant.now().minus(expected), ZoneId.of("UTC"));
        DatabaseClock clock = new DatabaseClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sync();

        assertThat(clock.currentOffset()).isBetween(expected.minus(TOLERANCE), expected.plus(TOLERANCE));
    }

    /**
     * The previous version of this test asserted on {@code clock.instant()} (= {@code appClock.instant()}
     * plus the offset) — and since {@code appClock.advance(1h)} already embeds the +1h jump in the
     * application's read, a broken clamp that blindly applied the incorrect negative offset (~-1h) would
     * cancel the jump and produce a {@code second} nearly equal to {@code first} anyway — the assertion
     * passed even in the broken counterfactual.
     *
     * <p>Asserting directly on {@link DatabaseClock#currentOffset()} (before and after the second
     * {@code sync()}) is what actually distinguishes "clamp applied" from "clamp missing".
     */
    @Test
    void offsetNeverDecreasesAcrossAResampleThatWouldMoveItBackward() {
        MutableClock appClock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        DatabaseClock clock = new DatabaseClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sync();
        Duration offsetAfterFirstSync = clock.currentOffset();

        // The app clock runs an hour ahead while the database stays in real time — the next sample would try
        // to apply a negative offset large enough to go back in time. The clamp must discard that sample and
        // leave the previous offset untouched.
        appClock.advance(Duration.ofHours(1));
        clock.sync();

        assertThat(clock.currentOffset()).isEqualTo(offsetAfterFirstSync);
    }

    /**
     * The previous version built a NEW {@link DatabaseClock} directly against a broken {@link DataSource} —
     * {@code currentOffset() == Duration.ZERO} only proved that the field's default value survives a
     * failure, never that a non-zero offset **already learned** survives a resync that fails afterwards
     * (the property that actually matters). Here the same instance syncs successfully once (learning a
     * non-zero offset) and only then starts failing.
     */
    @Test
    void keepsThePreviouslyLearnedOffsetWhenAResyncFails() throws SQLException {
        Duration expected = Duration.ofSeconds(3);
        MutableClock appClock = new MutableClock(Instant.now().minus(expected), ZoneId.of("UTC"));
        DataSource flaky = Mockito.mock(DataSource.class);
        Mockito.when(flaky.getConnection())
                .thenReturn(dataSource.getConnection())
                .thenThrow(new SQLException("connection refused"));
        DatabaseClock clock = new DatabaseClock(flaky, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sync();
        Duration offsetAfterSuccess = clock.currentOffset();
        assertThat(offsetAfterSuccess).isBetween(expected.minus(TOLERANCE), expected.plus(TOLERANCE));

        assertThatCode(clock::sync).doesNotThrowAnyException();

        assertThat(clock.currentOffset()).isEqualTo(offsetAfterSuccess);
    }

    @Test
    void withZoneKeepsTheSameInstantAndDelegatesFutureSyncs() {
        MutableClock appClock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        DatabaseClock clock = new DatabaseClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);
        clock.sync();

        var saoPaulo = clock.withZone(ZoneId.of("America/Sao_Paulo"));

        assertThat(saoPaulo.getZone()).isEqualTo(ZoneId.of("America/Sao_Paulo"));
        assertThat(saoPaulo.instant()).isEqualTo(clock.instant());
    }

    @Test
    void warnsWhenSkewExceedsTheThreshold() {
        MutableClock appClock = new MutableClock(Instant.now().minus(Duration.ofMinutes(1)), ZoneId.of("UTC"));
        DatabaseClock clock = new DatabaseClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sync();

        assertThat(logAppender.list)
                .anyMatch(event -> event.getFormattedMessage().contains("clock skew"));
    }
}
