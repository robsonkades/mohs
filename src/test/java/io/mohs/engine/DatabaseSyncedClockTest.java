package io.mohs.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class DatabaseSyncedClockTest {

    private static final Duration SYNC_INTERVAL = Duration.ofMinutes(1);
    private static final Duration SKEW_WARN_THRESHOLD = Duration.ofSeconds(1);

    private final ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(DatabaseSyncedClock.class);
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

    private static DataSource dataSourceReturning(Instant databaseNow) throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getTimestamp(1)).thenReturn(Timestamp.from(databaseNow));
        return dataSource;
    }

    @Test
    void appliesTheSampledOffsetWhenDatabaseAndAppClockAgree() throws SQLException {
        Instant now = Instant.parse("2026-08-13T12:00:00Z");
        MutableClock appClock = new MutableClock(now, ZoneId.of("UTC"));
        DataSource dataSource = dataSourceReturning(now);
        DatabaseSyncedClock clock = new DatabaseSyncedClock(
                dataSource, SYNC_INTERVAL, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();

        Duration tolerance = Duration.ofMillis(50);
        assertThat(clock.currentOffset()).isBetween(tolerance.negated(), tolerance);
        assertThat(clock.instant()).isBetween(now.minus(tolerance), now.plus(tolerance));
    }

    @Test
    void appliesAPositiveOffsetWhenTheDatabaseIsAhead() throws SQLException {
        Instant appNow = Instant.parse("2026-08-13T12:00:00Z");
        Instant databaseNow = appNow.plus(Duration.ofSeconds(5));
        MutableClock appClock = new MutableClock(appNow, ZoneId.of("UTC"));
        DataSource dataSource = dataSourceReturning(databaseNow);
        DatabaseSyncedClock clock = new DatabaseSyncedClock(
                dataSource, SYNC_INTERVAL, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();

        Duration tolerance = Duration.ofMillis(50);
        Duration expected = Duration.ofSeconds(5);
        assertThat(clock.currentOffset()).isBetween(expected.minus(tolerance), expected.plus(tolerance));
    }

    @Test
    void instantNeverGoesBackwardsAcrossAResampleThatWouldMoveItBackward() throws SQLException {
        Instant start = Instant.parse("2026-08-13T12:00:00Z");
        MutableClock appClock = new MutableClock(start, ZoneId.of("UTC"));
        DataSource dataSource = dataSourceReturning(start);
        DatabaseSyncedClock clock = new DatabaseSyncedClock(
                dataSource, SYNC_INTERVAL, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();
        Instant first = clock.instant();

        // App clock corre uma hora à frente; o banco (mock) continua parado no
        // instante original — a próxima amostra tentaria aplicar um offset
        // negativo grande o bastante pra voltar no tempo.
        appClock.advance(Duration.ofHours(1));
        clock.sampleOnce();
        Instant second = clock.instant();

        assertThat(second).isAfterOrEqualTo(first);
    }

    @Test
    void keepsThePreviousOffsetWhenTheDatabaseIsUnreachable() throws SQLException {
        Instant appNow = Instant.parse("2026-08-13T12:00:00Z");
        Instant databaseNow = appNow.plus(Duration.ofSeconds(3));
        MutableClock appClock = new MutableClock(appNow, ZoneId.of("UTC"));
        DataSource healthy = dataSourceReturning(databaseNow);
        DatabaseSyncedClock clock = new DatabaseSyncedClock(
                healthy, SYNC_INTERVAL, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);
        clock.sampleOnce();
        Duration offsetAfterSuccess = clock.currentOffset();

        DataSource broken = Mockito.mock(DataSource.class);
        Mockito.when(broken.getConnection()).thenThrow(new SQLException("connection refused"));
        DatabaseSyncedClock brokenClock = new DatabaseSyncedClock(
                broken, SYNC_INTERVAL, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        assertThatCode(brokenClock::sampleOnce).doesNotThrowAnyException();
        assertThat(brokenClock.currentOffset()).isEqualTo(Duration.ZERO);
        Duration tolerance = Duration.ofMillis(50);
        Duration expected = Duration.ofSeconds(3);
        assertThat(offsetAfterSuccess).isBetween(expected.minus(tolerance), expected.plus(tolerance));
    }

    @Test
    void warnsWhenSkewExceedsTheThreshold() throws SQLException {
        Instant appNow = Instant.parse("2026-08-13T12:00:00Z");
        Instant databaseNow = appNow.plus(Duration.ofMinutes(1));
        MutableClock appClock = new MutableClock(appNow, ZoneId.of("UTC"));
        DataSource dataSource = dataSourceReturning(databaseNow);
        DatabaseSyncedClock clock = new DatabaseSyncedClock(
                dataSource, SYNC_INTERVAL, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();

        assertThat(logAppender.list)
                .anyMatch(event -> event.getFormattedMessage().contains("clock skew"));
    }

    @Test
    void sampleOnceRunsSynchronouslyWithoutWaitingForTheSyncInterval() throws SQLException {
        Instant now = Instant.parse("2026-08-13T12:00:00Z");
        MutableClock appClock = new MutableClock(now, ZoneId.of("UTC"));
        DataSource dataSource = dataSourceReturning(now);
        DatabaseSyncedClock clock = new DatabaseSyncedClock(
                dataSource, Duration.ofDays(1), SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        assertThatCode(clock::sampleOnce).doesNotThrowAnyException();
    }
}
