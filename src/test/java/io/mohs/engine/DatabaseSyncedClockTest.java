package io.mohs.engine;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

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
 * Contra um H2 real (embarcado, mesma JVM) em vez de mockar a cadeia
 * interna do {@link org.springframework.jdbc.core.JdbcTemplate} — mockar
 * até {@code ResultSet} seria frágil (depende de detalhe de implementação
 * do Spring, não de contrato estável). Os cenários controlados (offset
 * positivo, clamp, skew) são construídos manipulando o {@link MutableClock}
 * do lado do app, não fingindo a hora do banco.
 */
class DatabaseSyncedClockTest {

    private static final Duration SKEW_WARN_THRESHOLD = Duration.ofSeconds(1);
    private static final Duration TOLERANCE = Duration.ofSeconds(2);

    private final DataSource dataSource = h2DataSource();
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

    private static DataSource h2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:clock-sync-test;DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        return h2;
    }

    @Test
    void appliesTheSampledOffsetWhenDatabaseAndAppClockAgree() {
        MutableClock appClock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        DatabaseSyncedClock clock = new DatabaseSyncedClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();

        assertThat(clock.currentOffset().abs()).isLessThan(TOLERANCE);
    }

    @Test
    void appliesAPositiveOffsetWhenTheDatabaseIsAhead() {
        Duration expected = Duration.ofSeconds(5);
        MutableClock appClock = new MutableClock(Instant.now().minus(expected), ZoneId.of("UTC"));
        DatabaseSyncedClock clock = new DatabaseSyncedClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();

        assertThat(clock.currentOffset()).isBetween(expected.minus(TOLERANCE), expected.plus(TOLERANCE));
    }

    @Test
    void instantNeverGoesBackwardsAcrossAResampleThatWouldMoveItBackward() {
        MutableClock appClock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        DatabaseSyncedClock clock = new DatabaseSyncedClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();
        Instant first = clock.instant();

        // App clock corre uma hora à frente; o banco continua no tempo real —
        // a próxima amostra tentaria aplicar um offset negativo grande o
        // bastante pra voltar no tempo.
        appClock.advance(Duration.ofHours(1));
        clock.sampleOnce();
        Instant second = clock.instant();

        assertThat(second).isAfterOrEqualTo(first);
    }

    @Test
    void keepsThePreviousOffsetWhenTheDatabaseIsUnreachable() throws SQLException {
        Duration expected = Duration.ofSeconds(3);
        MutableClock appClock = new MutableClock(Instant.now().minus(expected), ZoneId.of("UTC"));
        DatabaseSyncedClock clock = new DatabaseSyncedClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);
        clock.sampleOnce();
        Duration offsetAfterSuccess = clock.currentOffset();

        DataSource broken = Mockito.mock(DataSource.class);
        Mockito.when(broken.getConnection()).thenThrow(new SQLException("connection refused"));
        DatabaseSyncedClock brokenClock = new DatabaseSyncedClock(broken, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        assertThatCode(brokenClock::sampleOnce).doesNotThrowAnyException();
        assertThat(brokenClock.currentOffset()).isEqualTo(Duration.ZERO);
        assertThat(offsetAfterSuccess).isBetween(expected.minus(TOLERANCE), expected.plus(TOLERANCE));
    }

    @Test
    void warnsWhenSkewExceedsTheThreshold() {
        MutableClock appClock = new MutableClock(Instant.now().minus(Duration.ofMinutes(1)), ZoneId.of("UTC"));
        DatabaseSyncedClock clock = new DatabaseSyncedClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sampleOnce();

        assertThat(logAppender.list)
                .anyMatch(event -> event.getFormattedMessage().contains("clock skew"));
    }
}
