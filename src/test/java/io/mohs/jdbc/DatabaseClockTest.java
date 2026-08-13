package io.mohs.jdbc;

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
        h2.setURL("jdbc:h2:mem:clock-sync-test;DB_CLOSE_DELAY=-1");
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
     * TEST-2 do code review: a versão anterior deste teste afirmava sobre
     * {@code clock.instant()} (= {@code appClock.instant() + offset}) — como
     * o {@code appClock.advance(1h)} já embute o salto de +1h na leitura do
     * app, um clamp quebrado que aplicasse cegamente o offset negativo
     * incorreto (~-1h) cancelaria o salto e produziria um {@code second}
     * quase igual a {@code first} de qualquer forma — a asserção passava
     * até no contrafactual quebrado. Afirmar sobre {@link DatabaseClock#currentOffset()}
     * direto (antes/depois do segundo {@code sync()}) é o que realmente
     * distingue "clamp aplicado" de "clamp ausente".
     */
    @Test
    void offsetNeverDecreasesAcrossAResampleThatWouldMoveItBackward() {
        MutableClock appClock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
        DatabaseClock clock = new DatabaseClock(dataSource, SKEW_WARN_THRESHOLD, ZoneId.of("UTC"), appClock);

        clock.sync();
        Duration offsetAfterFirstSync = clock.currentOffset();

        // App clock corre uma hora à frente; o banco continua no tempo real —
        // a próxima amostra tentaria aplicar um offset negativo grande o
        // bastante pra voltar no tempo. O clamp deve descartar essa amostra
        // e manter o offset anterior intocado.
        appClock.advance(Duration.ofHours(1));
        clock.sync();

        assertThat(clock.currentOffset()).isEqualTo(offsetAfterFirstSync);
    }

    /**
     * TEST-3 do code review: a versão anterior construía um
     * {@link DatabaseClock} NOVO diretamente contra um {@link DataSource}
     * quebrado — {@code currentOffset() == Duration.ZERO} só provava que o
     * valor default do campo sobrevive a uma falha, nunca que um offset
     * não-zero **já aprendido** sobrevive a uma resincronização que falha
     * depois (a propriedade que de fato importa). Aqui a mesma instância
     * sincroniza com sucesso uma vez (aprendendo um offset não-zero) e só
     * então passa a falhar.
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
