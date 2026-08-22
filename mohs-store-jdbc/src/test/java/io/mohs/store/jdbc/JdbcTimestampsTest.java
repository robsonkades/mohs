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
 * ADR-0049 — a regressão do gap de horário de verão, o bug reportado em
 * 2026-08-19: o caminho legado ({@code Timestamp.valueOf}) resolvia um
 * {@code LocalDateTime} inexistente pelo fuso DEFAULT da JVM empurrando-o
 * 1h pra frente — durante a hora do gap, todo instante gravado saía
 * errado. A travessia por {@link LocalDateTime} (JDBC 4.2) não consulta
 * fuso nenhum; estes testes fixam isso trocando o default da JVM para uma
 * zona com DST e atravessando um instante DENTRO do gap.
 */
class JdbcTimestampsTest {

    /**
     * O que atravessa a conversão legada é o WALL-CLOCK UTC — 02:30 — e é
     * ELE que precisa ser hora inexistente na zona default (o gap de
     * America/New_York em 2026-03-08 é 02:00–03:00 local): com o código
     * antigo, {@code Timestamp.valueOf(LDT 02:30)} era empurrado pra 03:30
     * e o round trip devolvia 03:30Z ≠ 02:30Z. A primeira versão deste
     * teste usava 07:30Z (o INSTANTE cujo horário local é 02:30) — vácuo:
     * passava com o bug presente (review da Phase 2).
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

    /** O mesmo gap por um round trip JDBC de verdade — é o driver que não pode consultar o fuso default, não só a conversão em memória. */
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
