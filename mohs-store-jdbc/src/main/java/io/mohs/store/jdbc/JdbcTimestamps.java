package io.mohs.store.jdbc;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Bind/leitura de instantes contra colunas temporais sem fuso ({@code
 * TIMESTAMP} do H2/Postgres, {@code DATETIME} do MySQL, {@code DATETIME2}
 * do SQL Server) — DBTUNE-1, revisto na ADR-0049 (Phase 2): a travessia é
 * {@link LocalDateTime} via JDBC 4.2 ({@code setObject}/{@code getObject}),
 * NUNCA {@code java.sql.Timestamp}. O caminho legado convertia pelo fuso
 * <b>default da JVM</b> nas duas pontas; o offset constante se cancelava
 * dentro de uma JVM, mas o gap de horário de verão não — {@code
 * Timestamp.valueOf} resolve um {@code LocalDateTime} inexistente
 * empurrando-o pra frente, e durante a hora do gap todo instante gravado
 * saía 1h errado (bug reportado 2026-08-19; em {@code refilled_at} isso é
 * um balde aparentemente vazio de tokens — burst acima do limite, o modo
 * de falha que a ADR-0042 existe pra impedir). {@link LocalDateTime} não
 * consulta fuso nenhum: o wall-clock UTC atravessa verbatim nos 4
 * dialetos, e as duas funções são inversas em TODO instante, gap incluso.
 *
 * <p>Contrato do projeto, inalterado: toda coluna temporal guarda o
 * wall-clock em UTC. Por que não {@code timestamptz}/{@code
 * datetimeoffset} já: MySQL torna a uniformidade impossível ({@code
 * TIMESTAMP} termina em 2038 — inaceitável para {@code next_fire_at} de um
 * scheduler) e a conversão de tipo nas tabelas atuais não paga nada que
 * esta travessia já não pague; as tabelas novas da Phase 5 (§7.2 do
 * redesign) nascem tz-aware.
 */
public final class JdbcTimestamps {

    private JdbcTimestamps() {
    }

    /** Público: usado também por {@code io.mohs.store.jdbc.dialect} (os 4 binds de {@code now} da claim query) — ambos são pacotes internos, isto nunca vira API pública do módulo. */
    public static LocalDateTime toUtcLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    public static Instant fromUtcLocalDateTime(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC);
    }

    /**
     * Travessia das tabelas TZ-AWARE da Phase 5 ({@code TIMESTAMPTZ} do
     * Postgres — §7.2 do redesign, o que a ADR-0049 adiou pra cá):
     * {@link OffsetDateTime} em UTC via JDBC 4.2. {@link LocalDateTime}
     * seria interpretado pelo fuso da SESSÃO numa coluna tz-aware —
     * exatamente a classe de bug que a ADR-0049 matou; o offset explícito
     * atravessa verbatim independente de {@code TimeZone} de sessão. Os
     * dialetos sem coluna tz-aware (H2/MySQL/SQL Server, equivalentes
     * funcionais do split) continuam na travessia LocalDateTime — a
     * escolha é do {@code JdbcDialect}.
     */
    public static OffsetDateTime toUtcOffsetDateTime(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    public static Instant fromUtcOffsetDateTime(OffsetDateTime dateTime) {
        return dateTime.toInstant();
    }
}
