package io.mohs.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Bind/leitura de instantes contra colunas temporais sem fuso ({@code
 * TIMESTAMP} do H2/Postgres, {@code DATETIME} do MySQL, {@code DATETIME2}
 * do SQL Server) — DBTUNE-1. {@code PreparedStatement.setTimestamp}/{@code
 * ResultSet.getTimestamp} sem {@code Calendar} convertem usando o fuso
 * <b>default da JVM</b>; dentro de uma única JVM o round-trip sempre fecha
 * (a mesma conversão se desfaz), mas entre nós com {@code user.timezone}
 * diferente o valor gravado diverge — nó em UTC grava {@code 12:00}, nó em
 * {@code America/Sao_Paulo} lê {@code 12:00} como hora local, 3h de erro em
 * {@code scheduled_at}/lease/ordenação do claim.
 *
 * <p>Contrato deste projeto: toda coluna temporal guarda o wall-clock em
 * UTC. {@link #toUtcTimestamp} e {@link #fromUtcTimestamp} são inversas —
 * cada uma aplica a conversão de fuso default da JVM uma vez, então as
 * duas convertem consistentemente para/de UTC independente de qual fuso é
 * o default (a conversão de fuso do {@code java.sql.Timestamp} legado
 * acontece nos dois lados e se cancela, sobrando só a diferença UTC que
 * pedimos explicitamente).
 */
public final class JdbcTimestamps {

    private JdbcTimestamps() {
    }

    /** Público: usado também por {@code io.mohs.jdbc.dialect} (os 4 binds de {@code now} da claim query) — ambos são pacotes internos, isto nunca vira API pública do módulo. */
    public static Timestamp toUtcTimestamp(Instant instant) {
        return Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
    }

    public static Instant fromUtcTimestamp(Timestamp timestamp) {
        return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC);
    }
}
