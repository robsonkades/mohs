package io.mohs.jdbc.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jspecify.annotations.Nullable;

/** Linha crua de {@link JdbcDialect#selectCandidates} — {@code io.mohs.jdbc.JdbcClaimer} decide o resto. */
public record Candidate(String id, String jobKey, boolean allowConcurrentExecutions, @Nullable String windowName,
        @Nullable String rateLimitName) {

    /**
     * Mapeamento compartilhado pelos 4 {@link JdbcDialect} — ao contrário do
     * texto SQL que os produz (deliberadamente duplicado por dialeto, ver
     * {@code package-info.java}), as colunas lidas aqui (`id`/`job_key`/
     * `allow_concurrent_executions`/`window_name`/`rate_limit`) já são um
     * contrato fixo entre os quatro {@code SELECT}s, então este mapeamento
     * não carrega nada específico de banco.
     */
    static Candidate fromResultSet(ResultSet rs, int rowNum) throws SQLException {
        return new Candidate(rs.getString("id"), rs.getString("job_key"), rs.getBoolean("allow_concurrent_executions"),
                rs.getString("window_name"), rs.getString("rate_limit"));
    }
}
