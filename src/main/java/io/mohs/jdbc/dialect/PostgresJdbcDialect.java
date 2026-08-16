package io.mohs.jdbc.dialect;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.jdbc.JdbcTimestamps;

/** PostgreSQL — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo (é de onde a sintaxe vem — ADR-0022/0023). */
public final class PostgresJdbcDialect implements JdbcDialect {

    /**
     * Constante (e não inline) pelo mesmo motivo de {@code
     * ANSI_SKIP_LOCKED_CANDIDATES}: {@code ClaimQueryExplainHarness} deriva
     * a versão com literais daqui — cópia manual já sofreu drift duas
     * vezes (nota de metodologia da rodada 08-15 do BASELINE.md).
     */
    public static final String BATCH_TRANSITION_TO_RUNNING = """
            UPDATE mohs_executions
            SET state = 'RUNNING', lease_expires_at = :leaseExpiresAt, node_id = :nodeId
            WHERE id IN (:ids) AND state IN ('ENQUEUED', 'RETRY_SCHEDULED') AND scheduled_at <= :now
            RETURNING id
            """;

    @Override
    public List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", JdbcTimestamps.toUtcTimestamp(now))
                .addValue("batchSize", batchSize);
        return jdbcTemplate.query(ANSI_SKIP_LOCKED_CANDIDATES, params, Candidate::fromResultSet);
    }

    /**
     * DBTUNE-16: os N CAS individuais do lote viram um único
     * {@code UPDATE ... WHERE id IN (...) ... RETURNING id} — as mesmas
     * guardas do template ANSI, aplicadas por linha (uma linha só
     * transiciona se as guardas valerem <b>nela</b>, exatamente como no
     * CAS individual); o {@code RETURNING} devolve quem venceu, que é a
     * informação que obrigava o default a ir linha a linha. Medido em
     * {@code docs/performance/BASELINE.md} (rodada 2026-08-16).
     */
    @Override
    public List<String> transitionToRunning(NamedParameterJdbcTemplate jdbcTemplate, List<String> ids, String nodeId,
            Instant now, Instant leaseExpiresAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("leaseExpiresAt", JdbcTimestamps.toUtcTimestamp(leaseExpiresAt))
                .addValue("nodeId", nodeId)
                .addValue("now", JdbcTimestamps.toUtcTimestamp(now))
                .addValue("ids", ids);
        return jdbcTemplate.query(BATCH_TRANSITION_TO_RUNNING, params, (rs, rowNum) -> rs.getString("id"));
    }
}
