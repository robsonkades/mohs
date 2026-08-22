package io.mohs.store.jdbc.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/** PostgreSQL — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo (é de onde a sintaxe vem — ADR-0022/0023). */
public final class PostgresJdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/postgresql";
    }

    /**
     * Constante (e não inline) pelo mesmo motivo de {@code
     * ANSI_SKIP_LOCKED_CANDIDATES}: {@code ClaimQueryExplainHarness} deriva
     * a versão com literais daqui — cópia manual já sofreu drift duas
     * vezes (nota de metodologia da rodada 08-15 do BASELINE.md).
     */
    public static final String BATCH_TRANSITION_TO_RUNNING = """
            UPDATE mohs_executions
            SET state = 'RUNNING', lease_expires_at = :leaseExpiresAt, node_id = :nodeId, fired_at = :now
            WHERE id IN (:ids) AND state IN ('ENQUEUED', 'RETRY_SCHEDULED') AND scheduled_at <= :now
            RETURNING id
            """;

    @Override
    public List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", JdbcTimestamps.toUtcLocalDateTime(now))
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
                .addValue("leaseExpiresAt", JdbcTimestamps.toUtcLocalDateTime(leaseExpiresAt))
                .addValue("nodeId", nodeId)
                .addValue("now", JdbcTimestamps.toUtcLocalDateTime(now))
                .addValue("ids", ids);
        return jdbcTemplate.query(BATCH_TRANSITION_TO_RUNNING, params, (rs, rowNum) -> rs.getString("id"));
    }

    // ─── Phase 5 (ADR-A) ─────────────────────────────────────────────────

    /** Tabelas do split são {@code TIMESTAMPTZ} aqui (§7.2): {@code OffsetDateTime} UTC — {@code LocalDateTime} seria lido pelo fuso da SESSÃO (ver Javadoc da interface). */
    @Override
    public Object splitTimestamp(Instant instant) {
        return JdbcTimestamps.toUtcOffsetDateTime(instant);
    }

    @Override
    public @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : JdbcTimestamps.fromUtcOffsetDateTime(value);
    }

    /**
     * O claim do §5.4 na forma que só o Postgres tem: UM statement — a CTE
     * seleciona com {@code SKIP LOCKED}, o {@code DELETE … USING} consome
     * a fila, o {@code INSERT} (também CTE) grava a posse e o SELECT final
     * devolve NA ORDEM {@code (priority, visible_at)} — a ordem de um
     * {@code RETURNING} de INSERT não é garantida, e a forma portátil dos
     * outros dialetos devolve ordenado: o contrato da porta é um só nos
     * quatro (review S5.2). O INSERT lê de {@code picked} — idêntico a ler
     * de {@code gone}, que deleta exatamente {@code picked}. Duas
     * constantes (com/sem filtro de inadmissíveis) porque {@code NOT IN}
     * de lista vazia não expande.
     */
    public static final String CLAIM_READY = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt, priority, visible_at
                  FROM mohs_ready
                 WHERE shard = :shard AND visible_at <= :now
                 ORDER BY priority, visible_at
                 LIMIT :limit
                   FOR UPDATE SKIP LOCKED
            ),
            gone AS (
                DELETE FROM mohs_ready r USING picked p
                 WHERE r.execution_id = p.execution_id
            ),
            leased AS (
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, claimed_at)
                SELECT execution_id, job_key, :nodeId, :epoch, attempt, :now FROM picked
            )
            SELECT execution_id, job_key, attempt FROM picked
            ORDER BY priority, visible_at
            """;

    public static final String CLAIM_READY_FILTERED = CLAIM_READY.replace(
            "WHERE shard = :shard AND visible_at <= :now",
            "WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)");

    static {
        // guarda do replace: se a âncora do WHERE mudar e o replace no-opar,
        // o filtro de inadmissíveis sumiria em silêncio (review S5.2)
        if (!CLAIM_READY_FILTERED.contains(":inadmissible")) {
            throw new ExceptionInInitializerError("CLAIM_READY_FILTERED lost its :inadmissible predicate — the replace anchor drifted");
        }
    }

    @Override
    public List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, long epoch,
            int limit, Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit)
                .addValue("nodeId", nodeId)
                .addValue("epoch", epoch);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(CLAIM_READY, params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(CLAIM_READY_FILTERED,
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
    }
}
