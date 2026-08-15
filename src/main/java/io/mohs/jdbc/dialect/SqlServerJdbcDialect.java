package io.mohs.jdbc.dialect;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.jdbc.JdbcTimestamps;

/**
 * SQL Server: sem {@code LIMIT} — usa {@code TOP (:batchSize)} logo após
 * {@code SELECT}, mudando a posição na query, não só o texto. Sem
 * {@code SKIP LOCKED} — usa o hint de tabela {@code WITH (UPDLOCK,
 * ROWLOCK, READPAST)}, confirmado via jOOQ (é o que jOOQ gera pra emular
 * {@code SKIP LOCKED} em SQL Server — ADR-0023). {@code BIT} compara com
 * {@code 1}, não {@code TRUE}.
 */
public final class SqlServerJdbcDialect implements JdbcDialect {

    @Override
    public List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", JdbcTimestamps.toUtcTimestamp(now))
                .addValue("batchSize", batchSize);
        // e.priority já é Priority.value() (menor reivindica primeiro) —
        // NOT NULL DEFAULT 20 no schema, então ordena direto, sem CASE.
        // j.retired = 0: job aposentado (Mohs.remove) nunca volta a ser candidato.
        return jdbcTemplate.query("""
                SELECT TOP (:batchSize) e.id AS id, e.job_key AS job_key,
                       j.allow_concurrent_executions AS allow_concurrent_executions,
                       j.window_name AS window_name
                FROM mohs_executions e WITH (UPDLOCK, ROWLOCK, READPAST)
                JOIN mohs_job_definitions j ON j.job_key = e.job_key
                WHERE e.state IN ('ENQUEUED', 'RETRY_SCHEDULED')
                  AND e.scheduled_at <= :now
                  AND j.retired = 0
                  AND (j.allow_concurrent_executions = 1 OR j.running_execution_count < j.max_concurrent_executions)
                ORDER BY e.priority ASC, e.scheduled_at ASC
                """, params, Candidate::fromResultSet);
    }

    @Override
    public String topClause() {
        return "TOP (:limit) ";
    }

    @Override
    public String limitClause() {
        return "";
    }
}
