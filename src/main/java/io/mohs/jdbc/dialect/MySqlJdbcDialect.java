package io.mohs.jdbc.dialect;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.jdbc.JdbcTimestamps;

/** MySQL 8.0+ — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo desde o 8.0 (ADR-0023). */
public final class MySqlJdbcDialect implements JdbcDialect {

    @Override
    public List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", JdbcTimestamps.toUtcTimestamp(now))
                .addValue("batchSize", batchSize);
        // e.priority já é Priority.value() (menor reivindica primeiro) —
        // NOT NULL DEFAULT 20 no schema, então ordena direto, sem CASE.
        return jdbcTemplate.query("""
                SELECT e.id AS id, e.job_key AS job_key,
                       j.allow_concurrent_executions AS allow_concurrent_executions
                FROM mohs_executions e
                JOIN mohs_job_definitions j ON j.job_key = e.job_key
                WHERE e.state = 'ENQUEUED'
                  AND e.scheduled_at <= :now
                  AND (j.allow_concurrent_executions = TRUE OR j.running_execution_count < j.max_concurrent_executions)
                ORDER BY e.priority ASC, e.scheduled_at ASC
                LIMIT :batchSize
                FOR UPDATE OF e SKIP LOCKED
                """, params, Candidate::fromResultSet);
    }
}
