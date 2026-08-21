package io.mohs.jdbc.dialect;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.jdbc.JdbcTimestamps;

/**
 * MySQL 8.0+ — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo desde o
 * 8.0 (ADR-0023). Usa o template ANSI por decisão <em>medida</em>, não
 * por coincidência: a candidata {@code UNION ALL} por estado (um braço
 * por estado claimável, merge externo) foi implementada e medida na
 * rodada 2026-08-15 do {@code BASELINE.md} e <b>rejeitada</b> — o
 * otimizador dirige o join por {@code mohs_job_definitions} e ordena
 * dentro de cada braço (dois scan+sort em vez de um; throughput 1429.8 →
 * 970.0 rows/s). A próxima alavanca registrada lá é hint de índice/join
 * order, não forma de query.
 */
public final class MySqlJdbcDialect implements JdbcDialect {

    @Override
    public List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", JdbcTimestamps.toUtcTimestamp(now))
                .addValue("batchSize", batchSize);
        return jdbcTemplate.query(ANSI_SKIP_LOCKED_CANDIDATES, params, Candidate::fromResultSet);
    }
}
