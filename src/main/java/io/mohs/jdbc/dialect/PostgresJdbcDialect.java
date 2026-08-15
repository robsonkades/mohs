package io.mohs.jdbc.dialect;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.jdbc.JdbcTimestamps;

/** PostgreSQL — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo (é de onde a sintaxe vem — ADR-0022/0023). */
public final class PostgresJdbcDialect implements JdbcDialect {

    @Override
    public List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", JdbcTimestamps.toUtcTimestamp(now))
                .addValue("batchSize", batchSize);
        return jdbcTemplate.query(ANSI_SKIP_LOCKED_CANDIDATES, params, Candidate::fromResultSet);
    }
}
