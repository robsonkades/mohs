package io.mohs.store.jdbc.dialect;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/** H2 (embarcado/teste) — {@code LIMIT} no fim, {@code SKIP LOCKED} nativo (ADR-0023). */
public final class H2JdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/h2";
    }

    @Override
    public List<Candidate> selectCandidates(NamedParameterJdbcTemplate jdbcTemplate, Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", JdbcTimestamps.toUtcLocalDateTime(now))
                .addValue("batchSize", batchSize);
        return jdbcTemplate.query(ANSI_SKIP_LOCKED_CANDIDATES, params, Candidate::fromResultSet);
    }
}
