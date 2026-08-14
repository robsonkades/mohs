package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.core.resource.RateLimit;
import io.mohs.engine.RateLimitStore;

/** {@link RateLimitStore} sobre {@code mohs_rate_limits} (Data Mapper, PoEAA). */
public final class JdbcRateLimitStore implements RateLimitStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcRateLimitStore(DataSource dataSource) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
    }

    @Override
    public RateLimit upsert(RateLimit rateLimit) {
        Objects.requireNonNull(rateLimit, "rateLimit");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", rateLimit.name())
                .addValue("maxCount", rateLimit.max())
                .addValue("windowDuration", rateLimit.window().toString());

        // ver CONC-2 em JdbcJobStore.upsert — mesma corrida, mesma correção.
        int updated = jdbcTemplate.update("UPDATE mohs_rate_limits SET max_count = :maxCount, window_duration = :windowDuration WHERE name = :name", params);
        if (updated == 0) {
            try {
                jdbcTemplate.update("INSERT INTO mohs_rate_limits (name, max_count, window_duration) VALUES (:name, :maxCount, :windowDuration)", params);
            } catch (DuplicateKeyException e) {
                jdbcTemplate.update("UPDATE mohs_rate_limits SET max_count = :maxCount, window_duration = :windowDuration WHERE name = :name", params);
            }
        }
        return rateLimit;
    }

    @Override
    public Optional<RateLimit> find(String name) {
        Objects.requireNonNull(name, "name");
        RateLimit result = jdbcTemplate.query(
                "SELECT * FROM mohs_rate_limits WHERE name = :name",
                new MapSqlParameterSource("name", name),
                rs -> rs.next() ? mapRow(rs) : null);
        return Optional.ofNullable(result);
    }

    @Override
    public Stream<RateLimit> findAll() {
        return jdbcTemplate.queryForStream("SELECT * FROM mohs_rate_limits", new MapSqlParameterSource(), (rs, rowNum) -> mapRow(rs));
    }

    private static RateLimit mapRow(ResultSet rs) throws SQLException {
        return new RateLimit(rs.getString("name"), rs.getInt("max_count"), Duration.parse(rs.getString("window_duration")));
    }
}
