package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.engine.BatchCounters;
import io.mohs.engine.BatchStore;

/** {@link BatchStore} sobre {@code mohs_batches} (Data Mapper, PoEAA). */
public final class JdbcBatchStore implements BatchStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;

    public JdbcBatchStore(DataSource dataSource, Clock clock) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void create(String batchId, int total) {
        Objects.requireNonNull(batchId, "batchId");
        if (total < 0) {
            throw new IllegalArgumentException("total must not be negative");
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", batchId)
                .addValue("total", total)
                .addValue("createdAt", JdbcTimestamps.toUtcTimestamp(clock.instant()));
        jdbcTemplate.update("""
                INSERT INTO mohs_batches (id, total, succeeded, failed, created_at)
                VALUES (:id, :total, 0, 0, :createdAt)
                """, params);
    }

    @Override
    public Optional<BatchCounters> find(String batchId) {
        Objects.requireNonNull(batchId, "batchId");
        return JdbcSupport.findOne(jdbcTemplate,
                "SELECT * FROM mohs_batches WHERE id = :id",
                new MapSqlParameterSource("id", batchId),
                JdbcBatchStore::mapRow);
    }

    @Override
    public void incrementSucceeded(String batchId) {
        jdbcTemplate.update("UPDATE mohs_batches SET succeeded = succeeded + 1 WHERE id = :id",
                new MapSqlParameterSource("id", Objects.requireNonNull(batchId, "batchId")));
    }

    @Override
    public void incrementFailed(String batchId) {
        jdbcTemplate.update("UPDATE mohs_batches SET failed = failed + 1 WHERE id = :id",
                new MapSqlParameterSource("id", Objects.requireNonNull(batchId, "batchId")));
    }

    private static BatchCounters mapRow(ResultSet rs) throws SQLException {
        return new BatchCounters(rs.getString("id"), rs.getInt("total"), rs.getInt("succeeded"), rs.getInt("failed"));
    }
}
