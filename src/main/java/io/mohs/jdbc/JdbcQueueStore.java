package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.core.resource.JobQueue;
import io.mohs.engine.QueueStore;
import io.mohs.engine.StoredQueue;

/** {@link QueueStore} sobre {@code mohs_job_queues} (Data Mapper, PoEAA). */
public final class JdbcQueueStore implements QueueStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcQueueStore(DataSource dataSource) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
    }

    @Override
    public JobQueue upsert(JobQueue queue) {
        Objects.requireNonNull(queue, "queue");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", queue.name())
                .addValue("maxConcurrent", queue.maxConcurrent());

        // tenta UPDATE primeiro; 0 linhas afetadas = nome novo, faz INSERT
        // (mesma disciplina de JdbcJobStore.upsert — sem SELECT prévio).
        int updated = jdbcTemplate.update("UPDATE mohs_job_queues SET max_concurrent = :maxConcurrent WHERE name = :name", params);
        if (updated == 0) {
            jdbcTemplate.update("INSERT INTO mohs_job_queues (name, max_concurrent, running_count) VALUES (:name, :maxConcurrent, 0)", params);
        }
        return queue;
    }

    @Override
    public Optional<StoredQueue> find(String name) {
        Objects.requireNonNull(name, "name");
        StoredQueue result = jdbcTemplate.query(
                "SELECT * FROM mohs_job_queues WHERE name = :name",
                new MapSqlParameterSource("name", name),
                rs -> rs.next() ? mapRow(rs) : null);
        return Optional.ofNullable(result);
    }

    @Override
    public Stream<StoredQueue> findAll() {
        return jdbcTemplate.queryForStream("SELECT * FROM mohs_job_queues", new MapSqlParameterSource(), (rs, rowNum) -> mapRow(rs));
    }

    private static StoredQueue mapRow(ResultSet rs) throws SQLException {
        JobQueue queue = JobQueue.named(rs.getString("name"))
                .maxConcurrent(rs.getInt("max_concurrent"))
                .build();
        return new StoredQueue(queue, rs.getInt("running_count"));
    }
}
