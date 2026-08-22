package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.core.EngineState;
import io.mohs.engine.NodeStore;
import io.mohs.engine.StoredNode;

/**
 * {@link NodeStore} sobre {@code mohs_nodes} (Data Mapper, PoEAA). Sem
 * {@link JdbcSupport#namedTemplateWithStreamFetchSize} — {@link #findAll}
 * devolve {@code List}, não {@code Stream} (tabela limitada pelo tamanho
 * do cluster), então não precisa do fetch size de cursor.
 */
public final class JdbcNodeStore implements NodeStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcNodeStore(DataSource dataSource) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
    }

    private static final String HEARTBEAT_UPDATE = """
            UPDATE mohs_nodes SET state = :state, last_heartbeat_at = :lastHeartbeatAt,
                epoch = :epoch, expires_at = :expiresAt
            WHERE node_id = :nodeId
            """;

    @Override
    public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(at, "at");
        Objects.requireNonNull(expiresAt, "expiresAt");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("nodeId", nodeId)
                .addValue("state", state.name())
                .addValue("epoch", epoch)
                .addValue("lastHeartbeatAt", JdbcTimestamps.toUtcLocalDateTime(at))
                .addValue("expiresAt", JdbcTimestamps.toUtcLocalDateTime(expiresAt));

        // ver CONC-2 em JdbcJobStore.upsert — mesma corrida, mesma correção.
        int updated = jdbcTemplate.update(HEARTBEAT_UPDATE, params);
        if (updated == 0) {
            try {
                jdbcTemplate.update("INSERT INTO mohs_nodes (node_id, state, last_heartbeat_at, epoch, expires_at)"
                        + " VALUES (:nodeId, :state, :lastHeartbeatAt, :epoch, :expiresAt)", params);
            } catch (DuplicateKeyException _) {
                jdbcTemplate.update(HEARTBEAT_UPDATE, params);
            }
        }
    }

    @Override
    public List<StoredNode> findAll() {
        return jdbcTemplate.query("SELECT * FROM mohs_nodes", (rs, _) -> mapRow(rs));
    }

    @Override
    public int deleteHeartbeatsBefore(Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        return jdbcTemplate.update("DELETE FROM mohs_nodes WHERE last_heartbeat_at < :cutoff",
                new MapSqlParameterSource().addValue("cutoff", JdbcTimestamps.toUtcLocalDateTime(cutoff)));
    }

    private static StoredNode mapRow(ResultSet rs) throws SQLException {
        LocalDateTime expiresAt = rs.getObject("expires_at", LocalDateTime.class);
        return new StoredNode(rs.getString("node_id"), EngineState.valueOf(rs.getString("state")),
                JdbcTimestamps.fromUtcLocalDateTime(rs.getObject("last_heartbeat_at", LocalDateTime.class)),
                rs.getLong("epoch"),
                expiresAt == null ? null : JdbcTimestamps.fromUtcLocalDateTime(expiresAt));
    }
}
