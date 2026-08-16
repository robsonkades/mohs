package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
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

    @Override
    public void heartbeat(String nodeId, EngineState state, Instant at) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(at, "at");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("nodeId", nodeId)
                .addValue("state", state.name())
                .addValue("lastHeartbeatAt", JdbcTimestamps.toUtcTimestamp(at));

        // ver CONC-2 em JdbcJobStore.upsert — mesma corrida, mesma correção.
        int updated = jdbcTemplate.update("UPDATE mohs_nodes SET state = :state, last_heartbeat_at = :lastHeartbeatAt WHERE node_id = :nodeId", params);
        if (updated == 0) {
            try {
                jdbcTemplate.update("INSERT INTO mohs_nodes (node_id, state, last_heartbeat_at) VALUES (:nodeId, :state, :lastHeartbeatAt)", params);
            } catch (DuplicateKeyException _) {
                jdbcTemplate.update("UPDATE mohs_nodes SET state = :state, last_heartbeat_at = :lastHeartbeatAt WHERE node_id = :nodeId", params);
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
                new MapSqlParameterSource().addValue("cutoff", JdbcTimestamps.toUtcTimestamp(cutoff)));
    }

    private static StoredNode mapRow(ResultSet rs) throws SQLException {
        return new StoredNode(rs.getString("node_id"), EngineState.valueOf(rs.getString("state")), JdbcTimestamps.fromUtcTimestamp(rs.getTimestamp("last_heartbeat_at")));
    }
}
