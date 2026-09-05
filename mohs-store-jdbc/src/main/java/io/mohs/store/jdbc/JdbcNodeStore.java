/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * {@link NodeStore} over {@code mohs_nodes} (a Data Mapper, PoEAA). Without
 * {@link JdbcSupport#namedTemplateWithStreamFetchSize} — {@link #findAll} returns a {@code List}, not a
 * {@code Stream} (a table bounded by the cluster's size), so it does not need the cursor fetch size.
 */
public final class JdbcNodeStore implements NodeStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final JdbcDelegate delegate;

    public JdbcNodeStore(DataSource dataSource, JdbcDelegate delegate) {
        // One template, the tick's: the heartbeat is the statement that must never wait out the lease,
        // and the purge runs on the same thread. The peers read also serves the facade's node listing —
        // one row per node, so sharing the ceiling costs it nothing
        this.jdbcTemplate = JdbcSupport.tickTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

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

        // See the equivalent race in JdbcJobStore.upsert — same race, same fix.
        int updated = jdbcTemplate.update(delegate.heartbeatUpdate(), params);
        if (updated == 0) {
            try {
                jdbcTemplate.update(delegate.insertNode(), params);
            } catch (DuplicateKeyException _) {
                jdbcTemplate.update(delegate.heartbeatUpdate(), params);
            }
        }
    }

    @Override
    public List<StoredNode> findAll() {
        return jdbcTemplate.query(delegate.findAllNodes(), (rs, _) -> mapRow(rs));
    }

    @Override
    public int deleteHeartbeatsBefore(Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        return jdbcTemplate.update(delegate.deleteHeartbeatsBefore(),
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
