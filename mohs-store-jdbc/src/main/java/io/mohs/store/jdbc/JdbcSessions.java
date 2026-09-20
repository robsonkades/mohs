/*
 * Copyright 2026 The Mohs Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.EngineState;
import io.mohs.engine.StoredNode;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Session arbitration inside the caller's short transaction; no handler runs while this lock is held. */
final class JdbcSessions {
    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcDelegate dialect;

    JdbcSessions(NamedParameterJdbcTemplate jdbc, JdbcDelegate dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    Optional<StoredNode> lock(String nodeId) {
        return jdbc.query(dialect.lockNode(), new MapSqlParameterSource("nodeId", nodeId),
                (rs, _) -> read(rs)).stream().findFirst();
    }

    Instant now() {
        return JdbcDatabaseTime.now(jdbc, dialect);
    }

    boolean active(StoredNode node) {
        return node.validAt(now());
    }

    static StoredNode read(ResultSet rs) throws SQLException {
        return new StoredNode(rs.getString("node_id"), EngineState.valueOf(rs.getString("state")),
                JdbcTimestamps.fromUtcLocalDateTime(rs.getObject("last_heartbeat_at", LocalDateTime.class)),
                JdbcTimestamps.fromUtcLocalDateTime(rs.getObject("expires_at", LocalDateTime.class)),
                rs.getBoolean("revoked"));
    }
}
