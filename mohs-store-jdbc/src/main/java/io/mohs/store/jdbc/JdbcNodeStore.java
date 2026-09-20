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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import io.mohs.core.EngineState;
import io.mohs.engine.NodeStore;
import io.mohs.engine.StoredNode;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Serializes renewal and irrevocable revocation against the current database time. */
public final class JdbcNodeStore implements NodeStore {
    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcDelegate dialect;
    private final JdbcSessions sessions;
    private final TransactionTemplate transaction;

    public JdbcNodeStore(DataSource dataSource, JdbcDelegate dialect) {
        this.jdbc = JdbcSupport.tickTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.sessions = new JdbcSessions(jdbc, dialect);
        this.transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setTimeout(JdbcSupport.TICK_STATEMENT_TIMEOUT_SECONDS);
    }

    @Override
    public StoredNode register(String nodeId, EngineState state, Duration ttl, io.mohs.engine.ExecutableCatalog catalog) {
        io.mohs.core.GeneratedId.require(nodeId, "nodeId");
        requireTtl(ttl);
        return Objects.requireNonNull(transaction.execute(_ -> {
            new JdbcCatalog(jdbc, dialect).join(Objects.requireNonNull(catalog, "catalog"));
            var existing = sessions.lock(nodeId);
            if (existing.isPresent()) {
                if (!sessions.active(existing.orElseThrow())) {
                    throw new NodeStore.SessionExpired();
                }
                return existing.orElseThrow();
            }
            Instant now = sessions.now();
            var node = new StoredNode(nodeId, state, now, now.plus(ttl), false);
            jdbc.update(dialect.insertNode(), parameters(node));
            return node;
        }));
    }

    @Override
    public Optional<StoredNode> renew(String nodeId, EngineState state, Duration ttl) {
        io.mohs.core.GeneratedId.require(nodeId, "nodeId");
        requireTtl(ttl);
        return Objects.requireNonNull(transaction.execute(_ -> {
            var current = sessions.lock(nodeId);
            Instant now = sessions.now();
            if (current.isEmpty() || !current.orElseThrow().validAt(now)) {
                return Optional.empty();
            }
            var renewed = new StoredNode(nodeId, state, now, now.plus(ttl), false);
            jdbc.update(dialect.heartbeatUpdate(), parameters(renewed));
            return Optional.of(renewed);
        }));
    }

    @Override
    public boolean revokeExpired(String nodeId) {
        return Boolean.TRUE.equals(transaction.execute(_ -> {
            var current = sessions.lock(nodeId);
            if (current.isEmpty()) {
                return false;
            }
            var node = current.orElseThrow();
            if (node.revoked()) {
                return true;
            }
            if (node.expiresAt().isAfter(sessions.now())) {
                return false;
            }
            jdbc.update("UPDATE mohs_nodes SET revoked = :revoked WHERE node_id = :nodeId",
                    new MapSqlParameterSource("nodeId", nodeId).addValue("revoked", true));
            return true;
        }));
    }

    @Override
    public void stop(String nodeId) {
        transaction.executeWithoutResult(_ -> {
            if (sessions.lock(nodeId).isEmpty()) {
                return;
            }
            Instant now = sessions.now();
            jdbc.update("UPDATE mohs_nodes SET revoked = :revoked, state = :state, expires_at = :at WHERE node_id = :nodeId",
                    new MapSqlParameterSource("nodeId", nodeId).addValue("revoked", true)
                            .addValue("state", EngineState.STOPPED.name()).addValue("at", JdbcTimestamps.toUtcLocalDateTime(now)));
        });
    }

    @Override
    public List<StoredNode> findAll() {
        return jdbc.query(dialect.findAllNodes(), (rs, _) -> JdbcSessions.read(rs));
    }

    @Override
    public int deleteHeartbeatsBefore(Instant cutoff) {
        return jdbc.update(dialect.deleteHeartbeatsBefore(), new MapSqlParameterSource()
                .addValue("cutoff", JdbcTimestamps.toUtcLocalDateTime(cutoff)).addValue("revoked", true));
    }

    private static MapSqlParameterSource parameters(StoredNode node) {
        return new MapSqlParameterSource("nodeId", node.nodeId()).addValue("state", node.state().name())
                .addValue("lastHeartbeatAt", JdbcTimestamps.toUtcLocalDateTime(node.lastHeartbeatAt()))
                .addValue("expiresAt", JdbcTimestamps.toUtcLocalDateTime(node.expiresAt())).addValue("revoked", node.revoked());
    }

    private static void requireTtl(Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        io.mohs.core.TimePrecision.micros(ttl);
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("session ttl must be positive");
        }
    }
}
