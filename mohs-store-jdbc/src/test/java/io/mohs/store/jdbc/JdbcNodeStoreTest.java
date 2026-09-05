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
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.EngineState;
import io.mohs.engine.StoredNode;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcNodeStoreTest {

    /** The scenarios' node lease — the value itself does not matter, only the {@code at + TTL} arithmetic. */
    private static final Duration NODE_LEASE_TTL = Duration.ofSeconds(15);

    private JdbcNodeStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcNodeStore(freshH2DataSource(), new H2JdbcDelegate());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:node-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private void heartbeat(String nodeId, EngineState state, long epoch, Instant at) {
        store.heartbeat(nodeId, state, epoch, at, at.plus(NODE_LEASE_TTL));
    }

    @Test
    void heartbeatInsertsANewNode() {
        Instant at = Instant.parse("2026-08-14T12:00:00Z");

        heartbeat("node-1", EngineState.RUNNING, 1, at);

        assertThat(store.findAll()).containsExactly(
                new StoredNode("node-1", EngineState.RUNNING, at, 1, at.plus(NODE_LEASE_TTL)));
    }

    @Test
    void heartbeatOnExistingNodeUpdatesStateEpochAndLease() {
        heartbeat("node-1", EngineState.RUNNING, 1, Instant.parse("2026-08-14T12:00:00Z"));

        Instant later = Instant.parse("2026-08-14T12:00:30Z");
        heartbeat("node-1", EngineState.DRAINING, 2, later);

        assertThat(store.findAll()).containsExactly(
                new StoredNode("node-1", EngineState.DRAINING, later, 2, later.plus(NODE_LEASE_TTL)));
    }

    @Test
    void findAllReturnsEveryNode() {
        Instant at = Instant.parse("2026-08-14T12:00:00Z");
        heartbeat("node-1", EngineState.RUNNING, 1, at);
        heartbeat("node-2", EngineState.RUNNING, 1, at);

        assertThat(store.findAll()).extracting(StoredNode::nodeId).containsExactlyInAnyOrder("node-1", "node-2");
    }

    @Test
    void findAllReturnsEmptyWhenNoNodeHasEverBeaten() {
        assertThat(store.findAll()).isEmpty();
    }

    /** Mixed-version tolerance: a row written by an older jar, with no {@code expires_at}, comes back with the field null — the reaper decides by the heartbeat's staleness. */
    @Test
    void findAllReadsALegacyRowWithoutANodeLease() {
        Instant at = Instant.parse("2026-08-14T12:00:00Z");
        DataSource dataSource = freshH2DataSource();
        new JdbcTemplate(dataSource).update(
                "INSERT INTO mohs_nodes (node_id, state, last_heartbeat_at, epoch) VALUES (?, 'RUNNING', ?, 0)",
                "legacy-node", JdbcTimestamps.toUtcLocalDateTime(at));

        assertThat(new JdbcNodeStore(dataSource, new H2JdbcDelegate()).findAll()).containsExactly(
                new StoredNode("legacy-node", EngineState.RUNNING, at, 0, null));
    }

    /** A cutoff strictly BEFORE: a row exactly at the cutoff stays (a heartbeat at the threshold is still a signal, not rubbish). */
    @Test
    void deleteHeartbeatsBeforeRemovesOnlyStrictlyStaleNodes() {
        Instant cutoff = Instant.parse("2026-08-14T11:30:00Z");
        heartbeat("stale", EngineState.RUNNING, 1, cutoff.minusSeconds(1));
        heartbeat("at-cutoff", EngineState.RUNNING, 1, cutoff);
        heartbeat("fresh", EngineState.RUNNING, 1, Instant.parse("2026-08-14T12:00:00Z"));

        int purged = store.deleteHeartbeatsBefore(cutoff);

        assertThat(purged).isEqualTo(1);
        assertThat(store.findAll()).extracting(StoredNode::nodeId).containsExactlyInAnyOrder("at-cutoff", "fresh");
    }
}
