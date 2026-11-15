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
    @Test
    void sessionTtlCannotSilentlyLoseSubMicrosecondPrecision() {
        var catalog = new io.mohs.engine.ExecutableCatalog(java.util.Map.of());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.register("01991337-0000-7000-8000-000000000087", EngineState.RUNNING,
                Duration.ofNanos(1), catalog)).isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.renew("01991337-0000-7000-8000-000000000087", EngineState.RUNNING,
                Duration.ofSeconds(1).plusNanos(1))).isInstanceOf(IllegalArgumentException.class);
        assertThat(store.findAll()).isEmpty();
    }

    private static final Duration TTL = Duration.ofSeconds(15);
    private JdbcNodeStore store;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        var source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:session-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(source);
        store = new JdbcNodeStore(source, new H2JdbcDelegate());
        jdbc = new JdbcTemplate(source);
    }

    @Test
    void registerUsesDatabaseTimeAndRepeatingItReturnsTheSameSession() {
        var registered = store.register("01991337-0000-7000-8000-000000000064", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        assertThat(registered.expiresAt()).isEqualTo(registered.lastHeartbeatAt().plus(TTL));
        assertThat(registered.revoked()).isFalse();
        assertThat(store.register("01991337-0000-7000-8000-000000000064", EngineState.PAUSED, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()))).isEqualTo(registered);
        assertThat(store.findAll()).containsExactly(registered);
    }

    @Test
    void renewChangesStateWithoutChangingIdentity() {
        var original = store.register("01991337-0000-7000-8000-000000000064", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        var renewed = store.renew("01991337-0000-7000-8000-000000000064", EngineState.DRAINING, TTL).orElseThrow();
        assertThat(renewed.nodeId()).isEqualTo(original.nodeId());
        assertThat(renewed.state()).isEqualTo(EngineState.DRAINING);
        assertThat(renewed.lastHeartbeatAt()).isAfterOrEqualTo(original.lastHeartbeatAt());
        assertThat(renewed.expiresAt()).isEqualTo(renewed.lastHeartbeatAt().plus(TTL));
    }

    @Test
    void expiredOrStoppedIdentitiesCannotBeResurrected() {
        var original = store.register("01991337-0000-7000-8000-00000000004c", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        jdbc.update("UPDATE mohs_nodes SET expires_at = ? WHERE node_id = '01991337-0000-7000-8000-00000000004c'",
                JdbcTimestamps.toUtcLocalDateTime(original.lastHeartbeatAt().minusSeconds(1)));
        assertThat(store.renew("01991337-0000-7000-8000-00000000004c", EngineState.RUNNING, TTL)).isEmpty();
        assertThat(store.revokeExpired("01991337-0000-7000-8000-00000000004c")).isTrue();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.register("01991337-0000-7000-8000-00000000004c", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource())))
                .isInstanceOf(io.mohs.engine.NodeStore.SessionExpired.class);
        store.register("01991337-0000-7000-8000-000000000086", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        store.stop("01991337-0000-7000-8000-000000000086");
        assertThat(store.renew("01991337-0000-7000-8000-000000000086", EngineState.RUNNING, TTL)).isEmpty();
    }

    @Test
    void healthySessionCannotBeRevokedByAnObsoleteFailureObservation() {
        store.register("01991337-0000-7000-8000-000000000051", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        assertThat(store.revokeExpired("01991337-0000-7000-8000-000000000051")).isFalse();
        assertThat(store.renew("01991337-0000-7000-8000-000000000051", EngineState.RUNNING, TTL)).isPresent();
        assertThat(store.renew("01991337-0000-7000-8000-00000000008e", EngineState.RUNNING, TTL)).isEmpty();
    }

    @Test
    void findAllReturnsEverySessionAndNoSyntheticNodes() {
        assertThat(store.findAll()).isEmpty();
        store.register("01991337-0000-7000-8000-000000000008", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        store.register("01991337-0000-7000-8000-00000000000a", EngineState.PAUSED, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        assertThat(store.findAll()).extracting(StoredNode::nodeId).containsExactlyInAnyOrder("01991337-0000-7000-8000-000000000008", "01991337-0000-7000-8000-00000000000a");
    }

    @Test
    void purgeRequiresRevocationStrictAgeAndNoOutstandingOwnership() {
        var cutoff = store.register("01991337-0000-7000-8000-000000000084", EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource())).lastHeartbeatAt();
        for (String id : java.util.List.of("01991337-0001-7000-8000-000000000090", "01991337-0002-7000-8000-000000000099", "01991337-0002-7000-8000-00000000009a")) store.register(id, EngineState.RUNNING, TTL, io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        for (String id : java.util.List.of("01991337-0000-7000-8000-000000000084", "01991337-0001-7000-8000-000000000090", "01991337-0002-7000-8000-00000000009a")) store.stop(id);
        jdbc.update("UPDATE mohs_nodes SET last_heartbeat_at = ?", JdbcTimestamps.toUtcLocalDateTime(cutoff.minusSeconds(1)));
        jdbc.update("UPDATE mohs_nodes SET last_heartbeat_at = ? WHERE node_id = '01991337-0001-7000-8000-000000000090'", JdbcTimestamps.toUtcLocalDateTime(cutoff));
        jdbc.update("""
                INSERT INTO mohs_work (work_state, execution_id, job_key, node_id, claim_token, attempt, priority, claimed_at)
                VALUES ('LEASED', 'execution', 'job', '01991337-0002-7000-8000-00000000009a', 'token', 1, 20, ?)
                """, JdbcTimestamps.toUtcLocalDateTime(cutoff));
        assertThat(store.deleteHeartbeatsBefore(cutoff)).isEqualTo(1);
        assertThat(store.findAll()).extracting(StoredNode::nodeId).containsExactlyInAnyOrder("01991337-0001-7000-8000-000000000090", "01991337-0002-7000-8000-000000000099", "01991337-0002-7000-8000-00000000009a");
    }
}
