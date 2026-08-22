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

import static org.assertj.core.api.Assertions.assertThat;

class JdbcNodeStoreTest {

    /** Lease de nó dos cenários (ADR-0051) — o valor em si não importa, só a aritmética {@code at + TTL}. */
    private static final Duration NODE_LEASE_TTL = Duration.ofSeconds(15);

    private JdbcNodeStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcNodeStore(freshH2DataSource());
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

    /** Tolerância de versão mista (ADR-0051): linha gravada por jar antigo, sem {@code expires_at}, volta com o campo nulo — o reaper decide pela staleness do heartbeat. */
    @Test
    void findAllReadsALegacyRowWithoutANodeLease() {
        Instant at = Instant.parse("2026-08-14T12:00:00Z");
        DataSource dataSource = freshH2DataSource();
        new JdbcTemplate(dataSource).update(
                "INSERT INTO mohs_nodes (node_id, state, last_heartbeat_at, epoch) VALUES (?, 'RUNNING', ?, 0)",
                "legacy-node", JdbcTimestamps.toUtcLocalDateTime(at));

        assertThat(new JdbcNodeStore(dataSource).findAll()).containsExactly(
                new StoredNode("legacy-node", EngineState.RUNNING, at, 0, null));
    }

    /** ADR-0041: corte estritamente ANTES do cutoff — linha exatamente no cutoff fica (heartbeat no limiar ainda é sinal, não lixo). */
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
