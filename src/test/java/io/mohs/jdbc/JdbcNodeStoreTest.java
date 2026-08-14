package io.mohs.jdbc;

import java.time.Instant;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.EngineState;
import io.mohs.engine.StoredNode;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcNodeStoreTest {

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

    @Test
    void heartbeatInsertsANewNode() {
        Instant at = Instant.parse("2026-08-14T12:00:00Z");

        store.heartbeat("node-1", EngineState.RUNNING, at);

        assertThat(store.findAll()).containsExactly(new StoredNode("node-1", EngineState.RUNNING, at));
    }

    @Test
    void heartbeatOnExistingNodeUpdatesStateAndTimestamp() {
        store.heartbeat("node-1", EngineState.RUNNING, Instant.parse("2026-08-14T12:00:00Z"));

        Instant later = Instant.parse("2026-08-14T12:00:30Z");
        store.heartbeat("node-1", EngineState.DRAINING, later);

        assertThat(store.findAll()).containsExactly(new StoredNode("node-1", EngineState.DRAINING, later));
    }

    @Test
    void findAllReturnsEveryNode() {
        Instant at = Instant.parse("2026-08-14T12:00:00Z");
        store.heartbeat("node-1", EngineState.RUNNING, at);
        store.heartbeat("node-2", EngineState.RUNNING, at);

        assertThat(store.findAll()).extracting(StoredNode::nodeId).containsExactlyInAnyOrder("node-1", "node-2");
    }

    @Test
    void findAllReturnsEmptyWhenNoNodeHasEverBeaten() {
        assertThat(store.findAll()).isEmpty();
    }
}
