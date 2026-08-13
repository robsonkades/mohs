package io.mohs.jdbc;

import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.resource.JobQueue;
import io.mohs.engine.StoredQueue;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcQueueStoreTest {

    private JdbcQueueStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcQueueStore(freshH2DataSource());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:queue-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(h2);
        return h2;
    }

    @Test
    void upsertInsertsANewQueueWithZeroRunningCount() {
        store.upsert(JobQueue.named("emails").maxConcurrent(10).build());

        StoredQueue stored = store.find("emails").orElseThrow();

        assertThat(stored.queue()).isEqualTo(JobQueue.named("emails").maxConcurrent(10).build());
        assertThat(stored.runningCount()).isZero();
    }

    @Test
    void upsertOnExistingNameAppliesDefinitionalChanges() {
        store.upsert(JobQueue.named("emails").maxConcurrent(10).build());

        store.upsert(JobQueue.named("emails").maxConcurrent(25).build());

        assertThat(store.find("emails")).map(StoredQueue::queue)
                .contains(JobQueue.named("emails").maxConcurrent(25).build());
    }

    @Test
    void findReturnsEmptyForUnknownName() {
        assertThat(store.find("ghost")).isEmpty();
    }

    @Test
    void findAllReturnsEveryStoredQueue() {
        store.upsert(JobQueue.named("emails").maxConcurrent(10).build());
        store.upsert(JobQueue.named("sms").maxConcurrent(5).build());

        try (var all = store.findAll()) {
            assertThat(all.map(sq -> sq.queue().name())).containsExactlyInAnyOrder("emails", "sms");
        }
    }
}
