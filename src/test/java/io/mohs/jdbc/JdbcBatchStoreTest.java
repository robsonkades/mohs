package io.mohs.jdbc;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.engine.BatchCounters;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcBatchStoreTest {

    private JdbcBatchStore store;

    @BeforeEach
    void setUp() {
        DataSource dataSource = freshH2DataSource();
        MutableClock clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
        store = new JdbcBatchStore(dataSource, clock);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:batch-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    @Test
    void insertStartsAllCountersAtZeroExceptTotal() {
        store.insert("batch-1", 10);

        BatchCounters counters = store.find("batch-1").orElseThrow();

        assertThat(counters.total()).isEqualTo(10);
        assertThat(counters.succeeded()).isZero();
        assertThat(counters.failed()).isZero();
        assertThat(counters.pending()).isEqualTo(10);
    }

    @Test
    void insertRejectsNegativeTotal() {
        assertThatThrownBy(() -> store.insert("batch-1", -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findReturnsEmptyForUnknownBatch() {
        assertThat(store.find("ghost")).isEmpty();
    }

    @Test
    void incrementSucceededAndFailedUpdateTheCounters() {
        store.insert("batch-1", 10);

        store.incrementSucceeded("batch-1");
        store.incrementSucceeded("batch-1");
        store.incrementFailed("batch-1");

        BatchCounters counters = store.find("batch-1").orElseThrow();
        assertThat(counters.succeeded()).isEqualTo(2);
        assertThat(counters.failed()).isEqualTo(1);
        assertThat(counters.pending()).isEqualTo(7);
    }

    @Test
    void incrementsAreAtomicUnderConcurrentCompletion() throws InterruptedException {
        store.insert("batch-1", 100);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 100).forEach(i -> executor.submit(() -> store.incrementSucceeded("batch-1")));
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        Optional<BatchCounters> counters = store.find("batch-1");
        assertThat(counters).map(BatchCounters::succeeded).contains(100);
    }
}
