package io.mohs.jdbc;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.resource.RateLimit;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcRateLimitStoreTest {

    private JdbcRateLimitStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcRateLimitStore(freshH2DataSource());
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:rate-limit-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(h2);
        return h2;
    }

    @Test
    void upsertInsertsANewRateLimit() {
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));

        store.upsert(rateLimit);

        assertThat(store.find("smtp")).contains(rateLimit);
    }

    @Test
    void upsertOnExistingNameAppliesChanges() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));

        store.upsert(new RateLimit("smtp", 250, Duration.ofSeconds(30)));

        assertThat(store.find("smtp")).contains(new RateLimit("smtp", 250, Duration.ofSeconds(30)));
    }

    @Test
    void findReturnsEmptyForUnknownName() {
        assertThat(store.find("ghost")).isEmpty();
    }

    @Test
    void findAllReturnsEveryRateLimit() {
        store.upsert(new RateLimit("smtp", 100, Duration.ofMinutes(1)));
        store.upsert(new RateLimit("partner-api", 50, Duration.ofSeconds(10)));

        try (var all = store.findAll()) {
            assertThat(all.map(RateLimit::name)).containsExactlyInAnyOrder("smtp", "partner-api");
        }
    }

    /** CONC-2 — ver JdbcQueueStoreTest.upsertHandlesConcurrentFirstTimeInsertWithoutThrowing. */
    @Test
    void upsertHandlesConcurrentFirstTimeInsertWithoutThrowing() throws Exception {
        RateLimit rateLimit = new RateLimit("smtp", 100, Duration.ofMinutes(1));
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Callable<RateLimit> upsert = () -> {
            barrier.await();
            return store.upsert(rateLimit);
        };

        Future<RateLimit> futureA = executor.submit(upsert);
        Future<RateLimit> futureB = executor.submit(upsert);
        futureA.get(10, TimeUnit.SECONDS);
        futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(store.find("smtp")).contains(rateLimit);
    }
}
