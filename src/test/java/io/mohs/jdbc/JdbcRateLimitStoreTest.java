package io.mohs.jdbc;

import java.time.Duration;
import java.util.UUID;

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
}
