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

import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.engine.BatchCounters;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcBatchStoreTest {

    private JdbcBatchStore store;
    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        MutableClock clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
        store = new JdbcBatchStore(dataSource, clock, new H2JdbcDelegate());
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
        store.insert("batch-1", "nightly", 10);

        BatchCounters counters = store.find("batch-1").orElseThrow();

        assertThat(counters.total()).isEqualTo(10);
        assertThat(counters.succeeded()).isZero();
        assertThat(counters.failed()).isZero();
        assertThat(counters.pending()).isEqualTo(10);
    }

    @Test
    void insertRejectsNegativeTotal() {
        assertThatThrownBy(() -> store.insert("batch-1", "nightly", -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findReturnsEmptyForUnknownBatch() {
        assertThat(store.find("ghost")).isEmpty();
    }

    @Test
    void incrementSucceededAndFailedUpdateTheCounters() {
        store.insert("batch-1", "nightly", 10);

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
        store.insert("batch-1", "nightly", 100);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 100).forEach(i -> executor.submit(() -> store.incrementSucceeded("batch-1")));
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        Optional<BatchCounters> counters = store.find("batch-1");
        assertThat(counters).map(BatchCounters::succeeded).contains(100);
    }

    /**
     * The property the batch design buys, and the reason the increment returns the balance: with 100
     * members completing at once, ONE caller sees {@code pending() == 0} — and that is the one that fires
     * {@code BatchCompleted}.
     *
     * <p>Each completion runs in its own transaction because that is what makes the re-read stable (the
     * UPDATE's row lock holds until the commit). This test would fail with TWO closers if the increment
     * and the read sat outside a transaction — exactly the failure mode described for the derived design,
     * and which does not exist here.
     */
    @Test
    void exactlyOneConcurrentCompletionSeesTheBatchClose() throws InterruptedException {
        store.insert("batch-1", "nightly", 100);
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        AtomicInteger closers = new AtomicInteger();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 100).forEach(i -> executor.submit(() ->
                    transaction.executeWithoutResult(status -> {
                        if (store.incrementSucceeded("batch-1").pending() == 0) {
                            closers.incrementAndGet();
                        }
                    })));
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(closers).hasValue(1);
        assertThat(store.find("batch-1").orElseThrow().succeeded()).isEqualTo(100);
    }

    @Test
    void countingAMemberIntoAnUnknownBatchFailsLoudly() {
        assertThatThrownBy(() -> store.incrementSucceeded("ghost"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ghost");
    }
}
