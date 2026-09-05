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
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.event.Enqueued;
import io.mohs.core.resource.MohsRunner;
import io.mohs.engine.BatchCompletionCallbacks;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.MohsImpl;
import io.mohs.engine.RateLimitStore;
import io.mohs.engine.RunnerRegistry;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The Postgres twin of {@code ScheduleCommandImplTest}'s savepoint test: it is HERE that the
 * {@code REQUIRED} bug was worst — without a savepoint, the deduplication's primary-key violation aborts
 * the entire transaction ({@code 25P02 current transaction is aborted}) and the recovery path that reads
 * the winner runs on a dead connection.
 *
 * <p>H2 proves the cross-delegate half (rollback-only avoided); only real Postgres proves the connection
 * stays healthy AFTER the conflict.
 */
class ScheduleCommandPostgresTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    private DataSource dataSource;
    private Mohs mohs;

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock, new PostgresJdbcDelegate());
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock, new PostgresJdbcDelegate());
        JdbcHistoryStore historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new PostgresJdbcDelegate());
        JdbcWorkQueue workQueue = new JdbcWorkQueue(dataSource, new PostgresJdbcDelegate(), batchStore);
        JdbcLeaseStore leaseStore = new JdbcLeaseStore(dataSource, new PostgresJdbcDelegate(), batchStore);
        mohs = new MohsImpl(jobStore, workQueue, historyStore, leaseStore, new JdbcStoreTransactions(dataSource),
                new JdbcNodeStore(dataSource, new PostgresJdbcDelegate()), mock(RateLimitStore.class), new HandlerRegistry(), clock,
                mock(MohsLifecycle.class), batchStore, new BatchCompletionCallbacks(),
                new RunnerRegistry(List.of(MohsRunner.io("io").build())), () -> { }, List.of(), Runnable::run);
        mohs.define(JobDefinition.of("welcome-email", Handler.class, JobSpec::onDemand));
    }

    /** The proof only the 25P02 delegate can give: a primary-key conflict, the savepoint undone, and the SAME connection reads the winner while the host commits. */
    @Test
    void duplicateIdempotencyKeyInsideAHostTransactionLeavesTheConnectionUsable() {
        TransactionTemplate host = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        Enqueued[] receipts = new Enqueued[2];

        host.executeWithoutResult(_ -> {
            receipts[0] = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
            receipts[1] = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
        });

        assertThat(receipts[1].executionId()).isEqualTo(receipts[0].executionId());
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "SELECT COUNT(*) FROM mohs_execution", Integer.class)).isEqualTo(1);
    }
}
