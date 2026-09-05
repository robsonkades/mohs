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
package io.mohs.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobSpec;
import io.mohs.core.event.Enqueued;
import io.mohs.core.execution.Execution;
import io.mohs.core.resource.MohsRunner;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcHistoryStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcNodeStore;
import io.mohs.store.jdbc.JdbcStoreTransactions;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * {@code ScheduleCommand}'s terminal against the real JDBC ports (H2) — the Idempotent Receiver (EIP)
 * contract depends on {@code mohs_idempotency}'s primary-key conflict in the schema, so a mocked store
 * would prove nothing here.
 */
class ScheduleCommandImplTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    record Handler() {
    }

    private DataSource dataSource;
    private Mohs mohs;
    private final AtomicInteger wakes = new AtomicInteger();

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        MutableClock clock = new MutableClock(NOW, ZoneId.of("UTC"));
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, clock, new H2JdbcDelegate());
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock, new H2JdbcDelegate());
        JdbcHistoryStore historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new H2JdbcDelegate());
        JdbcWorkQueue workQueue = new JdbcWorkQueue(dataSource, new H2JdbcDelegate(), batchStore);
        JdbcLeaseStore leaseStore = new JdbcLeaseStore(dataSource, new H2JdbcDelegate(), batchStore);
        mohs = new MohsImpl(jobStore, workQueue, historyStore, leaseStore, new JdbcStoreTransactions(dataSource),
                new JdbcNodeStore(dataSource, new H2JdbcDelegate()), mock(RateLimitStore.class), new HandlerRegistry(), clock,
                mock(MohsLifecycle.class), batchStore, new BatchCompletionCallbacks(),
                new RunnerRegistry(List.of(MohsRunner.io("io").build())), wakes::incrementAndGet);
        mohs.define(JobDefinition.of("welcome-email", Handler.class, JobSpec::onDemand));
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:schedule-command-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private int executionCount() {
        Integer count = new JdbcTemplate(dataSource).queryForObject("SELECT COUNT(*) FROM mohs_execution", Integer.class);
        return count == null ? 0 : count;
    }

    /** The local wake-up tier: an already-due terminal fires the local signal; a future schedule does not — waking the loop for a row that is still invisible would be a wasted lap. */
    @Test
    void aDueTerminalFiresTheLocalWakeSignalAndAFutureOneDoesNot() {
        mohs.schedule("welcome-email", "hello").now();
        assertThat(wakes.get()).isEqualTo(1);

        mohs.schedule("welcome-email", "hello").after(Duration.ofHours(1));
        assertThat(wakes.get()).isEqualTo(1);
    }

    @Test
    void schedulingWithAnUnknownJobFailsWithAnErrorThatTeaches() {
        assertThatThrownBy(() -> mohs.schedule("ghost", "payload").now())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Mohs.define");
    }

    /**
     * The scheduler actor is load-bearing (the fixed-delay rearm, the upsert's cure) — a manual schedule
     * may never pass itself off as the engine.
     *
     * <p>The rejection is case- and whitespace-insensitive because the cure compares the actor in the
     * DATABASE, whose default collation (MySQL, SQL Server) is case-insensitive: "Scheduler" would slip
     * past an exact guard and still count as a scheduler occurrence in the SQL predicate.
     */
    @Test
    void reservedSchedulerActorIsRejectedInAnyCaseWithAnErrorThatTeaches() {
        for (String forged : new String[] {Execution.SCHEDULER_ACTOR, "Scheduler", " scheduler ", "SCHEDULER"}) {
            assertThatThrownBy(() -> mohs.schedule("welcome-email", "hello").as(forged))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reserved for engine-fired occurrences");
        }
    }

    /** The header's use case: a client resends the POST after a network timeout — the same receipt, zero duplication. */
    @Test
    void sameIdempotencyKeyReturnsTheOriginalReceiptWithoutDuplicating() {
        Enqueued first = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();

        Enqueued retry = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();

        assertThat(retry.executionId()).isEqualTo(first.executionId());
        assertThat(retry.scheduledAt()).isEqualTo(first.scheduledAt());
        assertThat(executionCount()).isEqualTo(1);
    }

    @Test
    void distinctIdempotencyKeysProduceDistinctExecutions() {
        Enqueued first = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
        Enqueued second = mohs.schedule("welcome-email", "hello").idempotencyKey("req-2").now();

        assertThat(second.executionId()).isNotEqualTo(first.executionId());
        assertThat(executionCount()).isEqualTo(2);
    }

    /** Refused at the call, not at the write: a dialect that truncated it would let two keys collide and deduplicate a new schedule away. */
    @Test
    void aBlankIdempotencyKeyIsRefusedBeforeAnyWrite() {
        assertThatThrownBy(() -> mohs.schedule("welcome-email", "hello").idempotencyKey("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
        assertThat(executionCount()).isZero();
    }

    @Test
    void anIdempotencyKeyWiderThanTheColumnIsRefusedBeforeAnyWrite() {
        assertThatThrownBy(() -> mohs.schedule("welcome-email", "hello").idempotencyKey("k".repeat(256)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("255");
        assertThat(executionCount()).isZero();
    }

    @Test
    void executionsWithoutAnIdempotencyKeyNeverCollide() {
        mohs.schedule("welcome-email", "hello").now();
        mohs.schedule("welcome-email", "hello").now();

        assertThat(executionCount()).isEqualTo(2);
    }

    /**
     * The showcase scenario: deduplication INSIDE the host's transaction. The Idempotent Receiver's
     * primary-key conflict must not doom the caller's transaction — the savepoint (NESTED in
     * {@code JdbcStoreTransactions}) undoes only the enqueue unit, the connection stays healthy (on
     * Postgres, without a savepoint it would be aborted — 25P02) and the host's commit remains possible.
     * With REQUIRED this blew up with an {@code UnexpectedRollbackException} AFTER we had returned a
     * successful receipt.
     */
    @Test
    void duplicateIdempotencyKeyInsideAHostTransactionLeavesItCommittable() {
        org.springframework.transaction.support.TransactionTemplate host =
                new org.springframework.transaction.support.TransactionTemplate(
                        new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource));
        Enqueued[] receipts = new Enqueued[2];

        host.executeWithoutResult(_ -> {
            receipts[0] = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
            receipts[1] = mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
        });

        assertThat(receipts[1].executionId()).isEqualTo(receipts[0].executionId());
        assertThat(executionCount()).isEqualTo(1);
    }

    /** The real retry race: two simultaneous POSTs with the same key — the database decides, and both receive the same receipt. */
    @Test
    void concurrentSchedulesWithTheSameKeyConvergeOnASingleExecution() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Callable<Enqueued> schedule = () -> {
            barrier.await();
            return mohs.schedule("welcome-email", "hello").idempotencyKey("req-1").now();
        };

        Future<Enqueued> futureA = executor.submit(schedule);
        Future<Enqueued> futureB = executor.submit(schedule);
        Enqueued a = futureA.get(10, TimeUnit.SECONDS);
        Enqueued b = futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(a.executionId()).isEqualTo(b.executionId());
        assertThat(executionCount()).isEqualTo(1);
    }
}
