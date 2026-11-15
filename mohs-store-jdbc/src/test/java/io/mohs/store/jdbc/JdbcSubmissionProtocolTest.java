package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import io.mohs.core.execution.ExecutionId;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.SubmissionStore;
import io.mohs.engine.WorkQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class JdbcSubmissionProtocolTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void retirementAfterLocalValidationPreventsAcceptance(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var input = prepared(f, "01991337-0003-7000-8000-00000000009c", null, "key");
        assertThat(f.jobs.find(f.job)).isPresent();
        f.jobs.remove(f.job);
        var durable = new AtomicInteger();
        assertThatThrownBy(() -> store(backend, f, f.queue).accept(List.of(input), null, durable::incrementAndGet))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no active job");
        assertEmpty(f);
        assertThat(durable).hasValue(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void acceptedWorkIsVisibleToRetirementAfterHostCommit(JdbcJobBindingTest.Backend backend) throws Exception {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var accepted = new CountDownLatch(1);
        var commit = new CountDownLatch(1);
        var retireStarted = new CountDownLatch(1);
        var durable = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var submission = executor.submit(() -> host(backend).executeWithoutResult(_ -> {
                store(backend, f, f.queue).accept(List.of(prepared(f, "01991337-0003-7000-8000-00000000009d", null, "key")), null,
                        durable::incrementAndGet);
                accepted.countDown();
                await(commit);
            }));
            try {
                assertThat(accepted.await(5, TimeUnit.SECONDS)).isTrue();
                var retirement = executor.submit(() -> { retireStarted.countDown(); f.jobs.remove(f.job); });
                assertThat(retireStarted.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> retirement.get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(java.util.concurrent.TimeoutException.class);
                assertThat(durable).hasValue(0);
                commit.countDown();
                submission.get(5, TimeUnit.SECONDS);
                retirement.get(5, TimeUnit.SECONDS);
            } finally {
                commit.countDown();
            }
        }
        assertThat(durable).hasValue(1);
        assertThat(f.jdbc.queryForObject("SELECT state FROM mohs_execution WHERE execution_id = '01991337-0003-7000-8000-00000000009d'", String.class))
                .isEqualTo("CANCELLED");
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Long.class)).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void hostRollbackRemovesReceiptHistoryAndQueueAndPublishesNothing(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var durable = new AtomicInteger();
        host(backend).executeWithoutResult(status -> {
            store(backend, f, f.queue).accept(List.of(prepared(f, "01991337-0003-7000-8000-00000000009e", null, "key")), null,
                    durable::incrementAndGet);
            assertThat(durable).hasValue(0);
            status.setRollbackOnly();
        });
        assertEmpty(f);
        assertThat(durable).hasValue(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void failureAfterQueueInsertionRollsBackAllBatchMembers(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var broken = new JdbcSubmissionStore(new TestJdbcFaults(backend.dataSource()), backend.delegate(), java.time.Duration.ofDays(7));
        var durable = new AtomicInteger();
        assertThatThrownBy(() -> broken.accept(
                List.of(prepared(f, "01991337-0003-7000-8000-00000000009f", "01991337-0000-7000-8000-00000000000d", null), prepared(f, "01991337-0003-7000-8000-0000000000a0", "01991337-0000-7000-8000-00000000000d", null)),
                new SubmissionStore.NewBatch("01991337-0000-7000-8000-00000000000d", "atomic members"), durable::incrementAndGet))
                .hasMessageContaining("queue response lost");
        assertEmpty(f);
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_batches", Long.class)).isZero();
        assertThat(durable).hasValue(0);
    }

    private static JdbcSubmissionStore store(JdbcJobBindingTest.Backend backend, JdbcOwnershipProtocolTest.Fixture f, WorkQueue queue) {
        return new JdbcSubmissionStore(backend.dataSource(), backend.delegate(), java.time.Duration.ofDays(7));
    }

    private static SubmissionStore.NewExecution prepared(JdbcOwnershipProtocolTest.Fixture f, String id, String batch, String key) {
        return new SubmissionStore.NewExecution(ExecutionId.of(id), f.job, 0, 20,
                NOW, NOW, "test", batch, key, io.mohs.test.InputFixtures.encode(f.job, "input"), SubmissionStore.Origin.MANUAL, null);
    }

    private static TransactionTemplate host(JdbcJobBindingTest.Backend backend) {
        var host = new TransactionTemplate(new DataSourceTransactionManager(backend.dataSource()));
        host.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        host.setTimeout(10);
        return host;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("barrier timed out");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static void assertEmpty(JdbcOwnershipProtocolTest.Fixture f) {
        for (String table : List.of("mohs_execution", "mohs_work", "mohs_idempotency")) {
            assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class)).as(table).isZero();
        }
    }
}
