package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.SubmissionStore;
import io.mohs.engine.WorkQueue;
import io.mohs.test.InputFixtures;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcRetirementProtocolTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void staleHostSnapshotCannotRetireWithoutDrainingNewlyCommittedWork(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        var host = new TransactionTemplate(new DataSourceTransactionManager(backend.dataSource()));
        host.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        host.setTimeout(10);
        assertThatThrownBy(() -> host.executeWithoutResult(_ -> {
            assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_execution", Long.class)).isZero();
            try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
                threads.submit(() -> f.enqueue("01991337-0000-7000-8000-000000000009")).get(5, TimeUnit.SECONDS);
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
            f.jobs.remove(f.job);
        })).isInstanceOf(org.springframework.transaction.IllegalTransactionStateException.class)
                .hasMessageContaining("requires READ_COMMITTED");
        assertThat(f.jobs.find(f.job)).isPresent();
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Long.class)).isOne();
        f.jobs.remove(f.job);
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Long.class)).isZero();
        assertThat(f.history.find(ExecutionId.of("01991337-0000-7000-8000-000000000009"), NOW).orElseThrow().state())
                .isEqualTo(ExecutionState.CANCELLED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void bulkRetirementCrossesParameterChunksAndPreservesTheAcquiredMember(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        int total = 2102;
        TestWork.batch(backend.dataSource(), backend.delegate(), "01991337-0000-7000-8000-000000000080", "bulk retirement", total);
        var executions = IntStream.range(0, total).mapToObj(i -> new SubmissionStore.NewExecution(
                ExecutionId.of(io.mohs.test.TestIds.value("retiring-" + i)), f.job, 0, 20, NOW, NOW, "test", "01991337-0000-7000-8000-000000000080", null,
                InputFixtures.encode(f.job, "input"), SubmissionStore.Origin.MANUAL, null)).toList();
        TestWork.history(backend.dataSource(), backend.delegate(), executions);
        TestWork.ready(backend.dataSource(), backend.delegate(), executions.stream().map(e ->
                new WorkQueue.ReadyEntry(e.executionId(), e.jobKey(), 0, 20, 1, NOW)).toList());
        f.register("01991337-0000-7000-8000-000000000006");
        var grant = f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW).getFirst();
        f.jobs.remove(f.job);
        assertThat(f.jobs.find(f.job)).isEmpty();
        long revision = f.jdbc.queryForObject("SELECT schedule_revision FROM mohs_job_definitions WHERE job_key = ?", Long.class, f.job.value());
        f.jobs.remove(f.job);
        assertThat(f.jdbc.queryForObject("SELECT schedule_revision FROM mohs_job_definitions WHERE job_key = ?", Long.class, f.job.value()))
                .isEqualTo(revision);
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Long.class)).isZero();
        var batches = new JdbcBatchStore(backend.dataSource(), backend.delegate());
        assertThat(batches.find("01991337-0000-7000-8000-000000000080").orElseThrow().failed()).isEqualTo(total - 1);
        assertThat(batches.find("01991337-0000-7000-8000-000000000080").orElseThrow().pending()).isOne();
        assertThat(f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006"))).hasSize(1);
        var result = new LeaseStore.CompletionResult(grant.executionId(), f.job, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
        assertThat(f.leases.complete(List.of(result)).get(grant.executionId()).closedBatch()).isNotNull();
        assertThat(batches.find("01991337-0000-7000-8000-000000000080").orElseThrow().pending()).isZero();
    }
}
