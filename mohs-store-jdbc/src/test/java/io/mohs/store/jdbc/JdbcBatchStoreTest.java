package io.mohs.store.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.EngineState;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.SubmissionStore;
import io.mohs.test.InputFixtures;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcBatchStoreTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void concurrentTerminalOperationsCountEveryMemberAndReturnOneCloser(JdbcJobBindingTest.Backend backend) throws Exception {
        var source = backend.dataSource();
        var dialect = backend.delegate();
        var batches = new JdbcBatchStore(source, dialect);
        assertThat(batches.find("absent")).isEmpty();
        var queue = new JdbcWorkQueue(source, dialect);
        var leases = new JdbcLeaseStore(source, dialect);
        var executions = IntStream.range(0, 12).mapToObj(i -> {
            var key = JobKey.of("batch-job-" + i);
            return new SubmissionStore.NewExecution(ExecutionId.of(io.mohs.test.TestIds.value("batch-member-" + i)), key, 0, 20, NOW, NOW,
                    "test", "01991337-0000-7000-8000-00000000000d", null, InputFixtures.encode(key, "input"), SubmissionStore.Origin.MANUAL, null);
        }).toList();
        TestSubmissions.declare(source, dialect, executions);
        new JdbcSubmissionStore(source, dialect, Duration.ofDays(7))
                .accept(executions, new SubmissionStore.NewBatch("01991337-0000-7000-8000-00000000000d", "nightly"), () -> {});
        var initial = batches.find("01991337-0000-7000-8000-00000000000d").orElseThrow();
        assertThat(initial.total()).isEqualTo(12);
        assertThat(initial.pending()).isEqualTo(12);
        assertThat(initial.succeeded()).isZero();
        assertThat(initial.failed()).isZero();
        var results = IntStream.range(0, 12).mapToObj(i -> {
            String node = io.mohs.test.TestIds.value("batch-node-" + i);
            TestSessions.seed(source, dialect, node, EngineState.RUNNING, Duration.ofMinutes(5));
            var claimed = queue.claim(0, node, 1, List.of(), NOW).getFirst();
            return new LeaseStore.CompletionResult(claimed.executionId(), claimed.jobKey(), node, claimed.claimToken(), 1,
                    NOW, NOW, i % 3 == 0 ? ExecutionState.FAILED : ExecutionState.SUCCEEDED, null, null, null);
        }).toList();
        var barrier = new CyclicBarrier(results.size());
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = results.stream().map(result -> threads.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return leases.complete(List.of(result)).get(result.executionId());
            })).toList();
            int closers = 0;
            for (var future : futures) {
                var completion = future.get(10, TimeUnit.SECONDS);
                assertThat(completion.owned()).isTrue();
                if (completion.closedBatch() != null) closers++;
            }
            assertThat(closers).isOne();
        }
        var complete = batches.find("01991337-0000-7000-8000-00000000000d").orElseThrow();
        assertThat(complete.succeeded()).isEqualTo(8);
        assertThat(complete.failed()).isEqualTo(4);
        assertThat(complete.pending()).isZero();
        assertThat(leases.complete(List.of(results.getFirst())).get(results.getFirst().executionId()))
                .isEqualTo(LeaseStore.Completion.ALREADY_APPLIED);
        assertThat(batches.find("01991337-0000-7000-8000-00000000000d")).contains(complete);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void missingBatchRollsBackTheWholeCompletion(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        f.register("01991337-0000-7000-8000-000000000006");
        var id = f.enqueue("01991337-0000-7000-8000-00000000005f");
        f.jdbc.update("UPDATE mohs_execution SET batch_id = '01991337-0001-7000-8000-000000000091' WHERE execution_id = ?", id.value());
        var grant = f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW).getFirst();
        var result = new LeaseStore.CompletionResult(id, f.job, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1, NOW, NOW,
                ExecutionState.SUCCEEDED, null, null, null);
        assertThatThrownBy(() -> f.leases.complete(List.of(result)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no batch '01991337-0001-7000-8000-000000000091'");
        assertThat(f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006"))).hasSize(1);
        assertThat(f.jdbc.queryForObject("SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, id.value()))
                .isEqualTo("PENDING");
        assertThat(f.jdbc.queryForObject("SELECT last_attempt_number FROM mohs_execution WHERE execution_id = ?", Integer.class, id.value()))
                .isZero();
    }
}
