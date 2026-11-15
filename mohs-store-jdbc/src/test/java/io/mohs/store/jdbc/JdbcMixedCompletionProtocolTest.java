package io.mohs.store.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import io.mohs.core.EngineState;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.SubmissionStore;
import io.mohs.test.InputFixtures;
import io.mohs.test.TestIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Mixed terminal/retry batches must remain atomic and respect canonical job lock ordering. */
class JdbcMixedCompletionProtocolTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void failedMixedWriteRollsBackAndConcurrentOwnersCompleteSharedJobs(JdbcJobBindingTest.Backend backend) throws Exception {
        var source = backend.dataSource();
        var dialect = backend.delegate();
        var jdbc = new JdbcTemplate(source);
        String batchId = TestIds.value("mixed-completion-batch");
        var submissions = IntStream.range(0, 16).mapToObj(i -> {
            var key = JobKey.of("mixed-job-" + i % 2);
            return new SubmissionStore.NewExecution(ExecutionId.of(TestIds.value("mixed-execution-" + i)),
                    key, 0, 20, NOW, NOW, "test", batchId, null, InputFixtures.encode(key, "input"),
                    SubmissionStore.Origin.MANUAL, null);
        }).toList();
        TestSubmissions.declare(source, dialect, submissions);
        new JdbcSubmissionStore(source, dialect, Duration.ofDays(7)).accept(submissions,
                new SubmissionStore.NewBatch(batchId, "mixed"), () -> {});
        var queue = new JdbcWorkQueue(source, dialect);
        var leases = new JdbcLeaseStore(source, dialect);
        var batches = new JdbcBatchStore(source, dialect);
        List<List<LeaseStore.CompletionResult>> groups = new ArrayList<>();
        for (int owner = 0; owner < 2; owner++) {
            String node = TestIds.value("mixed-node-" + owner);
            TestSessions.seed(source, dialect, node, EngineState.RUNNING, Duration.ofMinutes(5));
            var claimed = queue.claim(0, node, 8, List.of(), NOW).stream()
                    .sorted(Comparator.comparing(work -> work.executionId().value())).toList();
            assertThat(claimed).hasSize(8);
            var results = IntStream.range(0, claimed.size()).mapToObj(i -> {
                var work = claimed.get(i);
                return new LeaseStore.CompletionResult(work.executionId(), work.jobKey(), node, work.claimToken(), 1,
                        NOW, NOW, i % 2 == 0 ? ExecutionState.SUCCEEDED : ExecutionState.FAILED,
                        null, null, i % 2 == 0 ? null : NOW);
            }).toList();
            groups.add(owner == 0 ? results : results.reversed());
        }
        var all = groups.stream().flatMap(List::stream).toList();
        var failing = new JdbcLeaseStore(new TestJdbcFaults(source, "UPDATE mohs_work SET"), dialect);
        assertThatThrownBy(() -> failing.complete(all)).isInstanceOf(DataAccessException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isEqualTo(16);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_execution WHERE last_attempt_number <> 0", Integer.class)).isZero();
        assertThat(batches.find(batchId).orElseThrow().pending()).isEqualTo(16);

        var barrier = new CyclicBarrier(2);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = groups.stream().map(group -> workers.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return leases.complete(group);
            })).toList();
            for (var future : futures) {
                assertThat(future.get(10, TimeUnit.SECONDS).values()).allMatch(LeaseStore.Completion::owned);
            }
        }
        assertThat(batches.find(batchId).orElseThrow().succeeded()).isEqualTo(8);
        assertThat(batches.find(batchId).orElseThrow().pending()).isEqualTo(8);
        assertThat(leases.complete(all).values()).allMatch(LeaseStore.Completion::alreadyApplied);
        String finalNode = TestIds.value("mixed-node-final");
        TestSessions.seed(source, dialect, finalNode, EngineState.RUNNING, Duration.ofMinutes(5));
        var retries = queue.claim(0, finalNode, 16, List.of(), NOW);
        assertThat(retries).hasSize(8).allMatch(work -> work.attemptNumber() == 2);
        var completed = leases.complete(retries.stream().map(work -> new LeaseStore.CompletionResult(
                work.executionId(), work.jobKey(), finalNode, work.claimToken(), 2,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, null)).toList());
        assertThat(completed.values()).allMatch(LeaseStore.Completion::owned);
        assertThat(completed.values().stream().filter(result -> result.closedBatch() != null)).hasSize(1);
        assertThat(batches.find(batchId).orElseThrow().succeeded()).isEqualTo(16);
        assertThat(batches.find(batchId).orElseThrow().pending()).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work", Integer.class)).isZero();
    }
}
