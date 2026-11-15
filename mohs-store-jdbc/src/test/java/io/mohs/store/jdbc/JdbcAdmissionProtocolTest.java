package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.JobDefinitions;
import static org.assertj.core.api.Assertions.assertThat;

/** Admission is an atomic database reservation, including across nodes and queue shards. */
class JdbcAdmissionProtocolTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void twoNodesCannotReserveTheSameJobsLastSlot(JdbcJobBindingTest.Backend backend) throws Exception {
        var f = fixture(backend);
        f.enqueue("01991337-0000-7000-8000-000000000012");
        f.enqueue("01991337-0000-7000-8000-000000000014");
        var barrier = new CyclicBarrier(2);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var a = pool.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW);
            });
            var b = pool.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return f.queue.claim(0, "01991337-0002-7000-8000-00000000009b", 1, List.of(), NOW);
            });
            assertThat(a.get(10, TimeUnit.SECONDS).size() + b.get(10, TimeUnit.SECONDS).size()).isEqualTo(1);
        }
        assertThat(f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006", "01991337-0002-7000-8000-00000000009b"))).hasSize(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void oneBatchCannotExceedTheCapAndTerminalCompletionReleasesIt(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        f.enqueue("01991337-0000-7000-8000-000000000012");
        f.enqueue("01991337-0000-7000-8000-000000000014");
        assertThat(f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 10, List.of(), NOW)).hasSize(1);
        var grant = f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006")).getFirst();
        assertThat(f.queue.claim(0, "01991337-0002-7000-8000-00000000009b", 10, List.of(), NOW)).isEmpty();
        var result = new LeaseStore.CompletionResult(grant.executionId(), f.job, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
        assertThat(f.leases.complete(List.of(result)).get(grant.executionId()).owned()).isTrue();
        assertThat(f.queue.claim(0, "01991337-0002-7000-8000-00000000009b", 10, List.of(), NOW)).hasSize(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void pausingTheTriggerDoesNotBlockAlreadyAcceptedWork(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        f.enqueue("01991337-0000-7000-8000-000000000012");
        f.jobs.pause(f.job);
        assertThat(f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 10, List.of(), NOW)).hasSize(1);
    }

    private static JdbcOwnershipProtocolTest.Fixture fixture(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        f.jobs.upsert(JobDefinitions.definition(f.job, JobOptions.defaults().withMaxConcurrentExecutions(1),
                new OnDemandSpec(), Misfire.IGNORE, false));
        f.jobs.upsert(JobDefinitions.definition(io.mohs.core.job.JobKey.of("another-job"), JobOptions.defaults().withMaxConcurrentExecutions(1),
                new OnDemandSpec(), Misfire.IGNORE, false));
        f.register("01991337-0000-7000-8000-000000000006");
        f.register("01991337-0002-7000-8000-00000000009b");
        return f;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aFullJobDoesNotBlockAnotherJobAndItsCapSpansShards(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        f.enqueue("01991337-0000-7000-8000-000000000013");
        assertThat(f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW)).hasSize(1);
        f.enqueue("01991337-0000-7000-8000-000000000015");
        f.jdbc.update("UPDATE mohs_work SET shard = 1, priority = 10 WHERE execution_id = ?", "01991337-0000-7000-8000-000000000015");
        var other = io.mohs.core.job.JobKey.of("another-job");
        var id = io.mohs.core.execution.ExecutionId.of("01991337-0000-7000-8000-00000000007c");
        f.firing.accept(List.of(new io.mohs.engine.SubmissionStore.NewExecution(id, other, 1, 20, NOW, NOW,
                "test", null, null, io.mohs.test.InputFixtures.encode(other, "input"), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)), null, () -> {});

        assertThat(f.queue.claim(1, "01991337-0002-7000-8000-00000000009b", 10, List.of(), NOW)).extracting(io.mohs.engine.WorkQueue.ClaimedWork::executionId)
                .containsExactly(id);
        assertThat(f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006", "01991337-0002-7000-8000-00000000009b"))).hasSize(2);
    }
}
