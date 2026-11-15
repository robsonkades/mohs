package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import static org.assertj.core.api.Assertions.assertThat;

/** Cancellation is arbitrated at commit, even if the worker/reaper computed a retry earlier. */
class JdbcCancellationProtocolTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void cancellationAcceptedAfterTheHandlerFailedPreventsItsBufferedRetry(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var lease = claim(f);
        var buffered = retry(lease);
        assertThat(f.queue.cancel(lease.executionId(), NOW)).isTrue();
        assertThat(f.leases.complete(List.of(buffered)).get(lease.executionId()).owned()).isTrue();
        assertCancelled(f, lease.executionId());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void recoveryRechecksCancellationInsteadOfTrustingItsOldSnapshot(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var snapshot = claim(f);
        f.queue.cancel(snapshot.executionId(), NOW);
        f.nodes.stop("01991337-0000-7000-8000-000000000006");
        assertThat(f.leases.reclaim(List.of(retry(snapshot))).get(snapshot.executionId()).owned()).isTrue();
        assertCancelled(f, snapshot.executionId());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void requeueCannotDiscardAnAcceptedCancellation(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var lease = claim(f);
        f.queue.cancel(lease.executionId(), NOW);
        f.queue.requeue(List.of(new WorkQueue.Requeue(lease.executionId(), lease.nodeId(), lease.claimToken(), 1, NOW)));
        assertCancelled(f, lease.executionId());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void successfulExternalWorkCanStillBeRecordedAfterACancelRequest(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var lease = claim(f);
        f.queue.cancel(lease.executionId(), NOW);
        var success = new LeaseStore.CompletionResult(lease.executionId(), lease.jobKey(), lease.nodeId(),
                lease.claimToken(), 1, NOW, NOW, ExecutionState.SUCCEEDED, null, null, null);
        assertThat(f.leases.complete(List.of(success)).get(lease.executionId()).owned()).isTrue();
        assertThat(f.history.find(lease.executionId(), NOW).orElseThrow().state()).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private static JdbcOwnershipProtocolTest.Fixture fixture(JdbcJobBindingTest.Backend backend) {
        var fixture = new JdbcOwnershipProtocolTest.Fixture(backend);
        fixture.register("01991337-0000-7000-8000-000000000006");
        return fixture;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aBufferedRetryCannotReturnWorkToARetiredJobsQueue(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var lease = claim(f);
        var buffered = retry(lease);
        f.jobs.remove(lease.jobKey());
        var completion = f.leases.complete(List.of(buffered)).get(lease.executionId());
        assertThat(completion.owned()).isTrue();
        assertThat(completion.result().terminalState()).isEqualTo(ExecutionState.FAILED);
        assertThat(f.history.find(lease.executionId(), NOW).orElseThrow().state()).isEqualTo(ExecutionState.FAILED);
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Integer.class)).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void returningAnUnstartedGrantAfterRetirementCancelsIt(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var lease = claim(f);
        f.jobs.remove(lease.jobKey());
        f.queue.requeue(List.of(new WorkQueue.Requeue(lease.executionId(), lease.nodeId(), lease.claimToken(), 1, NOW)));
        assertCancelled(f, lease.executionId());
    }

    private static LeaseStore.Lease claim(JdbcOwnershipProtocolTest.Fixture f) {
        f.enqueue("01991337-0000-7000-8000-000000000011");
        f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW);
        return f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006")).getFirst();
    }

    private static LeaseStore.CompletionResult retry(LeaseStore.Lease lease) {
        return new LeaseStore.CompletionResult(lease.executionId(), lease.jobKey(), lease.nodeId(), lease.claimToken(),
                1, NOW, NOW, ExecutionState.FAILED, "ControlledFailure", "failed before cancel", NOW);
    }

    private static void assertCancelled(JdbcOwnershipProtocolTest.Fixture f, ExecutionId id) {
        assertThat(f.history.find(id, NOW).orElseThrow().state()).isEqualTo(ExecutionState.CANCELLED);
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY' AND  execution_id = ?", Integer.class, id.value())).isZero();
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = ?", Integer.class, id.value())).isZero();
    }
}
