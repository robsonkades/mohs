package io.mohs.store.jdbc;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcRetryMetadataTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void admissionDeferralPreservesAcceptedRoutingAndAttempt(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        f.register("01991337-0000-7000-8000-000000000006");
        var id = f.enqueue("01991337-0000-7000-8000-00000000001a");
        var grant = f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW).getFirst();
        f.queue.requeue(List.of(new WorkQueue.Requeue(id, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1, NOW.plusSeconds(1))));
        assertReady(f, id, 1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void retryPreservesAcceptedIdentityAndAdvancesExactlyOneAttempt(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        f.register("01991337-0000-7000-8000-000000000006");
        var id = f.enqueue("01991337-0000-7000-8000-000000000081");
        var grant = f.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW).getFirst();
        f.leases.complete(List.of(new LeaseStore.CompletionResult(id, f.job, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1,
                NOW, NOW, ExecutionState.FAILED, "test", "failed", NOW.plusSeconds(1))));
        assertReady(f, id, 2);
    }

    @Test
    void onlyAFailedAttemptCanHaveARetry() {
        var id = ExecutionId.of("01991337-0000-7000-8000-000000000056");
        var job = JobKey.of("job");
        assertThatThrownBy(() -> new LeaseStore.CompletionResult(id, job, "01991337-0000-7000-8000-000000000006", "01991337-0000-7000-8000-000000000088", 1,
                NOW, NOW, ExecutionState.SUCCEEDED, null, null, NOW.plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertReady(JdbcOwnershipProtocolTest.Fixture f, ExecutionId id, int attempt) {
        var rows = f.jdbc.query("SELECT execution_id, job_key, shard, priority, attempt FROM mohs_work WHERE work_state = 'READY'", (rs, _) ->
                List.of(rs.getString(1), rs.getString(2), rs.getInt(3), rs.getInt(4), rs.getInt(5)));
        assertThat(rows).containsExactly(List.of(id.value(), f.job.value(), 0, 20, attempt));
    }
}
