package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.EngineState;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.JobDefinitions;
import io.mohs.engine.WorkQueue;

import static org.assertj.core.api.Assertions.assertThat;

/** Controlled store interleavings: a fresh owner must survive an obsolete failure observation. */
class JdbcOwnershipProtocolTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");

    static Stream<JdbcJobBindingTest.Backend> backends() {
        return JdbcJobBindingTest.backends();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void obsoleteAliveSnapshotCannotReclaimFreshOwnership(JdbcJobBindingTest.Backend backend) {
        var fixture = new Fixture(backend);
        fixture.register("01991337-0000-7000-8000-000000000006");
        fixture.expire("01991337-0000-7000-8000-000000000006");
        var obsoleteObservation = fixture.nodes.findAll();
        fixture.register("01991337-0000-7000-8000-000000000007");
        var id = fixture.enqueue("01991337-0000-7000-8000-00000000004f");
        fixture.queue.claim(0, "01991337-0000-7000-8000-000000000007", 1, List.of(), NOW);
        obsoleteObservation.forEach(node -> fixture.nodes.revokeExpired(node.nodeId()));
        assertThat(fixture.leases.findOrphaned(100)).isEmpty();
        var fresh = fixture.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000007")).getFirst();
        assertThat(fixture.leases.reclaim(List.of(failure(fresh))).get(id).owned())
                .as("recovery cannot use a fresh grant without revoking its session").isFalse();
        assertThat(fixture.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000007"))).extracting(LeaseStore.Lease::executionId).contains(id);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void oldOwnerCannotCompleteAfterItsIncarnationChanges(JdbcJobBindingTest.Backend backend) {
        var fixture = new Fixture(backend);
        fixture.register("01991337-0000-7000-8000-000000000006");
        var id = fixture.enqueue("01991337-0000-7000-8000-000000000078");
        fixture.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW);
        var oldLease = fixture.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006")).getFirst();
        fixture.expire("01991337-0000-7000-8000-000000000006");
        assertThat(fixture.nodes.renew("01991337-0000-7000-8000-000000000006", EngineState.RUNNING, Duration.ofMinutes(1))).isEmpty();
        assertThat(fixture.nodes.revokeExpired("01991337-0000-7000-8000-000000000006")).isTrue();
        fixture.register("01991337-0000-7000-8000-000000000007");
        assertThat(fixture.leases.complete(List.of(failure(oldLease))).get(id).owned()).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void reacquisitionUsesANewTokenWithoutConsumingARetry(JdbcJobBindingTest.Backend backend) {
        var fixture = new Fixture(backend);
        fixture.register("01991337-0000-7000-8000-000000000006");
        var id = fixture.enqueue("01991337-0000-7000-8000-000000000082");
        fixture.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW);
        var first = fixture.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006")).getFirst();
        assertThat(fixture.queue.requeue(List.of(new WorkQueue.Requeue(id, "01991337-0000-7000-8000-000000000006", first.claimToken(), 1, NOW)))).isEqualTo(1);
        var second = fixture.queue.claim(0, "01991337-0000-7000-8000-000000000006", 1, List.of(), NOW).getFirst();
        assertThat(second.attemptNumber()).isEqualTo(first.attemptNumber());
        assertThat(second.claimToken()).isNotEqualTo(first.claimToken());
        assertThat(fixture.leases.complete(List.of(failure(first))).get(id).owned()).isFalse();
        var current = fixture.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006")).getFirst();
        assertThat(fixture.leases.complete(List.of(failure(current))).get(id).owned()).isTrue();
        assertThat(fixture.leases.complete(List.of(failure(current))).get(id).alreadyApplied()).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void queuedCancellationCommitsTheFixedDelayRearm(JdbcJobBindingTest.Backend backend) {
        var fixture = new Fixture(backend);
        var recurring = JobKey.of("cancel-chain");
        fixture.jobs.upsert(JobDefinitions.definition(recurring, JobOptions.defaults(),
                new IntervalSpec(Duration.ofSeconds(1), true), Misfire.IGNORE, false));
        var observed = fixture.jobs.find(recurring).orElseThrow().nextFireAt();
        var id = ExecutionId.of("01991337-0000-7000-8000-000000000010");
        var occurrence = Execution.enqueued(id, recurring, observed, Execution.SCHEDULER_ACTOR, Priority.NORMAL);
        assertThat(fixture.firing.fire(recurring, 1L, observed, null, List.of(occurrence), observed)).isTrue();
        assertThat(fixture.queue.cancel(id, observed)).isTrue();
        assertThat(fixture.history.find(id, observed).orElseThrow().state()).isEqualTo(ExecutionState.CANCELLED);
        assertThat(fixture.jobs.find(recurring).orElseThrow().nextFireAt()).isEqualTo(observed.plusSeconds(1));
    }

    private static LeaseStore.CompletionResult failure(LeaseStore.Lease lease) {
        return new LeaseStore.CompletionResult(lease.executionId(), lease.jobKey(), lease.nodeId(), lease.claimToken(),
                lease.attemptNumber(), NOW, NOW, ExecutionState.FAILED, "test.Failure", "controlled failure", null);
    }

    static final class Fixture {
        final JobKey job = JobKey.of("protocol-job");
        final JdbcNodeStore nodes;
        final JdbcJobStore jobs;
        final JdbcWorkQueue queue;
        final JdbcLeaseStore leases;
        final JdbcHistoryStore history;
        final JdbcSubmissionStore firing;
        final org.springframework.jdbc.core.JdbcTemplate jdbc;

        Fixture(JdbcJobBindingTest.Backend backend) {
            var clock = Clock.fixed(NOW, ZoneOffset.UTC);
            jdbc = new org.springframework.jdbc.core.JdbcTemplate(backend.dataSource());
            var batches = new JdbcBatchStore(backend.dataSource(), backend.delegate());
            nodes = new JdbcNodeStore(backend.dataSource(), backend.delegate());
            jobs = new JdbcJobStore(backend.dataSource(), clock, backend.delegate());
            queue = new JdbcWorkQueue(backend.dataSource(), backend.delegate());
            leases = new JdbcLeaseStore(backend.dataSource(), backend.delegate());
            history = new JdbcHistoryStore(backend.dataSource(), backend.delegate());
            firing = new JdbcSubmissionStore(backend.dataSource(), backend.delegate(), java.time.Duration.ofDays(7));
            jobs.upsert(JobDefinitions.definition(job, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false));
        }

        void register(String id) {
            nodes.register(id, EngineState.RUNNING, Duration.ofMinutes(5), io.mohs.store.jdbc.TestSessions.catalog(jdbc.getDataSource()));
        }

        void expire(String id) {
            var session = nodes.findAll().stream().filter(node -> node.nodeId().equals(id)).findFirst().orElseThrow();
            jdbc.update("UPDATE mohs_nodes SET expires_at = ? WHERE node_id = ?",
                    JdbcTimestamps.toUtcLocalDateTime(session.lastHeartbeatAt().minusSeconds(1)), id);
        }

        ExecutionId enqueue(String value) {
            var id = ExecutionId.of(value);
            firing.accept(List.of(new SubmissionStore.NewExecution(id, job, 0, 20, NOW, NOW, "test", null, null, io.mohs.test.InputFixtures.encode(job, new LinkedHashMap<>()), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)), null, () -> {});

            return id;
        }
    }
}
