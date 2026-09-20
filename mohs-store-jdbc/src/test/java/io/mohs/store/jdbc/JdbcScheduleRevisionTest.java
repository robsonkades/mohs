package io.mohs.store.jdbc;

import io.mohs.engine.SubmissionStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.JobDefinitions;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;
import static org.assertj.core.api.Assertions.assertThat;

/** Schedule policy observed by an engine is not permission to fire or rearm after reconfiguration. */
class JdbcScheduleRevisionTest {
    private static final Instant NOW = Instant.parse("2026-09-09T12:00:00Z");
    private static final JobKey SERIES = JobKey.of("revision-series");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void pausingAfterPlanningPreventsAnUncommittedFiring(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        Instant observed = f.jobs.find(SERIES).orElseThrow().nextFireAt();
        f.jobs.pause(SERIES);
        assertThat(fire(f, observed, "01991337-0000-7000-8000-00000000007d")).isFalse();
        assertThat(f.jobs.find(SERIES).orElseThrow().nextFireAt()).isEqualTo(observed);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void replacingASeriesWithTheSameTimestampStillInvalidatesTheOldPlan(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var observed = f.jobs.find(SERIES).orElseThrow();
        f.jobs.reschedule(SERIES, observed.definition().schedule());
        var replacement = f.jobs.find(SERIES).orElseThrow();
        assertThat(replacement.nextFireAt()).isEqualTo(observed.nextFireAt());
        assertThat(replacement.scheduleRevision()).isEqualTo(observed.scheduleRevision() + 1);
        assertThat(f.firing.fire(SERIES, observed.scheduleRevision(), observed.nextFireAt(), null,
                List.of(), NOW)).isFalse();
        assertThat(f.jobs.find(SERIES).orElseThrow().nextFireAt()).isEqualTo(observed.nextFireAt());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void identicalRegistrationPreservesTheRevisionAndRetirementStartsANewOne(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var initial = f.jobs.find(SERIES).orElseThrow();
        f.jobs.upsert(initial.definition());
        assertThat(f.jobs.find(SERIES).orElseThrow().scheduleRevision()).isEqualTo(initial.scheduleRevision());
        f.jobs.remove(SERIES);
        f.jobs.reactivate(SERIES, TestSessions.catalog(backend.dataSource()));
        assertThat(f.jobs.find(SERIES).orElseThrow().scheduleRevision()).isGreaterThan(initial.scheduleRevision());
        assertThat(f.firing.fire(SERIES, initial.scheduleRevision(), initial.nextFireAt(), null,
                List.of(), NOW)).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void anOldCompletionCannotRearmAReplacementSeries(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        f.register("01991337-0000-7000-8000-000000000006");
        Instant observed = f.jobs.find(SERIES).orElseThrow().nextFireAt();
        assertThat(fire(f, observed, "01991337-0000-7000-8000-000000000079")).isTrue();
        var oldId = ExecutionId.of("01991337-0000-7000-8000-000000000079");
        f.queue.claim(Shards.of(oldId), "01991337-0000-7000-8000-000000000006", 1, List.of(), observed);
        var old = f.leases.findByNodes(List.of("01991337-0000-7000-8000-000000000006")).getFirst();
        f.jobs.reschedule(SERIES, new IntervalSpec(Duration.ofSeconds(2), true));
        Instant replacement = f.jobs.find(SERIES).orElseThrow().nextFireAt();
        assertThat(fire(f, replacement, "01991337-0000-7000-8000-00000000007f")).isTrue();
        assertThat(f.jobs.find(SERIES).orElseThrow().nextFireAt()).isNull();
        var result = new LeaseStore.CompletionResult(old.executionId(), SERIES, "01991337-0000-7000-8000-000000000006", old.claimToken(), 1,
                observed, observed.plusSeconds(1), ExecutionState.SUCCEEDED, null, null, null);
        assertThat(f.leases.complete(List.of(result)).get(old.executionId()).owned()).isTrue();
        assertThat(f.jobs.find(SERIES).orElseThrow().nextFireAt()).as("the new occurrence still owns its series").isNull();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void anAuditActorCannotMakeAManualExecutionPartOfTheSeries(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        var observed = f.jobs.find(SERIES).orElseThrow().nextFireAt();
        assertThat(fire(f, observed, "01991337-0000-7000-8000-00000000007e")).isTrue();
        var manual = ExecutionId.of("01991337-0000-7000-8000-00000000005d");
        f.firing.accept(List.of(new SubmissionStore.NewExecution(manual, SERIES, Shards.of(manual), 20, NOW, NOW,
                Execution.SCHEDULER_ACTOR, null, null, io.mohs.test.InputFixtures.encode(SERIES, "manual input"), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)), null, () -> {});

        assertThat(f.queue.cancel(manual, NOW)).isTrue();
        assertThat(f.jobs.find(SERIES).orElseThrow().nextFireAt()).isNull();
    }

    private static JdbcOwnershipProtocolTest.Fixture fixture(JdbcJobBindingTest.Backend backend) {
        var f = new JdbcOwnershipProtocolTest.Fixture(backend);
        f.jobs.upsert(JobDefinitions.definition(SERIES, JobOptions.defaults(),
                new IntervalSpec(Duration.ofSeconds(1), true), Misfire.IGNORE, false));
        return f;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void aCompletedScheduledOccurrenceCannotBeReopenedIntoTheNextCycle(JdbcJobBindingTest.Backend backend) {
        var f = fixture(backend);
        f.register("01991337-0000-7000-8000-000000000006");
        Instant observed = f.jobs.find(SERIES).orElseThrow().nextFireAt();
        assertThat(fire(f, observed, "01991337-0000-7000-8000-000000000016")).isTrue();
        var id = ExecutionId.of("01991337-0000-7000-8000-000000000016");
        var grant = f.queue.claim(Shards.of(id), "01991337-0000-7000-8000-000000000006", 1, List.of(), observed).getFirst();
        var result = new LeaseStore.CompletionResult(id, SERIES, "01991337-0000-7000-8000-000000000006", grant.claimToken(), 1,
                observed, observed, ExecutionState.FAILED, "failure", "failed", null);
        assertThat(f.leases.complete(List.of(result)).get(id).owned()).isTrue();
        var next = f.jobs.find(SERIES).orElseThrow().nextFireAt();
        assertThat(fire(f, next, "01991337-0000-7000-8000-000000000062")).isTrue();
        // Reopening the old occurrence would let its second terminal result rearm over this cycle.
        assertThat(f.queue.rearmForManualRetry(id, next)).isFalse();
        assertThat(f.jobs.find(SERIES).orElseThrow().nextFireAt()).isNull();
    }

    private static boolean fire(JdbcOwnershipProtocolTest.Fixture f, Instant observed, String id) {
        return f.firing.fire(SERIES, f.jobs.find(SERIES).orElseThrow().scheduleRevision(), observed, null, List.of(Execution.enqueued(ExecutionId.of(id), SERIES,
                observed, Execution.SCHEDULER_ACTOR, Priority.NORMAL)), observed);
    }
}
