package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.engine.SubmissionStore;
import io.mohs.test.InputFixtures;
import io.mohs.test.TestIds;
import static org.assertj.core.api.Assertions.assertThat;

class JdbcPolicyReadProtocolTest {
    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void pagesDistinctVisibleJobsAndRestrictsClaimsWithoutChangingCase(JdbcJobBindingTest.Backend backend) {
        var source = backend.dataSource();
        var dialect = backend.delegate();
        var requests = new ArrayList<SubmissionStore.NewExecution>();
        for (String key : List.of("policy-A", "policy-a", "policy-z")) {
            for (int i = 0; i < 2; i++) {
                var job = JobKey.of(key);
                requests.add(new SubmissionStore.NewExecution(ExecutionId.of(TestIds.value(key + i)), job, 0, 20,
                        NOW, NOW, "test", null, null, InputFixtures.encode(job, "input"), SubmissionStore.Origin.MANUAL, null));
            }
        }
        var future = JobKey.of("future");
        requests.add(new SubmissionStore.NewExecution(ExecutionId.of(TestIds.value("future-page")), future, 0, 10,
                NOW.plusSeconds(60), NOW, "test", null, null, InputFixtures.encode(future, "input"), SubmissionStore.Origin.MANUAL, null));
        TestSubmissions.accept(source, dialect, requests);
        var queue = new JdbcWorkQueue(source, dialect);
        var first = queue.findCandidateJobs(0, null, 2, NOW);
        assertThat(first).containsExactly(JobKey.of("policy-A"), JobKey.of("policy-a"));
        var second = queue.findCandidateJobs(0, first.getLast(), 2, NOW);
        assertThat(second).containsExactly(JobKey.of("policy-z"));
        assertThat(queue.findCandidateJobs(0, second.getLast(), 2, NOW)).isEmpty();
        var jobs = new JdbcJobStore(source, Clock.fixed(NOW, ZoneOffset.UTC), dialect);
        assertThat(jobs.findByKeys(List.of(JobKey.of("policy-A"), JobKey.of("POLICY-A"))))
                .extracting(job -> job.definition().key()).containsExactly(JobKey.of("policy-A"));
        String node = TestIds.value("policy-page-node");
        TestSessions.seed(source, dialect, node, EngineState.RUNNING, Duration.ofMinutes(5));
        assertThat(queue.claim(0, node, 2, second, NOW)).hasSize(2)
                .allSatisfy(work -> assertThat(work.jobKey()).isEqualTo(JobKey.of("policy-z")));
        var leases = new JdbcLeaseStore(source, dialect);
        var firstGrant = leases.findByNodePage(node, null, 1).getFirst();
        var grantCursor = new io.mohs.engine.LeaseStore.LeaseCursor(firstGrant.claimedAt(), firstGrant.executionId());
        var secondGrant = leases.findByNodePage(node, grantCursor, 1).getFirst();
        assertThat(secondGrant.executionId()).isNotEqualTo(firstGrant.executionId());
        assertThat(leases.findByNodePage(node, new io.mohs.engine.LeaseStore.LeaseCursor(
                secondGrant.claimedAt(), secondGrant.executionId()), 1)).isEmpty();
        assertThat(queue.findCandidateJobs(0, first.getLast(), 2, NOW)).isEmpty();
        jobs.remove(JobKey.of("policy-A"));
        assertThat(jobs.findByKeys(first)).extracting(job -> job.definition().key()).containsExactly(JobKey.of("policy-a"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void triggerHorizonExcludesPausedRetiredAndAlreadyDueSeries(JdbcJobBindingTest.Backend backend) {
        var jobs = new JdbcJobStore(backend.dataSource(), Clock.fixed(NOW, ZoneOffset.UTC), backend.delegate());
        for (var entry : java.util.Map.of("paused", 30, "retired", 45, "past", 10, "healthy", 90).entrySet()) {
            jobs.upsert(JobDefinition.of(entry.getKey(), new IntervalSpec(Duration.ofSeconds(entry.getValue()), false),
                    JobOptions.defaults(), Misfire.IGNORE, false));
        }
        jobs.pause(JobKey.of("paused"));
        jobs.remove(JobKey.of("retired"));
        assertThat(jobs.nextFireAfter(NOW.plusSeconds(60))).contains(NOW.plusSeconds(90));
        jobs.pause(JobKey.of("healthy"));
        assertThat(jobs.nextFireAfter(NOW.plusSeconds(60))).isEmpty();
    }
}
