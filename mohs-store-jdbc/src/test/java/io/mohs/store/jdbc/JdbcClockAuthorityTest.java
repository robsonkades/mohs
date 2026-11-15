package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.EngineState;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.SubmissionStore;
import io.mohs.test.InputFixtures;
import io.mohs.test.TestIds;
import static org.assertj.core.api.Assertions.assertThat;

/** A caller's forward wall clock cannot make durable work or refill eligible early. */
class JdbcClockAuthorityTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void oldEligibilityHorizonDoesNotBackdateTheGrant(JdbcJobBindingTest.Backend backend) {
        var source = backend.dataSource();
        var dialect = backend.delegate();
        var database = new NamedParameterJdbcTemplate(source);
        Instant before = JdbcDatabaseTime.now(database, dialect);
        Instant oldHorizon = before.minusSeconds(600);
        var key = JobKey.of("past-clock-work");
        var id = ExecutionId.of(TestIds.value("past-clock-execution"));
        var execution = new SubmissionStore.NewExecution(id, key, 0, 20, oldHorizon, oldHorizon,
                "test", null, null, InputFixtures.encode(key, "input"), SubmissionStore.Origin.MANUAL, null);
        TestSubmissions.accept(source, dialect, List.of(execution));
        String node = TestIds.value("past-clock-node");
        TestSessions.seed(source, dialect, node, EngineState.RUNNING, Duration.ofMinutes(5));
        assertThat(new JdbcWorkQueue(source, dialect).claim(0, node, 1, List.of(), oldHorizon)).hasSize(1);
        var grant = new JdbcLeaseStore(source, dialect).findByNodes(List.of(node)).getFirst();
        assertThat(grant.claimedAt()).isBetween(before, JdbcDatabaseTime.now(database, dialect));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void forwardTriggerObservationCannotPlanOrAdvanceFutureWork(JdbcJobBindingTest.Backend backend) {
        var source = backend.dataSource();
        var dialect = backend.delegate();
        var database = new NamedParameterJdbcTemplate(source);
        Instant before = JdbcDatabaseTime.now(database, dialect);
        var jobs = new JdbcJobStore(source, Clock.fixed(before, ZoneOffset.UTC), dialect);
        var key = JobKey.of("future-clock-trigger");
        jobs.upsert(io.mohs.core.definition.JobDefinition.of(key.value(),
                new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(10), false),
                io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        var stored = jobs.find(key).orElseThrow();
        Instant forward = before.plus(Duration.ofDays(1));
        var page = jobs.findDueRecurring(forward, 100);
        assertThat(page.jobs()).isEmpty();
        assertThat(page.observedAt()).isBetween(before, JdbcDatabaseTime.now(database, dialect));
        var firing = new JdbcSubmissionStore(source, dialect, Duration.ZERO);
        assertThat(firing.fire(key, stored.scheduleRevision(), stored.nextFireAt(), forward.plusSeconds(60),
                List.of(), forward)).as("a forward misfire decision cannot skip future occurrences").isFalse();
        assertThat(jobs.find(key).orElseThrow().nextFireAt()).isEqualTo(stored.nextFireAt());
        assertThat(new JdbcTemplate(source).queryForObject("SELECT COUNT(*) FROM mohs_execution", Long.class)).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void forwardClaimHorizonDoesNotAcquireFutureWork(JdbcJobBindingTest.Backend backend) {
        var source = backend.dataSource();
        var dialect = backend.delegate();
        Instant now = JdbcDatabaseTime.now(new NamedParameterJdbcTemplate(source), dialect);
        var key = JobKey.of("future-clock-work");
        var id = ExecutionId.of(TestIds.value("future-clock-execution"));
        var execution = new SubmissionStore.NewExecution(id, key, 0, 20, now.plusSeconds(600), now,
                "test", null, null, InputFixtures.encode(key, "input"), SubmissionStore.Origin.MANUAL, null);
        TestSubmissions.accept(source, dialect, List.of(execution));
        String node = TestIds.value("future-clock-node");
        TestSessions.seed(source, dialect, node, EngineState.RUNNING, Duration.ofMinutes(5));
        assertThat(new JdbcWorkQueue(source, dialect).claim(0, node, 1, List.of(), now.plus(Duration.ofDays(1))))
                .as("session and eligibility must both respect database time").isEmpty();
        assertThat(new JdbcLeaseStore(source, dialect).findByNodes(List.of(node))).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void bucketCreationAndRefillUseDatabaseTimeWithoutACallerClock(JdbcJobBindingTest.Backend backend) {
        var source = backend.dataSource();
        var dialect = backend.delegate();
        var database = new NamedParameterJdbcTemplate(source);
        Instant before = JdbcDatabaseTime.now(database, dialect);
        var store = new JdbcRateLimitStore(source, dialect);
        store.upsert(new RateLimit("clock-bucket", 1, Duration.ofHours(1)));
        Instant refilledAt = JdbcTimestamps.fromUtcLocalDateTime(new JdbcTemplate(source)
                .queryForObject("SELECT refilled_at FROM mohs_rate_limits WHERE name = 'clock-bucket'", java.time.LocalDateTime.class));
        Instant after = JdbcDatabaseTime.now(database, dialect);
        assertThat(refilledAt).isBetween(before, after);
        assertThat(store.charge("clock-bucket", 1)).isTrue();
        assertThat(store.charge("clock-bucket", 1))
                .as("the API no longer accepts a caller time that could create a second permit").isFalse();
    }
}
