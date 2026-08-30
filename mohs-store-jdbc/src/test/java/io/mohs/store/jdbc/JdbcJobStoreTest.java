/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.store.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.CronSpec;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.Schedule;
import io.mohs.engine.StoredJob;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcJobStoreTest {

    record Handler() {
    }

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcJobStore store;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(Instant.parse("2026-08-13T00:00:00Z"), ZoneId.of("UTC"));
        store = new JdbcJobStore(dataSource, clock);
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:job-store-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private static JobDefinition definition(String id, Schedule schedule) {
        return JobDefinition.of(id, Handler.class, spec -> {
            PolicySpec policySpec = switch (schedule) {
                case CronSpec cron -> spec.cron(cron.expression(), cron.zone());
                case IntervalSpec interval -> interval.afterFinish()
                        ? spec.everyAfterFinish(interval.interval())
                        : spec.every(interval.interval());
                case OnDemandSpec onDemand -> spec.onDemand();
            };
            policySpec.runner("io").retries(3);
        });
    }

    private static JobDefinition definitionWithCap(String id, int max) {
        return JobDefinition.of(id, Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(max));
    }

    /** The public builder always stamps PROGRAMMATIC; only the canonical constructor yields ANNOTATION. */
    private static JobDefinition annotationSourcedDefinition(String id) {
        return new JobDefinition(JobKey.of(id), null, Handler.class, new OnDemandSpec(),
                null, null, Misfire.IGNORE, true, 0, 0, null, null, DefinitionSource.ANNOTATION);
    }

    @Test
    void upsertInsertsANewCronDefinition() {
        JobDefinition definition = definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo")));

        store.upsert(definition);
        Optional<StoredJob> stored = store.find(JobKey.of("welcome-email"));

        assertThat(stored).isPresent();
        assertThat(stored.get().definition()).isEqualTo(definition);
        assertThat(stored.get().orphaned()).isFalse();
        assertThat(stored.get().paused()).isFalse();
    }

    /** startPaused only arms at birth — the job is born paused and its trigger is not swept until the resume. */
    @Test
    void upsertCreatesAStartPausedJobBornPaused() {
        store.upsert(JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused()));

        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isTrue();
        clock.advance(Duration.ofMinutes(5));
        assertThat(store.findDueRecurring(clock.instant(), 10)).isEmpty();
    }

    /** After birth, paused belongs to the operator — a redeploy with startPaused=true never re-pauses a job the operator turned on. */
    @Test
    void redeployNeverReappliesStartPaused() {
        JobDefinition definition = JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused());
        store.upsert(definition);
        store.resume(JobKey.of("dormant"));

        store.upsert(definition);

        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isFalse();
    }

    /** The feature's complete happy path: born paused, then resumed, and the trigger armed at birth (now aged) becomes due — the misfire policy decides from there. */
    @Test
    void resumedDormantJobBecomesDueWithItsBirthTrigger() {
        store.upsert(JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused()));
        clock.advance(Duration.ofMinutes(5));
        assertThat(store.findDueRecurring(clock.instant(), 10)).isEmpty();

        store.resume(JobKey.of("dormant"));

        assertThat(store.findDueRecurring(clock.instant(), 10))
                .extracting(stored -> stored.definition().key().value())
                .containsExactly("dormant");
    }

    /** "The row is the memory": a soft retire preserves the row, so a resurrection through an upsert does NOT re-apply startPaused — paused comes back as the operator left it. */
    @Test
    void resurrectionAfterRetireKeepsTheOperatorPauseState() {
        JobDefinition definition = JobDefinition.of("dormant", Handler.class, spec -> spec.every(Duration.ofMinutes(1)).startPaused());
        store.upsert(definition);
        store.resume(JobKey.of("dormant"));
        store.remove(JobKey.of("dormant"));

        store.upsert(definition);

        assertThat(store.find(JobKey.of("dormant")).orElseThrow().paused()).isFalse();
    }

    @Test
    void roundTripsIntervalSchedule() {
        JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), true));

        store.upsert(definition);

        assertThat(store.find(JobKey.of("poll")).map(StoredJob::definition)).contains(definition);
    }

    @Test
    void roundTripsOnDemandSchedule() {
        JobDefinition definition = definition("import-file", new OnDemandSpec());

        store.upsert(definition);

        assertThat(store.find(JobKey.of("import-file")).map(StoredJob::definition)).contains(definition);
    }

    @Test
    void upsertGeneratesAnIdDistinctPerJobKey() {
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        store.upsert(definition("import-file", new OnDemandSpec()));

        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        String idA = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "welcome-email");
        String idB = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "import-file");

        assertThat(idA).isNotBlank();
        assertThat(idB).isNotBlank();
        assertThat(idA).isNotEqualTo(idB);
    }

    @Test
    void upsertKeepsTheSameIdAcrossReupsertOfTheSameJobKey() {
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        String idBeforeReupsert = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "welcome-email");

        store.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(9)));

        String idAfterReupsert = rawJdbcTemplate.queryForObject("SELECT id FROM mohs_job_definitions WHERE job_key = ?", String.class, "welcome-email");
        assertThat(idAfterReupsert).isEqualTo(idBeforeReupsert);
    }

    @Test
    void upsertOnExistingKeyAppliesDefinitionalChanges() {
        store.upsert(definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("UTC"))));

        JobDefinition changed = JobDefinition.of("welcome-email", Handler.class,
                spec -> spec.cron("0 0 3 * * *", ZoneId.of("UTC")).retries(9));
        store.upsert(changed);

        StoredJob stored = store.find(JobKey.of("welcome-email")).orElseThrow();
        assertThat(stored.definition().retries()).isEqualTo(9);
        assertThat(stored.definition().schedule()).isEqualTo(new CronSpec("0 0 3 * * *", ZoneId.of("UTC")));
    }

    @Test
    void upsertPreservesOperationalStateAcrossReupsert() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        store.pause(key);

        // A redeploy: the code reapplies the same definition
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        assertThat(store.find(key)).map(StoredJob::paused).contains(true);
    }

    @Test
    void findReturnsEmptyForUnknownKey() {
        assertThat(store.find(JobKey.of("ghost"))).isEmpty();
    }

    @Test
    void findAllReturnsEveryStoredJob() {
        store.upsert(definition("a", new OnDemandSpec()));
        store.upsert(definition("b", new OnDemandSpec()));

        try (Stream<StoredJob> all = store.findAll()) {
            assertThat(all).extracting(job -> job.definition().key()).containsExactlyInAnyOrder(JobKey.of("a"), JobKey.of("b"));
        }
    }

    /** The source filter is applied in the WHERE, not in memory — PROGRAMMATIC never leaves the cursor. */
    @Test
    void findAllAnnotationSourcedExcludesProgrammaticJobs() {
        store.upsert(definition("programmatic-job", new OnDemandSpec()));
        store.upsert(annotationSourcedDefinition("annotation-job"));

        try (Stream<StoredJob> annotationSourced = store.findAllAnnotationSourced()) {
            assertThat(annotationSourced).extracting(job -> job.definition().key()).containsExactly(JobKey.of("annotation-job"));
        }
    }

    @Test
    void markOrphanedSetsTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        store.markOrphaned(key);

        assertThat(store.find(key)).map(StoredJob::orphaned).contains(true);
    }

    /** orphaned is inferred by the system, unlike paused, which an operator chose: a re-upsert means the annotation is back, so it clears. */
    @Test
    void upsertClearsOrphanedOnReupsert() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        store.markOrphaned(key);

        store.upsert(definition("welcome-email", new OnDemandSpec()));

        assertThat(store.find(key)).map(StoredJob::orphaned).contains(false);
    }

    @Test
    void pauseAndResumeToggleTheFlag() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));

        store.pause(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(true);

        store.resume(key);
        assertThat(store.find(key)).map(StoredJob::paused).contains(false);
    }

    /** Advisory history (plus a queue entry while still pending) — a retired job normally does have executions. */
    private void seedExecution(String id, String jobKey, String state, boolean queued) {
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        raw.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES (?, ?, ?, ?, ?, 'test', '{}', 'java.lang.String')
                """, id, jobKey, state,
                JdbcTimestamps.toUtcLocalDateTime(clock.instant()), JdbcTimestamps.toUtcLocalDateTime(clock.instant()));
        if (queued) {
            raw.update("""
                    INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
                    VALUES (?, ?, 0, 20, 1, ?)
                    """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(clock.instant()));
        }
    }

    /**
     * A soft retire ({@code Mohs.remove}: "cancels future firings, preserves history"): the definition
     * disappears from the reads but the row remains — history in {@code mohs_execution} keeps pointing at
     * it — and the QUEUE is drained: what was queued becomes {@code CANCELLED} in the advisory state and
     * leaves {@code mohs_ready}; what was terminal stays intact.
     */
    @Test
    void removeRetiresTheJobDrainingTheQueueAndPreservingHistory() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        seedExecution("exec-done", "welcome-email", "SUCCEEDED", false);
        seedExecution("exec-queued", "welcome-email", "PENDING", true);

        store.remove(key);

        assertThat(store.find(key)).isEmpty();
        try (Stream<StoredJob> all = store.findAll()) {
            assertThat(all).isEmpty();
        }
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        assertThat(raw.queryForObject("SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, "exec-done")).isEqualTo("SUCCEEDED");
        assertThat(raw.queryForObject("SELECT state FROM mohs_execution WHERE execution_id = ?", String.class, "exec-queued")).isEqualTo("CANCELLED");
        assertThat(raw.queryForObject("SELECT COUNT(*) FROM mohs_ready", Integer.class)).isZero();
    }

    /** The same rationale as {@link #upsertClearsOrphanedOnReupsert}: the upsert happening at all proves a real source wants the job again. */
    @Test
    void upsertAfterRemoveResurrectsTheDefinitionWithItsHistory() {
        JobKey key = JobKey.of("welcome-email");
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        seedExecution("exec-done", "welcome-email", "SUCCEEDED", false);
        store.remove(key);

        store.upsert(definition("welcome-email", new OnDemandSpec()));

        assertThat(store.find(key)).isPresent();
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        assertThat(raw.queryForObject("SELECT COUNT(*) FROM mohs_execution WHERE job_key = ?", Integer.class, "welcome-email")).isEqualTo(1);
    }

    /** Two nodes seeing 0 rows from the UPDATE and racing for the first-time INSERT. */
    @Test
    void upsertHandlesConcurrentFirstTimeInsertWithoutThrowing() throws Exception {
        JobDefinition definitionToRegister = definition("welcome-email", new OnDemandSpec());
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Callable<JobDefinition> upsert = () -> {
            barrier.await();
            return store.upsert(definitionToRegister);
        };

        Future<JobDefinition> futureA = executor.submit(upsert);
        Future<JobDefinition> futureB = executor.submit(upsert);
        futureA.get(10, TimeUnit.SECONDS);
        futureB.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(store.find(JobKey.of("welcome-email"))).map(StoredJob::definition).contains(definitionToRegister);
    }

    /** The ceiling persists on the definition; the count of occupied slots derives from {@code mohs_lease} — no counter on the job's row. */
    @Test
    void upsertRoundTripsMaxConcurrentExecutions() {
        store.upsert(definitionWithCap("report-summary", 10));

        StoredJob stored = store.find(JobKey.of("report-summary")).orElseThrow();

        assertThat(stored.definition().maxConcurrentExecutions()).isEqualTo(10);
    }

    @Test
    void findSkipsRowsWhoseHandlerTypeNoLongerResolves() {
        // Simulates a handler removed from the code: it inserts the row directly, without going through
        // the store (which demands a real Class<?>).
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('ghost-id', 'ghost-handler', 'com.example.LongGoneHandler', 'ON_DEMAND', 'IGNORE', 0, 'ANNOTATION', TRUE, FALSE, ?, ?)
                """, now, now);
        store.upsert(definition("still-here", new OnDemandSpec()));

        try (Stream<StoredJob> all = store.findAll()) {
            assertThat(all).extracting(job -> job.definition().key()).containsExactly(JobKey.of("still-here"));
        }
    }

    /** A corrupt schedule_type must not silently become ON_DEMAND — the same spirit as the unresolved-handler case: a garbage value has to be loud, not a valid variant by accident. */
    @Test
    void findFailsFastOnUnknownScheduleType() {
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('corrupt-id', 'corrupt-schedule', ?, 'GARBAGE', 'IGNORE', 0, 'ANNOTATION', FALSE, FALSE, ?, ?)
                """, Handler.class.getName(), now, now);

        assertThatThrownBy(() -> store.find(JobKey.of("corrupt-schedule")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("GARBAGE")
                .hasMessageContaining("corrupt-schedule");
    }

    /** A row with an unresolved handler becomes visible as ORPHANED rather than simply vanishing from both sides (find and findAll). */
    @Test
    void findAllMarksRowsWithUnresolvedHandlerAsOrphaned() {
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('ghost-id', 'ghost-handler', 'com.example.LongGoneHandler', 'ON_DEMAND', 'IGNORE', 0, 'ANNOTATION', FALSE, FALSE, ?, ?)
                """, now, now);

        try (Stream<StoredJob> all = store.findAll()) {
            all.forEach(job -> { });
        }

        Boolean orphaned = rawJdbcTemplate.queryForObject(
                "SELECT orphaned FROM mohs_job_definitions WHERE job_key = ?", Boolean.class, "ghost-handler");
        assertThat(orphaned).isTrue();
    }

    @Test
    void findMarksTheRowWithUnresolvedHandlerAsOrphaned() {
        LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
        JdbcTemplate rawJdbcTemplate = new JdbcTemplate(dataSource);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_job_definitions (
                    id, job_key, handler_type, schedule_type, misfire, retries, source, orphaned, paused, created_at, updated_at)
                VALUES ('ghost-id', 'ghost-handler', 'com.example.LongGoneHandler', 'ON_DEMAND', 'IGNORE', 0, 'ANNOTATION', FALSE, FALSE, ?, ?)
                """, now, now);

        assertThat(store.find(JobKey.of("ghost-handler"))).isEmpty();

        Boolean orphaned = rawJdbcTemplate.queryForObject(
                "SELECT orphaned FROM mohs_job_definitions WHERE job_key = ?", Boolean.class, "ghost-handler");
        assertThat(orphaned).isTrue();
    }

    /** The trigger's state: what arms, preserves, recomputes and cures {@code next_fire_at} is the upsert; the due read is {@code findDueRecurring}. */
    @Nested
    class TriggerState {

        private JdbcTemplate rawJdbc() {
            return new JdbcTemplate(dataSource);
        }

        private @Nullable Instant nextFireAtOf(String jobKey) {
            LocalDateTime stored = rawJdbc().queryForObject(
                    "SELECT next_fire_at FROM mohs_job_definitions WHERE job_key = ?", LocalDateTime.class, jobKey);
            return stored == null ? null : JdbcTimestamps.fromUtcLocalDateTime(stored);
        }

        private void disarm(String jobKey) {
            rawJdbc().update("UPDATE mohs_job_definitions SET next_fire_at = NULL WHERE job_key = ?", jobKey);
        }

        @Test
        void upsertArmsTheTriggerOfACronDefinition() {
            store.upsert(definition("welcome-email", new CronSpec("0 0 2 * * *", ZoneId.of("America/Sao_Paulo"))));

            // Base 2026-08-13T00:00Z is 12 August 21:00 in Sao Paulo, so the next 02:00 SP is 05:00Z on 13 August
            assertThat(nextFireAtOf("welcome-email")).isEqualTo(Instant.parse("2026-08-13T05:00:00Z"));
        }

        @Test
        void upsertArmsTheTriggerOfAFixedRateDefinition() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        }

        @Test
        void upsertLeavesOnDemandUnarmed() {
            store.upsert(definition("import-file", new OnDemandSpec()));

            assertThat(nextFireAtOf("import-file")).isNull();
        }

        /** Without this, every redeploy of an every-30-minutes job would push the firing forward forever. */
        @Test
        void upsertPreservesTheTriggerWhenTheScheduleIsUnchanged() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), false));
            store.upsert(definition);
            Instant armedAt = nextFireAtOf("poll");

            clock.advance(Duration.ofMinutes(10));
            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isEqualTo(armedAt);
        }

        @Test
        void upsertRecomputesTheTriggerWhenTheScheduleChanges() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            clock.advance(Duration.ofMinutes(10));
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(1), false)));

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
        }

        /** Curing a new column on an old database: a recurring schedule with a NULL next_fire_at rearms at boot. */
        @Test
        void upsertHealsAnUnarmedRecurringTrigger() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), false));
            store.upsert(definition);
            disarm("poll");

            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        }

        /** afterFinish with a live scheduler occurrence does NOT cure — arming would create the overlap fixed-delay promises not to have. */
        @Test
        void upsertDoesNotHealAnAfterFinishChainWithALiveSchedulerOccurrence() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), true));
            store.upsert(definition);
            disarm("poll");
            // "Live" means the advisory is still PENDING (queued, running, or in backoff)
            seedExecution("occ-1", "poll", "scheduler", "PENDING");

            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isNull();
        }

        @Test
        void upsertHealsAnAfterFinishChainWhoseOccurrencesAllFinished() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), true));
            store.upsert(definition);
            disarm("poll");
            seedExecution("occ-1", "poll", "scheduler", "SUCCEEDED");
            seedExecution("man-1", "poll", "api:user", "PENDING"); // a manual execution is not the chain

            store.upsert(definition);

            assertThat(nextFireAtOf("poll")).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        }

        @Test
        void findDueRecurringReturnsDueJobsOldestFirstWithinTheLimit() {
            store.upsert(definition("due-late", new IntervalSpec(Duration.ofMinutes(1), false)));
            clock.advance(Duration.ofMinutes(2));
            store.upsert(definition("due-recent", new IntervalSpec(Duration.ofMinutes(1), false)));
            store.upsert(definition("not-due", new IntervalSpec(Duration.ofHours(6), false)));
            store.upsert(definition("import-file", new OnDemandSpec()));
            store.upsert(definition("paused", new IntervalSpec(Duration.ofMinutes(1), false)));
            store.pause(JobKey.of("paused"));
            store.upsert(definition("orphaned", new IntervalSpec(Duration.ofMinutes(1), false)));
            store.markOrphaned(JobKey.of("orphaned"));
            clock.advance(Duration.ofMinutes(2));

            assertThat(store.findDueRecurring(clock.instant(), 10))
                    .extracting(stored -> stored.definition().key().value())
                    .containsExactly("due-late", "due-recent");
            assertThat(store.findDueRecurring(clock.instant(), 1))
                    .extracting(stored -> stored.definition().key().value())
                    .containsExactly("due-late");
        }

        @Test
        void findDueRecurringExposesTheStoredNextFireAt() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(1), false)));
            Instant armedAt = clock.instant().plus(Duration.ofMinutes(1));
            clock.advance(Duration.ofMinutes(2));

            assertThat(store.findDueRecurring(clock.instant(), 10))
                    .singleElement()
                    .extracting(StoredJob::nextFireAt)
                    .isEqualTo(armedAt);
        }

        /**
         * Preserving means NOT writing the column. The real race (a firing or rearm CAS between the
         * snapshot and the upsert's UPDATE) is not reproducible in a unit test — what is pinned is the
         * mechanism that eliminates it: with an unchanged schedule, {@code next_fire_at} stays OUT of the
         * statement, so no write exists that could regress a concurrent advance.
         */
        @Test
        void upsertOmitsTheTriggerColumnWhenPreserving() {
            JobDefinition definition = definition("poll", new IntervalSpec(Duration.ofMinutes(5), false));
            store.upsert(definition);
            List<String> statements = new ArrayList<>();
            JdbcJobStore spying = new JdbcJobStore(sqlRecordingDataSource(statements), clock);

            spying.upsert(definition); // agenda inalterada — preserva

            assertThat(statements)
                    .filteredOn(sql -> sql.contains("UPDATE mohs_job_definitions"))
                    .isNotEmpty()
                    .noneMatch(sql -> sql.contains("next_fire_at"));
        }

        /** The counterpoint: a schedule change writes the column — an explicit reconfiguration beats a concurrent firing. */
        @Test
        void upsertWritesTheTriggerColumnWhenTheScheduleChanges() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));
            List<String> statements = new ArrayList<>();
            JdbcJobStore spying = new JdbcJobStore(sqlRecordingDataSource(statements), clock);

            spying.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(1), false)));

            // The template has already replaced the named parameters with ? by the time the SQL reaches prepareStatement
            assertThat(statements)
                    .filteredOn(sql -> sql.contains("UPDATE mohs_job_definitions"))
                    .anyMatch(sql -> sql.contains("next_fire_at = ?"));
        }

        /** A DataSource that records the SQL of every prepareStatement — pass-through otherwise (the same proxy technique as JdbcExecutionStoreTest's). */
        private DataSource sqlRecordingDataSource(List<String> statements) {
            InvocationHandler onDataSource = (_, method, args) -> {
                Object result = method.invoke(dataSource, args);
                if (result instanceof Connection connection) {
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                            (_, connectionMethod, connectionArgs) -> {
                                if (connectionMethod.getName().equals("prepareStatement")
                                        && connectionArgs != null && connectionArgs.length > 0
                                        && connectionArgs[0] instanceof String sql) {
                                    statements.add(sql);
                                }
                                return connectionMethod.invoke(connection, connectionArgs);
                            });
                }
                return result;
            };
            return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class}, onDataSource);
        }

        /** The new schedule and the recomputed trigger land in the SAME UPDATE; policies (retries and so on) stay intact. */
        @Test
        void rescheduleSwapsTheScheduleAndRearmsTheTrigger() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            boolean rescheduled = store.reschedule(JobKey.of("poll"), new IntervalSpec(Duration.ofMinutes(1), false));

            assertThat(rescheduled).isTrue();
            StoredJob stored = store.find(JobKey.of("poll")).orElseThrow();
            assertThat(stored.definition().schedule()).isEqualTo(new IntervalSpec(Duration.ofMinutes(1), false));
            assertThat(stored.definition().retries()).isEqualTo(3); // policy is not schedule — untouched
            assertThat(stored.nextFireAt()).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
        }

        /** Turning recurrence off at runtime is a valid case of the same endpoint. */
        @Test
        void rescheduleToOnDemandDisarmsTheTrigger() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            store.reschedule(JobKey.of("poll"), new OnDemandSpec());

            assertThat(store.find(JobKey.of("poll")).orElseThrow().definition().schedule()).isEqualTo(new OnDemandSpec());
            assertThat(nextFireAtOf("poll")).isNull();
        }

        @Test
        void rescheduleOfARetiredOrUnknownJobReturnsFalse() {
            assertThat(store.reschedule(JobKey.of("ghost"), new OnDemandSpec())).isFalse();

            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));
            store.remove(JobKey.of("poll"));
            assertThat(store.reschedule(JobKey.of("poll"), new OnDemandSpec())).isFalse();
        }

        /** A fixed-delay chain in flight (its trigger disarmed) rescheduled — the new series wins; the completion's late rearm no-ops on the IS NULL guard. */
        @Test
        void rescheduleArmsTheNewSeriesAndTheLateCompletionRearmLoses() {
            store.upsert(definition("chain", new IntervalSpec(Duration.ofMinutes(5), true)));
            disarm("chain"); // An occurrence in flight: next_fire_at = NULL, as the firing leaves it

            store.reschedule(JobKey.of("chain"), new IntervalSpec(Duration.ofMinutes(1), true));
            store.armNextFire(JobKey.of("chain"), clock.instant().plus(Duration.ofMinutes(5))); // the completion arrives late

            assertThat(nextFireAtOf("chain")).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
        }

        /** An unrealisable cron fails BEFORE any write — the old schedule stays intact. */
        @Test
        void rescheduleWithAnImpossibleCronFailsWithoutWriting() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), false)));

            assertThatThrownBy(() -> store.reschedule(JobKey.of("poll"), new CronSpec("0 0 0 30 2 *", ZoneId.of("UTC"))))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThat(store.find(JobKey.of("poll")).orElseThrow().definition().schedule())
                    .isEqualTo(new IntervalSpec(Duration.ofMinutes(5), false));
        }

        @Test
        void armNextFireOnlyArmsAnUnarmedTrigger() {
            store.upsert(definition("poll", new IntervalSpec(Duration.ofMinutes(5), true)));
            Instant armedAt = nextFireAtOf("poll");

            store.armNextFire(JobKey.of("poll"), clock.instant().plus(Duration.ofHours(1)));
            assertThat(nextFireAtOf("poll")).isEqualTo(armedAt); // already armed — the IS NULL guard protects it

            disarm("poll");
            Instant rearmAt = clock.instant().plus(Duration.ofHours(1));
            store.armNextFire(JobKey.of("poll"), rearmAt);
            assertThat(nextFireAtOf("poll")).isEqualTo(rearmAt);
        }

        private void seedExecution(String id, String jobKey, String actor, String state) {
            LocalDateTime now = JdbcTimestamps.toUtcLocalDateTime(clock.instant());
            rawJdbc().update("""
                    INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, priority, payload, payload_type)
                    VALUES (?, ?, ?, ?, ?, ?, 20, '{}', 'java.util.LinkedHashMap')
                    """, id, jobKey, state, now, now, actor);
        }
    }

    /**
     * Retiring the job cancels the pending ones, and cancelling is terminal: without counting, the member
     * disappears from the batch, pending never reaches zero and the batch stays open forever — there is
     * no reconciliation to cure it. Done in bulk per batch, not one call per member.
     */
    @Test
    void removeCountsCancelledBatchMembersIntoTheirBatch() {
        JdbcTemplate raw = new JdbcTemplate(dataSource);
        store.upsert(definition("welcome-email", new OnDemandSpec()));
        new JdbcBatchStore(dataSource, clock).insert("b6", "nightly", 3);
        seedExecution("m1", "welcome-email", "PENDING", true);
        seedExecution("m2", "welcome-email", "PENDING", true);
        seedExecution("m3", "welcome-email", "SUCCEEDED", false);
        raw.update("UPDATE mohs_execution SET correlation_id = ? WHERE execution_id IN (?, ?, ?)", "b6", "m1", "m2", "m3");

        store.remove(JobKey.of("welcome-email"));

        // Only the two in the queue were cancelled now; the SUCCEEDED one was already terminal
        assertThat(raw.queryForObject("SELECT failed FROM mohs_batches WHERE id = ?", Integer.class, "b6"))
                .isEqualTo(2);
    }
}
