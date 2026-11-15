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

import io.mohs.engine.SubmissionStore;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.IntStream;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link JdbcWorkQueue} over H2 — the claim's portable form (a locking SELECT, then DELETE, then the
 * ownership INSERT). Postgres's single statement has these scenarios mirrored in
 * {@code JdbcWorkQueuePostgresTest}.
 */
class JdbcWorkQueueTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");
    private static final String NODE = "01991337-0000-7000-8000-000000000065";
    private static final long EPOCH = 1;

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new H2JdbcDelegate(), "01991337-0000-7000-8000-000000000065", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new H2JdbcDelegate(), "01991337-0000-7000-8000-000000000067", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        queue = new JdbcWorkQueue(dataSource, new H2JdbcDelegate());
    }

    /** A batch member does not rearm — the batch already counted this failure; re-running would count the outcome TWICE in a batch that may already be closed (the same guard as the earlier era's CAS). */
    @Test
    void rearmForManualRetryRefusesABatchMember() {
        new JdbcJobStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC), new H2JdbcDelegate())
                .upsert(JobDefinition.of("job-a", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, batch_id, payload, input_contract)
                VALUES ('01991337-0000-7000-8000-000000000039', 'job-a', 'FAILED', ?, ?, 'test', 'batch-1', '{}', 'java.lang.Object')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));

        assertThat(queue.rearmForManualRetry(ExecutionId.of("01991337-0000-7000-8000-000000000039"), NOW)).isFalse();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT state FROM mohs_execution WHERE execution_id = '01991337-0000-7000-8000-000000000039'", String.class)).isEqualTo("FAILED");
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Integer.class)).isZero();
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:work-queue-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
        new JdbcJobStore(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), Clock.fixed(NOW, ZoneOffset.UTC),
                new H2JdbcDelegate()).upsert(io.mohs.core.definition.JobDefinition.of(jobKey, new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        TestWork.history(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(new io.mohs.engine.SubmissionStore.NewExecution(ExecutionId.of(id), JobKey.of(jobKey),
                        0, priority, visibleAt, NOW, "test", null, null, io.mohs.test.InputFixtures.encode(JobKey.of(jobKey), "payload"), io.mohs.engine.SubmissionStore.Origin.MANUAL, null)));
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), 0, priority, attempt, visibleAt);
    }

    /**
     * The idle gate's probe ({@code hasVisibleWork}) crosses the driver with the node's LIST of shards —
     * 64 parameters on a single node. Collection binding belongs to the driver, not the delegate, so each
     * one pays for its own test; the rest of the scenario proves the predicate: another node's shard does
     * not count, and an entry that is still invisible does not count.
     */
    @Test
    void hasVisibleWorkSeesOnlyVisibleEntriesInTheOwnedShards() {
        List<Integer> owned = IntStream.range(0, Shards.SHARD_COUNT).filter(shard -> shard % 2 == 0).boxed().toList();
        assertThat(queue.hasVisibleWork(owned, NOW)).isFalse();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000023", 7, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("another node's shard").isFalse();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000031", 8, NOW.plusSeconds(60))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("not visible yet").isFalse();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000029", 8, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).isTrue();
    }

    private WorkQueue.ReadyEntry shardedEntry(String id, int shard, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("job-a"), shard, 20, 1, visibleAt);
    }

    /**
     * The gauge's number, and the two ways it differs from the probe above: no shard predicate — a
     * backlog belongs to the queue, not to whoever asks — and the same visibility rule, so a retry
     * still waiting out its backoff is not backlog.
     */
    @Test
    void countVisibleCountsEveryShardAndOnlyWhatAClaimCouldTakeNow() {
        assertThat(queue.countVisible(NOW)).isZero();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000022", 7, NOW.minusSeconds(1)),
                shardedEntry("01991337-0000-7000-8000-000000000025", 8, NOW.minusSeconds(1))));
        assertThat(queue.countVisible(NOW)).as("every shard, not one node's slice").isEqualTo(2);

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000035", 9, NOW.plusSeconds(60))));
        assertThat(queue.countVisible(NOW)).as("waiting is not backlog").isEqualTo(2);
        assertThat(queue.countVisible(NOW.plusSeconds(61))).isEqualTo(3);
    }

    @Test
    void claimTakesVisibleEntriesInPriorityThenVisibilityOrder() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-000000000038", "job-a", 30, 1, NOW.minusSeconds(30)),
                entry("01991337-0000-7000-8000-000000000033", "job-a", 10, 1, NOW.minusSeconds(10)),
                entry("01991337-0000-7000-8000-00000000003e", "job-a", 20, 1, NOW.minusSeconds(20)),
                entry("01991337-0000-7000-8000-00000000003d", "job-a", 20, 1, NOW.minusSeconds(5))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, 10, List.of(), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("01991337-0000-7000-8000-000000000033", "01991337-0000-7000-8000-00000000003e", "01991337-0000-7000-8000-00000000003d", "01991337-0000-7000-8000-000000000038");
        // The queue emptied (a DELETE, not a state UPDATE) and the ownership was born in the same transaction
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_work WHERE work_state = 'LEASED' AND  node_id = '01991337-0000-7000-8000-000000000065'", String.class))
                .containsExactlyInAnyOrder("01991337-0000-7000-8000-000000000033", "01991337-0000-7000-8000-00000000003e", "01991337-0000-7000-8000-00000000003d", "01991337-0000-7000-8000-000000000038");
    }

    @Test
    void claimCopiesTheEntryAttemptIntoTheLease() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(entry("01991337-0000-7000-8000-000000000048", "job-a", 20, 3, NOW.minusSeconds(1))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, 10, List.of(), NOW);

        assertThat(claimed).singleElement().satisfies(w -> assertThat(w.attemptNumber()).isEqualTo(3));
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = '01991337-0000-7000-8000-000000000048'", Integer.class)).isEqualTo(3);
    }

    @Test
    void claimIgnoresEntriesNotYetVisible() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-00000000002b", "job-a", 20, 1, NOW),
                entry("01991337-0000-7000-8000-000000000030", "job-a", 20, 1, NOW.plusSeconds(60))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, 10, List.of(), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value()).containsExactly("01991337-0000-7000-8000-00000000002b");
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Integer.class)).isEqualTo(1);
    }

    @Test
    void claimIsBoundedByTheLimit() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-00000000001c", "job-a", 20, 1, NOW.minusSeconds(3)),
                entry("01991337-0000-7000-8000-00000000001e", "job-a", 20, 1, NOW.minusSeconds(2)),
                entry("01991337-0000-7000-8000-000000000020", "job-a", 20, 1, NOW.minusSeconds(1))));

        assertThat(queue.claim(0, NODE, 2, List.of(), NOW)).hasSize(2);
        assertThat(queue.claim(0, NODE, 2, List.of(), NOW)).hasSize(1);
    }

    /** One shard per statement — entries from another shard are invisible to the round. */
    @Test
    void claimSeesOnlyItsShard() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(new WorkQueue.ReadyEntry(ExecutionId.of("01991337-0000-7000-8000-000000000044"), JobKey.of("job-a"), 5, 20, 1, NOW.minusSeconds(1))));
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(entry("01991337-0000-7000-8000-000000000049", "job-a", 20, 1, NOW.minusSeconds(1))));

        assertThat(queue.claim(0, NODE, 10, List.of(), NOW))
                .extracting(w -> w.executionId().value()).containsExactly("01991337-0000-7000-8000-000000000049");
        assertThat(queue.claim(5, NODE, 10, List.of(), NOW))
                .extracting(w -> w.executionId().value()).containsExactly("01991337-0000-7000-8000-000000000044");
    }

    /** The inadmissible list filters PER JOB in the query itself — a closed window, a cap with no headroom or a missing handler never become a claim followed by a requeue. */
    @Test
    void claimRestrictsTheCandidateJobs() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-000000000043", "job-open", 20, 1, NOW.minusSeconds(2)),
                entry("01991337-0000-7000-8000-000000000027", "job-closed", 10, 1, NOW.minusSeconds(1))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, NODE, 10, List.of(JobKey.of("job-open")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value()).containsExactly("01991337-0000-7000-8000-000000000043");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY' AND  execution_id = '01991337-0000-7000-8000-000000000027'", Integer.class)).isEqualTo(1);
    }

    @Test
    void claimOfAnEmptyQueueReturnsEmpty() {
        assertThat(queue.claim(0, NODE, 10, List.of(), NOW)).isEmpty();
    }

    /** Requeue: the lease drops FENCED by the observed incarnation and the entry is reborn in the queue — the single path for recovery and for admission loss. */
    @Test
    void requeueDropsTheLeaseAndRebirthsTheEntryWhenTheFenceHolds() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(entry("01991337-0000-7000-8000-00000000001c", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, NODE, 10, List.of(), NOW);

        int requeued = queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), NODE, rawJdbcTemplate.queryForObject("SELECT claim_token FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = ?", String.class, ExecutionId.of("01991337-0000-7000-8000-00000000001c").value()), 1, NOW.plusSeconds(30))));

        assertThat(requeued).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isZero();
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_work WHERE work_state = 'READY' AND  execution_id = '01991337-0000-7000-8000-00000000001c'", Integer.class)).isEqualTo(1);
    }

    /** The fence in action: a requeue with a stale epoch (a zombie reaper) loses and does NOT duplicate the entry in the queue. */
    @Test
    void requeueWithAStaleFenceLosesAndDoesNotDuplicate() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(entry("01991337-0000-7000-8000-00000000001c", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, NODE, 10, List.of(), NOW);

        int requeued = queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), NODE, "01991337-0000-7000-8000-000000000085", 1, NOW)));

        assertThat(requeued).isZero();
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isEqualTo(1);
        assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Integer.class)).isZero();
    }

    @Test
    void claimAfterRequeueSeesTheRebornEntry() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new H2JdbcDelegate(), List.of(entry("01991337-0000-7000-8000-00000000001c", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, NODE, 10, List.of(), NOW);
        queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), NODE, rawJdbcTemplate.queryForObject("SELECT claim_token FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = ?", String.class, ExecutionId.of("01991337-0000-7000-8000-00000000001c").value()), 1, NOW)));

        List<WorkQueue.ClaimedWork> reclaimed = queue.claim(0, "01991337-0000-7000-8000-000000000067", 10, List.of(), NOW);

        assertThat(reclaimed).singleElement().satisfies(w -> {
            assertThat(w.executionId().value()).isEqualTo("01991337-0000-7000-8000-00000000001c");
            assertThat(w.attemptNumber()).isEqualTo(1);
        });
        assertThat(Set.copyOf(rawJdbcTemplate.queryForList(
                "SELECT node_id FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = '01991337-0000-7000-8000-00000000001c'", String.class))).containsExactly("01991337-0000-7000-8000-000000000067");
    }
}
