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

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The claim in the form only Tier 1 has — a SINGLE statement
 * ({@code WITH picked … DELETE … RETURNING → INSERT}) — plus the split tables'
 * {@code TIMESTAMPTZ}/{@code OffsetDateTime} crossing. The shared-semantics scenarios live in
 * {@code JdbcWorkQueueTest} (H2, portable form); what stays here are the ones that prove Postgres's
 * FORM.
 */
@Tag("docker")
class JdbcWorkQueuePostgresTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new PostgresJdbcDelegate(), "01991337-0000-7000-8000-00000000006f", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new PostgresJdbcDelegate(), "01991337-0000-7000-8000-000000000067", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        queue = new JdbcWorkQueue(dataSource, new PostgresJdbcDelegate());
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
        new JdbcJobStore(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), Clock.fixed(NOW, ZoneOffset.UTC),
                new PostgresJdbcDelegate()).upsert(io.mohs.core.definition.JobDefinition.of(jobKey, new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        TestWork.history(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(new io.mohs.engine.SubmissionStore.NewExecution(ExecutionId.of(id), JobKey.of(jobKey),
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

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000023", 7, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("another node's shard").isFalse();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000031", 8, NOW.plusSeconds(60))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("not visible yet").isFalse();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000029", 8, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).isTrue();
    }

    private WorkQueue.ReadyEntry shardedEntry(String id, int shard, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("job-a"), shard, 20, 1, visibleAt);
    }

    @Test
    void singleStatementClaimMovesQueueEntriesIntoLeases() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-000000000033", "job-a", 10, 1, NOW.minusSeconds(10)),
                entry("01991337-0000-7000-8000-00000000003c", "job-a", 20, 2, NOW.minusSeconds(20)),
                entry("01991337-0000-7000-8000-000000000030", "job-a", 10, 1, NOW.plusSeconds(60))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "01991337-0000-7000-8000-00000000006f", 10, List.of(), NOW);

        // The ORDER is pinned on purpose: an INSERT's RETURNING guarantees no order, so the statement
        // returns from the ordered SELECT over picked — the port's contract is the same in all four
        // delegates
        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("01991337-0000-7000-8000-000000000033", "01991337-0000-7000-8000-00000000003c");
        assertThat(claimed).filteredOn(w -> w.executionId().value().equals("01991337-0000-7000-8000-00000000003c"))
                .singleElement().satisfies(w -> assertThat(w.attemptNumber()).isEqualTo(2));
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_work WHERE work_state = 'READY'", String.class))
                .containsExactly("01991337-0000-7000-8000-000000000030");
        assertThat(rawJdbcTemplate.queryForList(
                "SELECT execution_id FROM mohs_work WHERE work_state = 'LEASED' AND  node_id = '01991337-0000-7000-8000-00000000006f'", String.class))
                .containsExactlyInAnyOrder("01991337-0000-7000-8000-000000000033", "01991337-0000-7000-8000-00000000003c");
    }

    @Test
    void singleStatementClaimRestrictsCandidateJobs() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-000000000043", "job-open", 20, 1, NOW.minusSeconds(2)),
                entry("01991337-0000-7000-8000-000000000027", "job-closed", 10, 1, NOW.minusSeconds(1))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "01991337-0000-7000-8000-00000000006f", 10, List.of(JobKey.of("job-open")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value()).containsExactly("01991337-0000-7000-8000-000000000043");
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_work WHERE work_state = 'READY'", String.class))
                .containsExactly("01991337-0000-7000-8000-000000000027");
    }

    /** The UTC OffsetDateTime crossing: the instant crosses the TIMESTAMPTZ column verbatim, regardless of the session's zone. */
    @Test
    void visibleAtRoundTripsThroughTimestamptzVerbatim() {
        Instant visibleAt = Instant.parse("2026-11-01T05:30:00.123456Z");
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(entry("01991337-0000-7000-8000-00000000004a", "job-a", 20, 1, visibleAt)));

        Instant readBack = rawJdbcTemplate.queryForObject(
                "SELECT visible_at FROM mohs_work WHERE work_state = 'READY' AND  execution_id = '01991337-0000-7000-8000-00000000004a'", java.time.OffsetDateTime.class)
                .toInstant();

        assertThat(readBack).isEqualTo(visibleAt);
    }

    /**
     * The heart of a multi-node claim (JCIP ch. 12 — controlled interleaving, not a race): another
     * transaction holds an entry's lock; the claim SKIPS the locked row without blocking — the CTE's
     * {@code SKIP LOCKED} doing the work that was measured under load.
     */
    @Test
    void claimSkipsRowsLockedByAConcurrentClaimant() throws Exception {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-000000000036", "job-a", 10, 1, NOW.minusSeconds(2)),
                entry("01991337-0000-7000-8000-00000000002f", "job-a", 20, 1, NOW.minusSeconds(1))));
        try (java.sql.Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (java.sql.Statement st = other.createStatement()) {
                st.execute("SELECT execution_id FROM mohs_work WHERE work_state = 'READY' AND  execution_id = '01991337-0000-7000-8000-000000000036' FOR UPDATE");

                assertThat(queue.claim(0, "01991337-0000-7000-8000-000000000067", 10, List.of(), NOW))
                        .extracting(w -> w.executionId().value())
                        .containsExactly("01991337-0000-7000-8000-00000000002f");
            } finally {
                other.rollback();
            }
        }
    }

    @Test
    void requeueFenceHoldsOnPostgresToo() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new PostgresJdbcDelegate(), List.of(entry("01991337-0000-7000-8000-00000000001c", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "01991337-0000-7000-8000-00000000006f", 10, List.of(), NOW);

        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), "01991337-0000-7000-8000-00000000006f", "01991337-0000-7000-8000-000000000085", 1, NOW)))).isZero();
        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), "01991337-0000-7000-8000-00000000006f", rawJdbcTemplate.queryForObject("SELECT claim_token FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = ?", String.class, ExecutionId.of("01991337-0000-7000-8000-00000000001c").value()), 1, NOW)))).isEqualTo(1);
    }
}
