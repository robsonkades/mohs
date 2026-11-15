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
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/** The claim's T-SQL sweep ({@code TOP} plus {@code UPDLOCK, ROWLOCK, READPAST}) against a real SQL Server (Tier 2). */
@Tag("docker")
class JdbcWorkQueueSqlServerTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = SqlServerTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new SqlServerJdbcDelegate(), "01991337-0000-7000-8000-00000000006c", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        io.mohs.store.jdbc.TestSessions.seed(dataSource, new SqlServerJdbcDelegate(), "01991337-0000-7000-8000-000000000067", io.mohs.core.EngineState.RUNNING, java.time.Duration.ofMinutes(5));
        queue = new JdbcWorkQueue(dataSource, new SqlServerJdbcDelegate());
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
        new JdbcJobStore(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), Clock.fixed(NOW, ZoneOffset.UTC),
                new SqlServerJdbcDelegate()).upsert(io.mohs.core.definition.JobDefinition.of(jobKey, new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        TestWork.history(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new SqlServerJdbcDelegate(), List.of(new io.mohs.engine.SubmissionStore.NewExecution(ExecutionId.of(id), JobKey.of(jobKey),
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

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new SqlServerJdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000023", 7, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("another node's shard").isFalse();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new SqlServerJdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000031", 8, NOW.plusSeconds(60))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("not visible yet").isFalse();

        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new SqlServerJdbcDelegate(), List.of(shardedEntry("01991337-0000-7000-8000-000000000029", 8, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).isTrue();
    }

    private WorkQueue.ReadyEntry shardedEntry(String id, int shard, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("job-a"), shard, 20, 1, visibleAt);
    }

    @Test
    void claimMovesQueueEntriesIntoLeasesInOrder() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new SqlServerJdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-00000000003c", "job-a", 20, 1, NOW.minusSeconds(20)),
                entry("01991337-0000-7000-8000-000000000033", "job-a", 10, 2, NOW.minusSeconds(10)),
                entry("01991337-0000-7000-8000-000000000034", "job-closed", 0, 1, NOW.minusSeconds(30))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "01991337-0000-7000-8000-00000000006c", 10, List.of(JobKey.of("job-a")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("01991337-0000-7000-8000-000000000033", "01991337-0000-7000-8000-00000000003c");
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_work WHERE work_state = 'READY'", String.class))
                .containsExactly("01991337-0000-7000-8000-000000000034");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = '01991337-0000-7000-8000-000000000033'", Integer.class)).isEqualTo(2);
    }

    /**
     * The lock here sits on the CLUSTERED key only (a seek by execution_id — the shape a mid-flight
     * {@code cancelQueued} holds), so the covering seek on {@code idx_mohs_ready_claim} still PICKS the
     * row: what makes the claim skip it instead of blocking is the {@code READPAST} on the DELETE
     * target, not the one on the pick. A concurrent claimant, by contrast, holds the claim-index key
     * and is skipped at the pick — {@code ConcurrentClaimAcrossDialectsTest} pins that. Losing either
     * hint passes green through the rest of the suite and becomes a lock wait against a cancel, or a
     * peer, in production (JCIP ch. 12, controlled interleaving).
     */
    @Test
    void claimSkipsARowAnotherTransactionHoldsAtTheClusteredKey() throws Exception {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new SqlServerJdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-000000000036", "job-a", 10, 1, NOW.minusSeconds(2)),
                entry("01991337-0000-7000-8000-00000000002f", "job-a", 20, 1, NOW.minusSeconds(1))));
        try (java.sql.Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (java.sql.Statement st = other.createStatement()) {
                st.execute("SELECT execution_id FROM mohs_work WITH (UPDLOCK, ROWLOCK) WHERE work_state = 'READY' AND  execution_id = '01991337-0000-7000-8000-000000000036'");

                assertThat(queue.claim(0, "01991337-0000-7000-8000-000000000067", 10, List.of(), NOW))
                        .extracting(w -> w.executionId().value())
                        .containsExactly("01991337-0000-7000-8000-00000000002f");
            } finally {
                other.rollback();
            }
        }
    }

    @Test
    void requeueFenceHoldsOnSqlServerToo() {
        TestWork.ready(java.util.Objects.requireNonNull(rawJdbcTemplate.getDataSource()), new SqlServerJdbcDelegate(), List.of(entry("01991337-0000-7000-8000-00000000001c", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "01991337-0000-7000-8000-00000000006c", 10, List.of(), NOW);

        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), "01991337-0000-7000-8000-00000000006c", "01991337-0000-7000-8000-000000000085", 1, NOW)))).isZero();
        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("01991337-0000-7000-8000-00000000001c"), "01991337-0000-7000-8000-00000000006c", rawJdbcTemplate.queryForObject("SELECT claim_token FROM mohs_work WHERE work_state = 'LEASED' AND  execution_id = ?", String.class, ExecutionId.of("01991337-0000-7000-8000-00000000001c").value()), 1, NOW)))).isEqualTo(1);
    }
}
