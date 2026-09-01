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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/** The claim's T-SQL sweep ({@code TOP} plus {@code UPDLOCK, ROWLOCK, READPAST}) against a real SQL Server (Tier 2). */
class JdbcWorkQueueSqlServerTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = SqlServerTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        queue = new JdbcWorkQueue(dataSource, new SqlServerJdbcDelegate(), new JdbcBatchStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC), new SqlServerJdbcDelegate()));
    }

    private WorkQueue.ReadyEntry entry(String id, String jobKey, int priority, int attempt, Instant visibleAt) {
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

        queue.offer(List.of(shardedEntry("exec-alheio", 7, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("another node's shard").isFalse();

        queue.offer(List.of(shardedEntry("exec-futuro", 8, NOW.plusSeconds(60))));
        assertThat(queue.hasVisibleWork(owned, NOW)).as("not visible yet").isFalse();

        queue.offer(List.of(shardedEntry("exec-devido", 8, NOW.minusSeconds(1))));
        assertThat(queue.hasVisibleWork(owned, NOW)).isTrue();
    }

    private WorkQueue.ReadyEntry shardedEntry(String id, int shard, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("job-a"), shard, 20, 1, visibleAt);
    }

    @Test
    void claimMovesQueueEntriesIntoLeasesInOrder() {
        queue.offer(List.of(
                entry("exec-normal", "job-a", 20, 1, NOW.minusSeconds(20)),
                entry("exec-high", "job-a", 10, 2, NOW.minusSeconds(10)),
                entry("exec-inadmissible", "job-closed", 0, 1, NOW.minusSeconds(30))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "node-mssql", 1, 10, List.of(JobKey.of("job-closed")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("exec-high", "exec-normal");
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_ready", String.class))
                .containsExactly("exec-inadmissible");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT attempt_number FROM mohs_lease WHERE execution_id = 'exec-high'", Integer.class)).isEqualTo(2);
    }

    /**
     * The subtlest part of the T-SQL emulation: {@code READPAST} is what makes the claim SKIP a row
     * locked by another node instead of blocking — losing it from the hint would pass green through the
     * rest of the suite and become a lock wait in a multi-node claim (JCIP ch. 12, controlled
     * interleaving).
     */
    @Test
    void claimSkipsRowsLockedByAConcurrentClaimant() throws Exception {
        queue.offer(List.of(
                entry("exec-locked", "job-a", 10, 1, NOW.minusSeconds(2)),
                entry("exec-free", "job-a", 20, 1, NOW.minusSeconds(1))));
        try (java.sql.Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (java.sql.Statement st = other.createStatement()) {
                st.execute("SELECT execution_id FROM mohs_ready WITH (UPDLOCK, ROWLOCK) WHERE execution_id = 'exec-locked'");

                assertThat(queue.claim(0, "node-b", 1, 10, List.of(), NOW))
                        .extracting(w -> w.executionId().value())
                        .containsExactly("exec-free");
            } finally {
                other.rollback();
            }
        }
    }

    @Test
    void requeueFenceHoldsOnSqlServerToo() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-mssql", 1, 10, List.of(), NOW);

        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-mssql", 99, entry("exec-1", "job-a", 20, 2, NOW))))).isZero();
        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-mssql", 1, entry("exec-1", "job-a", 20, 2, NOW))))).isEqualTo(1);
    }
}
