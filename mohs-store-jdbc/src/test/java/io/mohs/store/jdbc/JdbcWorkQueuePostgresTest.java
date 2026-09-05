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
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The claim in the form only Tier 1 has — a SINGLE statement
 * ({@code WITH picked … DELETE … RETURNING → INSERT}) — plus the split tables'
 * {@code TIMESTAMPTZ}/{@code OffsetDateTime} crossing. The shared-semantics scenarios live in
 * {@code JdbcWorkQueueTest} (H2, portable form); what stays here are the ones that prove Postgres's
 * FORM.
 */
class JdbcWorkQueuePostgresTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    private DataSource dataSource;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcWorkQueue queue;

    @BeforeEach
    void setUp() {
        dataSource = PostgresTestSupport.freshSchema();
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        queue = new JdbcWorkQueue(dataSource, new PostgresJdbcDelegate(), new JdbcBatchStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC), new PostgresJdbcDelegate()));
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
    void singleStatementClaimMovesQueueEntriesIntoLeases() {
        queue.offer(List.of(
                entry("exec-high", "job-a", 10, 1, NOW.minusSeconds(10)),
                entry("exec-normal", "job-a", 20, 2, NOW.minusSeconds(20)),
                entry("exec-future", "job-a", 10, 1, NOW.plusSeconds(60))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "node-pg", 3, 10, List.of(), NOW);

        // The ORDER is pinned on purpose: an INSERT's RETURNING guarantees no order, so the statement
        // returns from the ordered SELECT over picked — the port's contract is the same in all four
        // delegates
        assertThat(claimed).extracting(w -> w.executionId().value())
                .containsExactly("exec-high", "exec-normal");
        assertThat(claimed).filteredOn(w -> w.executionId().value().equals("exec-normal"))
                .singleElement().satisfies(w -> assertThat(w.attemptNumber()).isEqualTo(2));
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_ready", String.class))
                .containsExactly("exec-future");
        assertThat(rawJdbcTemplate.queryForList(
                "SELECT execution_id FROM mohs_lease WHERE node_id = 'node-pg' AND epoch = 3", String.class))
                .containsExactlyInAnyOrder("exec-high", "exec-normal");
    }

    @Test
    void singleStatementClaimHonoursTheInadmissibleList() {
        queue.offer(List.of(
                entry("exec-open", "job-open", 20, 1, NOW.minusSeconds(2)),
                entry("exec-closed", "job-closed", 10, 1, NOW.minusSeconds(1))));

        List<WorkQueue.ClaimedWork> claimed = queue.claim(0, "node-pg", 1, 10, List.of(JobKey.of("job-closed")), NOW);

        assertThat(claimed).extracting(w -> w.executionId().value()).containsExactly("exec-open");
        assertThat(rawJdbcTemplate.queryForList("SELECT execution_id FROM mohs_ready", String.class))
                .containsExactly("exec-closed");
    }

    /** The UTC OffsetDateTime crossing: the instant crosses the TIMESTAMPTZ column verbatim, regardless of the session's zone. */
    @Test
    void visibleAtRoundTripsThroughTimestamptzVerbatim() {
        Instant visibleAt = Instant.parse("2026-11-01T05:30:00.123456Z");
        queue.offer(List.of(entry("exec-tz", "job-a", 20, 1, visibleAt)));

        Instant readBack = rawJdbcTemplate.queryForObject(
                "SELECT visible_at FROM mohs_ready WHERE execution_id = 'exec-tz'", java.time.OffsetDateTime.class)
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
        queue.offer(List.of(
                entry("exec-locked", "job-a", 10, 1, NOW.minusSeconds(2)),
                entry("exec-free", "job-a", 20, 1, NOW.minusSeconds(1))));
        try (java.sql.Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (java.sql.Statement st = other.createStatement()) {
                st.execute("SELECT execution_id FROM mohs_ready WHERE execution_id = 'exec-locked' FOR UPDATE");

                assertThat(queue.claim(0, "node-b", 1, 10, List.of(), NOW))
                        .extracting(w -> w.executionId().value())
                        .containsExactly("exec-free");
            } finally {
                other.rollback();
            }
        }
    }

    @Test
    void requeueFenceHoldsOnPostgresToo() {
        queue.offer(List.of(entry("exec-1", "job-a", 20, 1, NOW.minusSeconds(1))));
        queue.claim(0, "node-pg", 1, 10, List.of(), NOW);

        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-pg", 99, 1, entry("exec-1", "job-a", 20, 2, NOW))))).isZero();
        assertThat(queue.requeue(List.of(new WorkQueue.Requeue(
                ExecutionId.of("exec-1"), "node-pg", 1, 1, entry("exec-1", "job-a", 20, 2, NOW))))).isEqualTo(1);
    }
}
