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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.Shards;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The statements SQL Server writes DIFFERENTLY, executed against a real SQL Server.
 *
 * <p>Four of the then-seven T-SQL divergences had no test that ran them anywhere (the count among
 * them has since gone portable — RCSI became the dialect's boot requirement and retired its hint):
 * {@code findExecutionPage},
 * {@code findOrphanedLeases}, {@code findOrphanedLeasesExceptAlive} and {@code visibleWorkCount}. The
 * claim's sweep and the idle-gate probe were covered ({@code JdbcWorkQueueSqlServerTest}); these were
 * not, in this era or the one before it — {@code topClause()} was consumed only by the page and the
 * reaper's sweep, and neither had a SQL Server test.
 *
 * <p>The gap became sharper when every statement moved into the delegates: {@code TOP (:limit)} used to
 * be one fragment with four consumers, and is now four independent copies. A correction applied to
 * three of them is exactly the drift {@code JdbcDelegateStatementDriftTest} cannot see, because
 * {@code SELECT TOP :limit} (invalid T-SQL — the parentheses are mandatory) and
 * {@code SELECT TOP (:limit)} bind the very same named parameter.
 *
 * <p>What each test asserts is the RESULT, not the SQL: a syntax error, a lost {@code ORDER BY} or a
 * ceiling that stopped bounding all surface here as a wrong answer rather than as a green run. The
 * failure this closes is a 3 a.m. one — a reaper that throws {@code Incorrect syntax near ':'} on every
 * sweep never reclaims a dead node's work, and the claim path stays green throughout.
 */
@Tag("docker")
class SqlServerTsqlStatementTest {

    private static final Instant NOW = Instant.parse("2026-08-22T12:00:00Z");

    record Handler() {
    }

    record Payload(String user) {
    }

    private DataSource dataSource;
    private JdbcWorkQueue queue;
    private JdbcLeaseStore leaseStore;
    private JdbcHistoryStore historyStore;

    @BeforeEach
    void setUp() {
        dataSource = SqlServerTestSupport.freshSchema();
        SqlServerJdbcDelegate delegate = new SqlServerJdbcDelegate();
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock, delegate);
        queue = new JdbcWorkQueue(dataSource, delegate, batchStore);
        leaseStore = new JdbcLeaseStore(dataSource, delegate, batchStore);
        historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), delegate);
        new JdbcJobStore(dataSource, clock, delegate).upsert(
                JobDefinition.of("job-a", Handler.class, spec -> spec.onDemand().runner("io")));
    }

    /**
     * {@code findDueRecurringJobs}: {@code TOP (:limit)} leads the sweep now that the ceiling lives in
     * the SQL. The ceiling and the {@code ORDER BY next_fire_at} are asserted together, with keys whose
     * lexicographic order contradicts the firing order — the same trick as the reaper's sweep below.
     */
    @Test
    void findDueRecurringBoundsWithTopAndFiresOldestFirst() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC), new SqlServerJdbcDelegate());
        jobStore.upsert(JobDefinition.of("job-b-due-later", Handler.class, spec -> spec.every(Duration.ofMinutes(5))));
        jobStore.upsert(JobDefinition.of("job-a-due-first", Handler.class, spec -> spec.every(Duration.ofMinutes(1))));

        assertThat(jobStore.findDueRecurring(NOW.plus(Duration.ofMinutes(10)), 1))
                .extracting(job -> job.definition().key().value())
                .containsExactly("job-a-due-first");
        assertThat(jobStore.findDueRecurring(NOW.plus(Duration.ofMinutes(10)), 10)).hasSize(2);
    }

    /**
     * {@code visibleWorkCount}: it used to carry {@code WITH (NOLOCK)} — the hint-placement parse was
     * what this pinned — until RCSI became the dialect's boot requirement and retired the hint. The
     * statement is portable text now; what remains worth pinning against a real SQL Server is that the
     * count still answers there, so a future hint that comes back malformed fails here first.
     */
    @Test
    void countVisibleAnswersOnSqlServer() {
        queue.offer(List.of(
                entry("exec-due-1", 20, 1, NOW.minusSeconds(2)),
                entry("exec-due-2", 20, 1, NOW.minusSeconds(1)),
                entry("exec-not-yet", 20, 2, NOW.plusSeconds(60))));

        assertThat(queue.countVisible(NOW)).isEqualTo(2);
    }

    /**
     * {@code findOrphanedLeases}: {@code TOP (:limit)} right after {@code SELECT}, with the
     * {@code ORDER BY claimed_at} that makes a mass death drain oldest first. The ceiling and the order
     * are asserted together because {@code TOP} without the order would still return the right COUNT.
     */
    @Test
    void findOrphanedBoundsWithTopAndDrainsOldestFirst() {
        // The ids are chosen so lexicographic order CONTRADICTS the expected one: the primary key is
        // clustered on execution_id (SQL Server's default), so a lost ORDER BY makes the scan answer
        // "exec-new" first. Rename these to exec-a/exec-b and the ORDER BY half stops guarding anything
        queue.offer(List.of(entry("exec-old", 20, 1, NOW.minusSeconds(30))));
        queue.claim(shardOf("exec-old"), "node-dead", 1, 10, List.of(), NOW.minusSeconds(20));
        queue.offer(List.of(entry("exec-new", 20, 1, NOW.minusSeconds(10))));
        queue.claim(shardOf("exec-new"), "node-dead", 1, 10, List.of(), NOW);

        List<LeaseStore.Lease> firstOnly = leaseStore.findOrphaned(List.of(), 1);

        assertThat(firstOnly).hasSize(1);
        assertThat(firstOnly.getFirst().executionId().value()).isEqualTo("exec-old");
        assertThat(leaseStore.findOrphaned(List.of(), 10)).hasSize(2);
    }

    /** {@code findOrphanedLeasesExceptAlive}: the same {@code TOP}, plus the {@code NOT IN} that spares the living. */
    @Test
    void findOrphanedExceptAliveSparesTheLivingNodes() {
        queue.offer(List.of(entry("exec-dead", 20, 1, NOW.minusSeconds(30))));
        queue.claim(shardOf("exec-dead"), "node-dead", 1, 10, List.of(), NOW.minusSeconds(20));
        queue.offer(List.of(entry("exec-alive", 20, 1, NOW.minusSeconds(10))));
        queue.claim(shardOf("exec-alive"), "node-alive", 1, 10, List.of(), NOW);

        assertThat(leaseStore.findOrphaned(List.of("node-alive"), 10))
                .extracting(lease -> lease.executionId().value())
                .containsExactly("exec-dead");
    }

    /**
     * {@code findExecutionPage}: {@code TOP (:limit)} leads a statement the other three delegates END
     * with {@code LIMIT :limit}, and the caller's {@code WHERE} is spliced between the joins and the
     * {@code ORDER BY}. Both arms are exercised — unfiltered and filtered — because the splice point is
     * where a malformed page would break.
     */
    @Test
    void findPageBoundsWithTopAndHonoursTheCallersFilters() {
        // Same reasoning as the ids above, in the other direction: the page orders execution_id DESC
        // while the clustered scan runs ascending, so a lost ORDER BY answers "019a-1" first
        historyStore.record(List.of(
                newExecution("019a-1"), newExecution("019a-2"), newExecution("019a-3")));

        List<Execution> firstPage = historyStore.findPage(null, null, null, null, null, 2, NOW);
        assertThat(firstPage).hasSize(2);
        assertThat(firstPage)
                .extracting(execution -> execution.id().value())
                .containsExactly("019a-3", "019a-2");

        assertThat(historyStore.findPage(JobKey.of("job-a"), null, null, null,
                ExecutionId.of("019a-3"), 10, NOW))
                .extracting(execution -> execution.id().value())
                .containsExactly("019a-2", "019a-1");

        assertThat(historyStore.findPage(JobKey.of("no-such-job"), ExecutionState.SUCCEEDED,
                null, null, null, 10, NOW)).isEmpty();
    }

    private WorkQueue.ReadyEntry entry(String id, int priority, int attempt, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("job-a"),
                shardOf(id), priority, attempt, visibleAt);
    }

    private static HistoryStore.NewExecution newExecution(String id) {
        return new HistoryStore.NewExecution(ExecutionId.of(id), JobKey.of("job-a"), shardOf(id), 20,
                NOW, NOW, "application", null, null, new Payload("ana"));
    }

    /** The shard an entry lands in — the claim is single-shard per statement, so a test that claims has to ask for the right one. */
    private static int shardOf(String executionId) {
        return Shards.of(ExecutionId.of(executionId));
    }
}
