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
    private JdbcNodeStore nodes;
    private JdbcWorkQueue queue;
    private JdbcLeaseStore leaseStore;
    private JdbcHistoryStore historyStore;

    @BeforeEach
    void setUp() {
        dataSource = SqlServerTestSupport.freshSchema();
        SqlServerJdbcDelegate delegate = new SqlServerJdbcDelegate();
        nodes = new JdbcNodeStore(dataSource, delegate);
        TestSessions.seed(dataSource, delegate, "01991337-0000-7000-8000-000000000069", io.mohs.core.EngineState.RUNNING, Duration.ofMinutes(5));
        TestSessions.seed(dataSource, delegate, "01991337-0000-7000-8000-000000000066", io.mohs.core.EngineState.RUNNING, Duration.ofMinutes(5));
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, delegate);
        queue = new JdbcWorkQueue(dataSource, delegate);
        leaseStore = new JdbcLeaseStore(dataSource, delegate);
        historyStore = new JdbcHistoryStore(dataSource, delegate);
        new JdbcJobStore(dataSource, clock, delegate).upsert(
                JobDefinition.of("job-a", new io.mohs.core.schedule.OnDemandSpec(), io.mohs.core.definition.JobOptions.defaults().withRunner("io"), io.mohs.core.schedule.Misfire.IGNORE, false));
    }

    /**
     * {@code findDueRecurringJobs}: {@code TOP (:limit)} leads the sweep now that the ceiling lives in
     * the SQL. The ceiling and the {@code ORDER BY next_fire_at} are asserted together, with keys whose
     * lexicographic order contradicts the firing order — the same trick as the reaper's sweep below.
     */
    @Test
    void findDueRecurringBoundsWithTopAndFiresOldestFirst() {
        JdbcJobStore jobStore = new JdbcJobStore(dataSource, Clock.fixed(NOW, ZoneOffset.UTC), new SqlServerJdbcDelegate());
        jobStore.upsert(JobDefinition.of("job-b-due-later", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(5), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));
        jobStore.upsert(JobDefinition.of("job-a-due-first", new io.mohs.core.schedule.IntervalSpec(Duration.ofMinutes(1), false), io.mohs.core.definition.JobOptions.defaults(), io.mohs.core.schedule.Misfire.IGNORE, false));

        assertThat(jobStore.findDueRecurring(NOW.plus(Duration.ofMinutes(10)), 1).jobs())
                .extracting(job -> job.definition().key().value())
                .containsExactly("job-a-due-first");
        assertThat(jobStore.findDueRecurring(NOW.plus(Duration.ofMinutes(10)), 10).jobs()).hasSize(2);
    }

    /**
     * {@code visibleWorkCount}: it used to carry {@code WITH (NOLOCK)} — the hint-placement parse was
     * what this pinned — until RCSI became the dialect's boot requirement and retired the hint. The
     * statement is portable text now; what remains worth pinning against a real SQL Server is that the
     * count still answers there, so a future hint that comes back malformed fails here first.
     */
    @Test
    void countVisibleAnswersOnSqlServer() {
        TestWork.ready(dataSource, new SqlServerJdbcDelegate(), List.of(
                entry("01991337-0000-7000-8000-00000000002c", 20, 1, NOW.minusSeconds(2)),
                entry("01991337-0000-7000-8000-00000000002d", 20, 1, NOW.minusSeconds(1)),
                entry("01991337-0000-7000-8000-00000000003f", 20, 2, NOW.plusSeconds(60))));

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
        // "01991337-0000-7000-8000-00000000003b" first. Rename these to exec-a/exec-b and the ORDER BY half stops guarding anything
        TestWork.ready(dataSource, new SqlServerJdbcDelegate(), List.of(entry("01991337-0000-7000-8000-000000000042", 20, 1, NOW.minusSeconds(30))));
        queue.claim(shardOf("01991337-0000-7000-8000-000000000042"), "01991337-0000-7000-8000-000000000069", 10, List.of(), NOW.minusSeconds(20));
        TestWork.ready(dataSource, new SqlServerJdbcDelegate(), List.of(entry("01991337-0000-7000-8000-00000000003b", 20, 1, NOW.minusSeconds(10))));
        queue.claim(shardOf("01991337-0000-7000-8000-00000000003b"), "01991337-0000-7000-8000-000000000069", 10, List.of(), NOW);

        nodes.stop("01991337-0000-7000-8000-000000000069");
        List<LeaseStore.Lease> firstOnly = leaseStore.findOrphaned( 1);

        assertThat(firstOnly).hasSize(1);
        assertThat(firstOnly.getFirst().executionId().value()).isEqualTo("01991337-0000-7000-8000-000000000042");
        assertThat(leaseStore.findOrphaned( 10)).hasSize(2);
    }

    /** {@code findOrphanedLeasesExceptAlive}: the same {@code TOP}, plus the {@code NOT IN} that spares the living. */
    @Test
    void findOrphanedExceptAliveSparesTheLivingNodes() {
        TestWork.ready(dataSource, new SqlServerJdbcDelegate(), List.of(entry("01991337-0000-7000-8000-000000000028", 20, 1, NOW.minusSeconds(30))));
        queue.claim(shardOf("01991337-0000-7000-8000-000000000028"), "01991337-0000-7000-8000-000000000069", 10, List.of(), NOW.minusSeconds(20));
        TestWork.ready(dataSource, new SqlServerJdbcDelegate(), List.of(entry("01991337-0000-7000-8000-000000000024", 20, 1, NOW.minusSeconds(10))));
        queue.claim(shardOf("01991337-0000-7000-8000-000000000024"), "01991337-0000-7000-8000-000000000066", 10, List.of(), NOW);

        nodes.stop("01991337-0000-7000-8000-000000000069");
        assertThat(leaseStore.findOrphaned( 10))
                .extracting(lease -> lease.executionId().value())
                .containsExactly("01991337-0000-7000-8000-000000000028");
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
        // while the clustered scan runs ascending, so a lost ORDER BY answers "01991337-0003-7000-8000-0000000000a1" first
        TestSubmissions.accept(dataSource, new SqlServerJdbcDelegate(), List.of(
                newExecution("01991337-0003-7000-8000-0000000000a1"), newExecution("01991337-0003-7000-8000-0000000000a2"), newExecution("01991337-0003-7000-8000-0000000000a3")));

        List<Execution> firstPage = historyStore.findPage(null, null, null, null, null, 2, NOW);
        assertThat(firstPage).hasSize(2);
        assertThat(firstPage)
                .extracting(execution -> execution.id().value())
                .containsExactly("01991337-0003-7000-8000-0000000000a3", "01991337-0003-7000-8000-0000000000a2");

        assertThat(historyStore.findPage(JobKey.of("job-a"), null, null, null,
                ExecutionId.of("01991337-0003-7000-8000-0000000000a3"), 10, NOW))
                .extracting(execution -> execution.id().value())
                .containsExactly("01991337-0003-7000-8000-0000000000a2", "01991337-0003-7000-8000-0000000000a1");

        assertThat(historyStore.findPage(JobKey.of("no-such-job"), ExecutionState.SUCCEEDED,
                null, null, null, 10, NOW)).isEmpty();
    }

    private WorkQueue.ReadyEntry entry(String id, int priority, int attempt, Instant visibleAt) {
        return new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of("job-a"),
                shardOf(id), priority, attempt, visibleAt);
    }

    private static SubmissionStore.NewExecution newExecution(String id) {
        return new SubmissionStore.NewExecution(ExecutionId.of(id), JobKey.of("job-a"), shardOf(id), 20,
                NOW, NOW, "application", null, null, io.mohs.test.InputFixtures.encode(JobKey.of("job-a"), new Payload("ana")), io.mohs.engine.SubmissionStore.Origin.MANUAL, null);
    }

    /** The shard an entry lands in — the claim is single-shard per statement, so a test that claims has to ask for the right one. */
    private static int shardOf(String executionId) {
        return Shards.of(ExecutionId.of(executionId));
    }
}
