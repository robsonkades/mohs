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

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.BatchStore;
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.ClaimedReady;
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * {@link WorkQueue} over {@code mohs_ready}/{@code mohs_lease}. The claim is the transaction in which
 * queue and ownership change together or not at all; the SQL's shape belongs to {@link JdbcDialect}
 * (a single statement on Postgres, three portable statements elsewhere).
 *
 * <p>{@link #offer} deliberately does NOT open a transaction: the enqueue takes part in the caller's —
 * it is the async contract's "joins your transaction", now with execution, queue and idempotency in the
 * same unit.
 */
public final class JdbcWorkQueue implements WorkQueue {

    /**
     * {@code EXISTS} rather than {@code LIMIT 1}/{@code TOP 1}: it short-circuits the same way and is
     * the same shape in all four dialects — not worth a {@link JdbcDialect} method for a yes-or-no
     * question.
     *
     * <p>The {@code %s} is the lock-free read hint: empty under MVCC, {@code WITH (NOLOCK)} on SQL
     * Server, where a plain {@code SELECT} would take shared locks on the system's hottest table — and
     * what would block is the tick's thread, which carries the heartbeat. The hint's anomalies are
     * exactly the error the probe's contract already declares acceptable (a missed row costs one poll; a
     * dirty row costs one lap).
     */
    private static final String VISIBLE_WORK_EXISTS = """
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM mohs_ready %sWHERE shard IN (:shards) AND visible_at <= :now
            ) THEN 1 ELSE 0 END
            """;

    /**
     * No shard predicate: the backlog is the queue's, not this node's. The lock-free hint is here for
     * the same reason it is on the probe above — a metric must never take a shared lock on the hot
     * path it is measuring — with one difference worth naming: a probe tolerates the hint's anomalies
     * because a wrong answer costs a lap, while a COUNT under them is simply approximate. That is what
     * a gauge is.
     *
     * <p>No index leads with {@code visible_at}, and none is added: a second index on the system's
     * hottest table, paid by every enqueue, claim and requeue, is a bad trade for a number sampled once
     * every ten seconds. The count therefore SCANS, and what it scans is {@code mohs_ready} — the
     * queue, never history — which also means its cost grows with the very backlog it is reporting.
     *
     */
    private static final String VISIBLE_WORK_COUNT = """
            SELECT COUNT(*) FROM mohs_ready %sWHERE visible_at <= :now
            """;

    /**
     * Shorter than the prune's, and for a stricter reason: the prune runs hourly and can afford to
     * wait for a peer's locks, while this runs every ten seconds and takes no locks at all — if it has
     * not answered in two seconds the queue is deep enough that the claim is the thing that matters.
     */
    private static final int COUNT_TIMEOUT_SECONDS = 2;

    private final String visibleWorkExists;
    private final String visibleWorkCount;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate countTemplate;
    private final TransactionTemplate claimTransaction;
    private final JdbcDialect dialect;
    private final BatchStore batchStore;

    public JdbcWorkQueue(DataSource dataSource, JdbcDialect dialect, BatchStore batchStore) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.claimTransaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // A claim is ALWAYS its own transaction — REQUIRES_NEW makes that executable rather than merely
        // conventional: with the default REQUIRED, an outer transaction (an interceptor, a test) would
        // impose ITS isolation and the READ COMMITTED below would be silently ignored (MySQL defaults to
        // REPEATABLE READ, the divergence this killed). The engine calls from its own loop, with no outer
        // transaction — the suspension never happens in normal operation.
        this.claimTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        // The same reasoning: SKIP LOCKED plus the inserts assume an explicit READ COMMITTED, never the
        // database's default (MySQL defaults to REPEATABLE READ).
        this.claimTransaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.visibleWorkExists = VISIBLE_WORK_EXISTS.formatted(dialect.lockFreeReadHint());
        this.visibleWorkCount = VISIBLE_WORK_COUNT.formatted(dialect.lockFreeReadHint());
        // A template of its own: the timeout must not reach the claim or the requeue, which are the
        // work rather than the measurement of it
        JdbcTemplate countOperations = new JdbcTemplate(dataSource);
        countOperations.setQueryTimeout(COUNT_TIMEOUT_SECONDS);
        this.countTemplate = new NamedParameterJdbcTemplate(countOperations);
    }

    @Override
    public List<ClaimedWork> claim(int shard, String nodeId, long epoch, int limit, Collection<JobKey> inadmissible,
            Instant now) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(now, "now");
        if (limit <= 0) {
            return List.of();
        }
        List<String> inadmissibleKeys = inadmissible.stream().map(JobKey::value).toList();
        // requireNonNull documents the invariant for @NullMarked — the callback never returns null
        List<ClaimedReady> claimed = Objects.requireNonNull(claimTransaction.execute(
                _ -> dialect.claimReady(jdbcTemplate, shard, nodeId, epoch, limit, inadmissibleKeys, now)));
        return claimed.stream()
                .map(row -> new ClaimedWork(ExecutionId.of(row.executionId()), JobKey.of(row.jobKey()), row.attempt(), row.priority()))
                .toList();
    }

    @Override
    public boolean hasVisibleWork(Collection<Integer> shards, Instant now) {
        Objects.requireNonNull(now, "now");
        if (shards.isEmpty()) {
            return false;
        }
        Integer found = jdbcTemplate.queryForObject(visibleWorkExists, new MapSqlParameterSource()
                .addValue("shards", shards)
                .addValue("now", dialect.splitTimestamp(now)), Integer.class);
        return found != null && found == 1;
    }

    /**
     * Bounded in time ({@code COUNT_TIMEOUT_SECONDS}, two seconds), because it runs on the engine's
     * loop thread ahead of the firing and the claim, and its cost grows with the backlog it reports.
     * The tick's sleep is already capped at {@code node-lease-ttl/3}: a count that outlives that
     * budget costs the node its heartbeat, and a node that misses its heartbeat has its work
     * reclaimed by a peer — the deep-queue case turning into the lost-work case.
     *
     * <p>A stale gauge is strictly better than a node that stopped claiming, and on timeout the
     * engine keeps the previous sample. Before the FIRST successful sample there is none, so a count
     * that fails from boot reports an empty queue rather than an old one — which is what makes
     * {@code mohs.tick.failed{step=queue-depth-sample}} the number to read first when the depth looks
     * implausibly calm.
     */
    @Override
    public long countVisible(Instant now) {
        Objects.requireNonNull(now, "now");
        Long depth = countTemplate.queryForObject(visibleWorkCount,
                new MapSqlParameterSource("now", dialect.splitTimestamp(now)), Long.class);
        return depth == null ? 0L : depth;
    }

    @Override
    public void offer(List<ReadyEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(JdbcSupport.READY_INSERT, entries.stream()
                .map(entry -> JdbcSupport.readyEntryParams(entry, dialect))
                .toArray(MapSqlParameterSource[]::new));
    }

    @Override
    public int requeue(List<Requeue> orders) {
        if (orders.isEmpty()) {
            return 0;
        }
        // The same canonical order as complete's DELETEs (JCIP ch. 10 on row locks): requeue and
        // completion lock overlapping sets — in opposite orders it would be the AB-BA deadlock the bench
        // measured (23 deadlocks)
        List<Requeue> ordered = orders.stream()
                .sorted(Comparator.comparing(order -> order.executionId().value()))
                .toList();
        // requireNonNull: the same invariant as claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            int requeued = 0;
            for (Requeue order : ordered) {
                int fenceWon = jdbcTemplate.update(JdbcSupport.FENCED_LEASE_DELETE,
                        JdbcSupport.fencedLeaseDeleteParams(order.executionId().value(), order.nodeId(), order.epoch()));
                if (fenceWon == 1) {
                    jdbcTemplate.update(JdbcSupport.READY_INSERT, JdbcSupport.readyEntryParams(order.entry(), dialect));
                    requeued++;
                }
            }
            return requeued;
        }));
    }

    @Override
    public boolean cancelQueued(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        // requireNonNull: the same invariant as claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            MapSqlParameterSource idParam = new MapSqlParameterSource("executionId", id.value());
            if (jdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = :executionId", idParam) == 0) {
                return false;
            }
            // The advisory terminal, matched by id — the primary key, the same match
            // LeaseStore's TERMINAL_UPDATE makes
            // batch-counted: incrementFailed just below, in this transaction
            jdbcTemplate.update("""
                    UPDATE mohs_execution SET state = 'CANCELLED', finished_at = :finishedAt
                    WHERE execution_id = :executionId
                    """, idParam.addValue("finishedAt", dialect.splitTimestamp(now)));
            // Cancelling is terminal, and an end that does not count leaves the batch open forever — it
            // counts as a failure in the SAME transaction as the delete (without the delete having taken
            // the entry, this point is not reached, so it counts exactly once)
            String batchId = jdbcTemplate.queryForObject(
                    "SELECT correlation_id FROM mohs_execution WHERE execution_id = :executionId", idParam, String.class);
            if (batchId != null) {
                batchStore.incrementFailed(batchId);
            }
            return true;
        }));
    }

    /**
     * The CAS and the queue rebirth in a single guarded pair: the UPDATE only wins with the advisory
     * {@code FAILED} and the job alive (the EXISTS narrows the window against a concurrent
     * {@code remove}); the INSERT derives the attempt and the priority from history itself (recorded
     * attempts plus 1; the original priority), with the caller carrying nothing.
     */
    @Override
    public boolean rearmForManualRetry(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        // requireNonNull: the same invariant as claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            MapSqlParameterSource params = new MapSqlParameterSource("executionId", id.value());
            // correlation_id IS NULL: a batch member does not rearm — the batch already counted this
            // failure; re-running would count the outcome TWICE in a batch that may already be closed (a
            // negative pending, a second BatchCompleted).
            int rearmed = jdbcTemplate.update("""
                    UPDATE mohs_execution SET state = 'PENDING', finished_at = NULL
                    WHERE execution_id = :executionId AND state = 'FAILED'
                      AND correlation_id IS NULL
                      AND EXISTS (SELECT 1 FROM mohs_job_definitions j
                                  WHERE j.job_key = mohs_execution.job_key AND j.retired = :retired)
                    """, params.addValue("retired", false));
            if (rearmed == 0) {
                return false;
            }
            jdbcTemplate.update("""
                    INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
                    SELECT e.execution_id, e.job_key, e.shard, e.priority,
                           (SELECT COUNT(*) + 1 FROM mohs_attempt a WHERE a.execution_id = e.execution_id),
                           :visibleAt
                    FROM mohs_execution e WHERE e.execution_id = :executionId
                    """, params.addValue("visibleAt", dialect.splitTimestamp(now)));
            return true;
        }));
    }
}
