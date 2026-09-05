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
import io.mohs.store.jdbc.delegate.ClaimedReady;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * {@link WorkQueue} over {@code mohs_ready}/{@code mohs_lease}. The claim is the transaction in which
 * queue and ownership change together or not at all; the SQL's shape belongs to {@link JdbcDelegate}
 * (a single statement on Postgres, a {@code DELETE … OUTPUT} plus the lease insert on SQL Server, three
 * portable statements elsewhere).
 *
 * <p>{@link #offer} deliberately does NOT open a transaction: the enqueue takes part in the caller's —
 * it is the async contract's "joins your transaction", now with execution, queue and idempotency in the
 * same unit.
 */
public final class JdbcWorkQueue implements WorkQueue {

    /**
     * Shorter than the prune's, and for a stricter reason: the prune runs hourly and can afford to
     * wait for a peer's locks, while this runs every ten seconds and takes no locks at all — if it has
     * not answered in two seconds the queue is deep enough that the claim is the thing that matters.
     */
    private static final int COUNT_TIMEOUT_SECONDS = 2;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    /** The claim, the requeue and the idle-gate probe — the loop thread's statements. */
    private final NamedParameterJdbcTemplate tickTemplate;
    private final NamedParameterJdbcTemplate countTemplate;
    private final TransactionTemplate claimTransaction;
    private final JdbcDelegate delegate;
    private final BatchStore batchStore;

    /**
     * Creates a {@code JdbcWorkQueue} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param delegate the database-specific SQL and timestamp adapter
     * @param batchStore the persistence port for batch counters
     */
    public JdbcWorkQueue(DataSource dataSource, JdbcDelegate delegate, BatchStore batchStore) {
        Objects.requireNonNull(dataSource, "dataSource");
        // The offer joins the caller's enqueue transaction, and cancel/rearm serve the API: none of
        // them runs on the loop thread, so none of them gets the tick's ceiling
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.tickTemplate = JdbcSupport.tickTemplate(dataSource);
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
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
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
                _ -> delegate.claimReady(tickTemplate, shard, nodeId, epoch, limit, inadmissibleKeys, now)));
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
        Integer found = tickTemplate.queryForObject(delegate.visibleWorkExists(), new MapSqlParameterSource()
                .addValue("shards", shards)
                .addValue("now", delegate.splitTimestamp(now)), Integer.class);
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
        Long depth = countTemplate.queryForObject(delegate.visibleWorkCount(),
                new MapSqlParameterSource("now", delegate.splitTimestamp(now)), Long.class);
        return depth == null ? 0L : depth;
    }

    @Override
    public void offer(List<ReadyEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(delegate.readyInsert(), entries.stream()
                .map(entry -> JdbcSupport.readyEntryParams(entry, delegate))
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
                int fenceWon = tickTemplate.update(delegate.fencedLeaseDelete(),
                        JdbcSupport.fencedLeaseDeleteParams(order.executionId().value(), order.nodeId(), order.epoch(),
                                order.attemptNumber()));
                if (fenceWon == 1) {
                    tickTemplate.update(delegate.readyInsert(), JdbcSupport.readyEntryParams(order.entry(), delegate));
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
            if (jdbcTemplate.update(delegate.deleteReadyById(), idParam) == 0) {
                return false;
            }
            jdbcTemplate.update(delegate.cancelExecution(), idParam.addValue("finishedAt", delegate.splitTimestamp(now)));
            // Cancelling is terminal, and an end that does not count leaves the batch open forever — it
            // counts as a failure in the SAME transaction as the delete (without the delete having taken
            // the entry, this point is not reached, so it counts exactly once)
            String batchId = jdbcTemplate.queryForObject(delegate.findBatchIdByExecution(), idParam, String.class);
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
            int rearmed = jdbcTemplate.update(delegate.rearmExecutionByCas(), params.addValue("retired", false));
            if (rearmed == 0) {
                return false;
            }
            jdbcTemplate.update(delegate.rearmReadyFromHistory(),
                    params.addValue("visibleAt", delegate.splitTimestamp(now)));
            return true;
        }));
    }
}
