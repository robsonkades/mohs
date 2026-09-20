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

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.delegate.ClaimedReady;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * Complete work transitions over mohs_work. Claim validates a locked session and ordered job
 * admission before a guarded READY-to-LEASED update. Cancellation/manual retry also coordinate
 * history, batch and schedule effects inside their transaction.
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

    /**
     * Creates a {@code JdbcWorkQueue} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcWorkQueue(DataSource dataSource, JdbcDelegate delegate) {
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
        this.claimTransaction.setTimeout(JdbcSupport.TICK_STATEMENT_TIMEOUT_SECONDS);
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        // A template of its own: the timeout must not reach the claim or the requeue, which are the
        // work rather than the measurement of it
        JdbcTemplate countOperations = new JdbcTemplate(dataSource);
        countOperations.setQueryTimeout(COUNT_TIMEOUT_SECONDS);
        this.countTemplate = new NamedParameterJdbcTemplate(countOperations);
    }

    @Override
    public List<JobKey> findCandidateJobs(int shard, @Nullable JobKey after, int limit, Instant horizon) {
        Objects.requireNonNull(horizon, "horizon");
        if (shard < 0 || shard >= io.mohs.engine.Shards.SHARD_COUNT || limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("candidate shard or page limit out of range");
        }
        return tickTemplate.query(delegate.candidateJobKeys(), new MapSqlParameterSource("shard", shard)
                .addValue("afterKey", after == null ? "" : after.value())
                .addValue("now", delegate.splitTimestamp(horizon))
                .addValue("limit", (long) limit, java.sql.Types.BIGINT), (rs, _) -> JobKey.of(rs.getString("job_key")));
    }

    @Override
    public List<ClaimedWork> claim(int shard, String nodeId, int limit, Collection<JobKey> candidateJobs,
            Instant now) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(now, "now");
        if (limit <= 0) {
            return List.of();
        }
        if (candidateJobs.size() > JdbcSupport.MAX_IDS_PER_QUERY) throw new IllegalArgumentException("too many candidate jobs");
        int boundedLimit = Math.min(limit, JdbcSupport.MAX_IDS_PER_QUERY);
        String claimToken = io.github.robsonkades.uuidv7.UUIDv7.randomUUIDString();
        List<String> candidateKeys = candidateJobs.stream().map(JobKey::value).toList();
        // requireNonNull documents the invariant for @NullMarked — the callback never returns null
        List<ClaimedReady> claimed = Objects.requireNonNull(claimTransaction.execute(_ -> {
            var sessions = new JdbcSessions(tickTemplate, delegate);
            var session = sessions.lock(nodeId);
            Instant observed = sessions.now();
            if (session.isEmpty() || !session.orElseThrow().validAt(observed)) {
                return List.of();
            }
            // A caller may request an earlier horizon; a forward local clock cannot advance eligibility.
            Instant eligibleThrough = now.isBefore(observed) ? now : observed;
            var selected = new JdbcAdmission(tickTemplate, delegate).select(shard, boundedLimit, candidateKeys, eligibleThrough);
            if (selected.isEmpty()) return List.of();
            Instant grantedAt = sessions.now();
            if (!session.orElseThrow().validAt(grantedAt)) return List.of();
            return delegate.claimReady(tickTemplate, shard, nodeId, claimToken, boundedLimit, selected, eligibleThrough, grantedAt);
        }));
        return claimed.stream()
                .map(row -> new ClaimedWork(ExecutionId.of(row.executionId()), JobKey.of(row.jobKey()), row.attempt(), row.priority(), claimToken))
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
            var sessions = new JdbcSessions(tickTemplate, delegate);
            var activeNodes = new java.util.HashSet<String>();
            for (String nodeId : ordered.stream().map(Requeue::nodeId).distinct().sorted().toList()) {
                sessions.lock(nodeId).filter(sessions::active).ifPresent(node -> activeNodes.add(node.nodeId()));
            }
            var transitions = new JdbcExecutionTransitions(tickTemplate, delegate);
            var metadata = transitions.metadata(ordered.stream().map(Requeue::executionId).toList());
            transitions.lockJobs(metadata.values().stream().map(JdbcExecutionTransitions.Metadata::jobKey).toList());
            var retired = transitions.retiredJobs(metadata.values().stream().map(JdbcExecutionTransitions.Metadata::jobKey).toList());
            var cancelled = transitions.cancellationRequests(ordered.stream().map(Requeue::executionId).toList());
            Map<ExecutionId, Instant> finishedCancellations = new java.util.LinkedHashMap<>();
            int requeued = 0;
            for (Requeue order : ordered) {
                if (!activeNodes.contains(order.nodeId())) {
                    continue;
                }
                var stored = metadata.get(order.executionId());
                if (stored == null) continue;
                boolean terminal = cancelled.contains(order.executionId()) || retired.contains(stored.jobKey());
                var params = JdbcSupport.fencedLeaseDeleteParams(order.executionId().value(), order.nodeId(),
                        order.claimToken(), order.attemptNumber());
                if (!terminal) params.addValue("visibleAt", delegate.splitTimestamp(order.visibleAt()))
                        .addValue("nextAttempt", order.attemptNumber()).addValue("cancelRequested", false);
                int fenceWon = tickTemplate.update(terminal ? delegate.fencedLeaseDelete() : delegate.fencedReleaseWork(), params);
                if (fenceWon == 1) {
                    if (terminal) finishedCancellations.put(order.executionId(), sessions.now());
                    else requeued++;
                }
            }
            transitions.finishCancellations(finishedCancellations);
            return requeued;
        }));
    }

    @Override
    public boolean cancel(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        // requireNonNull: the same invariant as claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            var transitions = new JdbcExecutionTransitions(jdbcTemplate, delegate);
            var job = transitions.jobOf(id);
            if (job.isEmpty()) {
                return false;
            }
            transitions.lockJobs(List.of(job.orElseThrow()));
            MapSqlParameterSource idParam = new MapSqlParameterSource("executionId", id.value());
            if (jdbcTemplate.update(delegate.deleteReadyById(), idParam) == 1) {
                transitions.finishCancellations(Map.of(id, now));
                return true;
            }
            return jdbcTemplate.update(delegate.requestLeaseCancellation(), idParam.addValue("flag", true)) == 1;
        }));
    }

    /**
     * The CAS and the queue rebirth in a single guarded pair: the UPDATE only wins with the advisory
     * {@code FAILED} and the job alive (the EXISTS narrows the window against a concurrent
     * {@code remove}); the INSERT derives the attempt and the priority from history itself
     * ({@code last_attempt_number + 1}; the original priority), with the caller carrying nothing.
     */
    @Override
    public boolean rearmForManualRetry(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        // requireNonNull: the same invariant as claim()
        return Objects.requireNonNull(claimTransaction.execute(_ -> {
            MapSqlParameterSource params = new MapSqlParameterSource("executionId", id.value());
            var transitions = new JdbcExecutionTransitions(jdbcTemplate, delegate);
            var job = transitions.jobOf(id);
            if (job.isEmpty()) return false;
            transitions.lockJobs(List.of(job.orElseThrow()));
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
