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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;


import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.JobPayload;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Execution and receipt queries, plus bounded retention. All execution writes belong to complete operations. */
public final class JdbcHistoryStore implements HistoryStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate pruneTemplate;
    private final NamedParameterJdbcTemplate sweepTemplate;
    private final JdbcDelegate delegate;
    private final org.springframework.transaction.support.TransactionTemplate pruning;

    /** Statement ceiling; the pruning transaction's remaining timeout may impose a shorter limit. */
    private static final int PRUNE_TIMEOUT_SECONDS = 5;

    /** A history page issues two deletes; keep each statement within the maintenance loop's budget. */
    private static final int SWEEP_TIMEOUT_SECONDS = 1;

    /**
     * Creates a {@code JdbcHistoryStore} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcHistoryStore(DataSource dataSource, JdbcDelegate delegate) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        // Templates of their own: the timeouts must not reach the read model or the payload read,
        // which run on the hot path and have no business being cancelled mid-flight
        JdbcTemplate pruneOperations = new JdbcTemplate(dataSource);
        pruneOperations.setQueryTimeout(PRUNE_TIMEOUT_SECONDS);
        this.pruneTemplate = new NamedParameterJdbcTemplate(pruneOperations);
        JdbcTemplate sweepOperations = new JdbcTemplate(dataSource);
        sweepOperations.setQueryTimeout(SWEEP_TIMEOUT_SECONDS);
        this.sweepTemplate = new NamedParameterJdbcTemplate(sweepOperations);
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.pruning = new org.springframework.transaction.support.TransactionTemplate(
                new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource));
        this.pruning.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.pruning.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.pruning.setTimeout(3);
    }

    @Override
    public Optional<io.mohs.core.event.Enqueued> findReceiptByIdempotencyKey(
            JobKey jobKey, String idempotencyKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        return JdbcSupport.findOne(jdbcTemplate, delegate.findIdempotencyReceipt(),
                new MapSqlParameterSource("jobKey", jobKey.value()).addValue("idempotencyKey", idempotencyKey)
                        .addValue("now", delegate.splitTimestamp(JdbcDatabaseTime.now(jdbcTemplate, delegate))), rs -> {
                    ExecutionId id = ExecutionId.of(rs.getString("execution_id"));
                    Instant scheduledAt = Objects.requireNonNull(delegate.readSplitTimestamp(rs, "scheduled_at"), "scheduled_at");
                    String actor = rs.getString("actor");
                    return new io.mohs.core.event.Enqueued(id, jobKey, scheduledAt, actor);
                });
    }

    /** The same discipline as {@code findPayloads}: a PER-ROW verdict — an unreadable row does not contaminate its neighbours; only infrastructure propagates. */
    @Override
    public PayloadBatch findPayloads(List<ExecutionId> ids) {
        Objects.requireNonNull(ids, "ids");
        if (ids.isEmpty()) {
            return new PayloadBatch(Map.of(), Map.of());
        }
        Map<ExecutionId, PayloadRow> rows = new LinkedHashMap<>();
        Map<ExecutionId, RuntimeException> unreadable = new LinkedHashMap<>();
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            jdbcTemplate.query(delegate.findPayloads(),
                    new MapSqlParameterSource("ids", chunk), rs -> {
                ExecutionId id = ExecutionId.of(rs.getString("execution_id"));
                try {
                    rows.put(id, new PayloadRow(mapHead(rs), readPayload(rs)));
                } catch (RuntimeException e) {
                    unreadable.put(id, e);
                }
            });
        }
        return new PayloadBatch(rows, unreadable);
    }

    @Override
    public List<ExecutionHead> findHeads(List<ExecutionId> ids) {
        Objects.requireNonNull(ids, "ids");
        if (ids.isEmpty()) {
            return List.of();
        }
        List<ExecutionHead> heads = new ArrayList<>(ids.size());
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            heads.addAll(jdbcTemplate.query(delegate.findHeads(),
                    new MapSqlParameterSource("ids", chunk), (rs, _) -> mapHead(rs)));
        }
        return heads;
    }

    private ExecutionHead mapHead(ResultSet rs) throws SQLException {
        return new ExecutionHead(
                ExecutionId.of(rs.getString("execution_id")),
                JobKey.of(rs.getString("job_key")),
                Objects.requireNonNull(delegate.readSplitTimestamp(rs, "scheduled_at"), "scheduled_at"),
                Objects.requireNonNull(delegate.readSplitTimestamp(rs, "created_at"), "created_at"),
                rs.getString("actor"),
                rs.getInt("priority"),
                rs.getString("batch_id"));
    }

    private JobPayload readPayload(ResultSet rs) throws SQLException {
        return new JobPayload(rs.getString("payload"), rs.getString("input_contract"));
    }

    /** One row-bounded transaction. The engine budgets repeated pages and resumes a full backlog later. */
    @Override
    public int pruneExpiredIdempotency() {
        return Objects.requireNonNull(pruning.execute(_ -> pruneTemplate.update(delegate.pruneExpiredIdempotency(),
                new MapSqlParameterSource("now", delegate.splitTimestamp(JdbcDatabaseTime.now(pruneTemplate, delegate)))
                        .addValue("limit", (long) HistoryStore.IDEMPOTENCY_PRUNE_BATCH, java.sql.Types.BIGINT))));
    }

    /**
     * Bounded twice over: in ROWS ({@code :limit} per statement — history is the biggest thing in
     * the schema, and an unbounded first sweep over a never-enforced window would hold locks past
     * any timeout's mercy) and in WAITING ({@link #SWEEP_TIMEOUT_SECONDS} per statement). Each
     * statement commits alone; the ORDER makes that safe (see the interface contract).
     */
    @Override
    public PrunedHistory pruneHistoryBefore(Instant cutoff, int limit) {
        Objects.requireNonNull(cutoff, "cutoff");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("cutoff", delegate.splitTimestamp(cutoff))
                .addValue("cutoffId", uuidV7LowerBound(cutoff))
                .addValue("limit", (long) limit, java.sql.Types.BIGINT);
        int executions = sweepTemplate.update(delegate.pruneTerminalExecutionsBefore(), params);
        int batches = sweepTemplate.update(delegate.pruneEmptyBatchesBefore(), params);
        return new PrunedHistory(executions, batches);
    }

    /**
     * The smallest UUIDv7 an id created at {@code instant} could be: the 48-bit millisecond prefix
     * with every random bit at zero (version nibble 7, minimal RFC variant 8). Ids are stored as
     * lowercase hex strings and UUIDv7 sorts lexicographically in time order, so
     * {@code execution_id < } this bound selects exactly the rows born before the instant — the
     * primary-key range scan the sweep's statements rely on instead of a new index on
     * {@code finished_at}.
     */
    private static String uuidV7LowerBound(Instant instant) {
        long ms = instant.toEpochMilli();
        return "%08x-%04x-7000-8000-000000000000".formatted(ms >>> 16, ms & 0xFFFF);
    }

    @Override
    public Optional<Execution> find(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        return JdbcSupport.findOne(jdbcTemplate, delegate.findExecutionById(),
                new MapSqlParameterSource("executionId", id.value()),
                rs -> mapDerived(rs, now, rs.getInt("last_attempt_number") == 0
                        ? Optional.empty() : Optional.of(mapAttempt(rs))));
    }

    @Override
    public List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit, Instant now) {
        Objects.requireNonNull(now, "now");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("limit", (long) limit, java.sql.Types.BIGINT)
                .addValue("now", delegate.splitTimestamp(now));
        List<String> conditions = new ArrayList<>();
        if (jobKey != null) {
            conditions.add("e.job_key = :jobKey");
            params.addValue("jobKey", jobKey.value());
        }
        if (from != null) {
            conditions.add("e.scheduled_at >= :from");
            params.addValue("from", delegate.splitTimestamp(from));
        }
        if (to != null) {
            conditions.add("e.scheduled_at <= :to");
            params.addValue("to", delegate.splitTimestamp(to));
        }
        if (cursor != null) {
            conditions.add("e.execution_id < :cursor");
            params.addValue("cursor", cursor.value());
        }
        if (status != null) {
            // A filter over the DERIVED state: terminal lives in the column; RUNNING lives in the
            // ownership; ENQUEUED/RETRY_WAITING live in the queue, separated by the visibility rule
            switch (status) {
                case SUCCEEDED, FAILED, CANCELLED -> {
                    conditions.add("e.state = :status");
                    params.addValue("status", status.name());
                }
                case RUNNING -> conditions.add("w.work_state = 'LEASED'");
                case RETRY_WAITING -> conditions.add("w.attempt > 1 AND w.visible_at > :now");
                case ENQUEUED -> conditions.add("w.work_state = 'READY' AND (w.attempt = 1 OR w.visible_at <= :now)");
            }
        }
        String where = conditions.isEmpty() ? "" : "WHERE " + String.join(" AND ", conditions) + "\n";
        // Summaries omit attempt details and their potentially large failure text.
        return jdbcTemplate.query(delegate.findExecutionPage(where), params, (rs, _) -> mapDerived(rs, now, Optional.empty()));
    }

    /** Row to {@link Execution}; the state, owner and firing instant come from {@link #deriveState}. */
    private Execution mapDerived(ResultSet rs, Instant now, Optional<Attempt> lastAttempt) throws SQLException {
        ExecutionId id = ExecutionId.of(rs.getString("execution_id"));
        DerivedState derived = deriveState(rs, now);
        return new Execution(id, JobKey.of(rs.getString("job_key")), derived.state(),
                Objects.requireNonNull(delegate.readSplitTimestamp(rs, "scheduled_at"), "scheduled_at"),
                derived.firedAt(), lastAttempt, rs.getString("actor"), Priority.fromValue(rs.getInt("priority")),
                rs.getString("idempotency_key"), rs.getString("batch_id"), derived.owner());
    }

    /**
     * Terminal lives in the column; RUNNING lives in the ownership; ENQUEUED/RETRY_WAITING live in the
     * queue, separated by the visibility rule — the Java twin of the SQL filter in {@link #findPage},
     * and of the contract in {@link HistoryStore#find}. The two must agree.
     */
    private DerivedState deriveState(ResultSet rs, Instant now) throws SQLException {
        String column = rs.getString("state");
        String leaseNode = rs.getString("lease_node");
        boolean queued = rs.getString("ready_id") != null;
        if (!"PENDING".equals(column)) {
            return DerivedState.unowned(ExecutionState.valueOf(column));
        }
        if ((leaseNode != null) == queued) {
            throw new IllegalStateException("pending execution " + rs.getString("execution_id")
                    + " must have exactly one active work record");
        }
        if (leaseNode != null) {
            return new DerivedState(ExecutionState.RUNNING, leaseNode, delegate.readSplitTimestamp(rs, "lease_claimed_at"));
        }
        if (queued && rs.getInt("ready_attempt") > 1
                && Objects.requireNonNull(delegate.readSplitTimestamp(rs, "ready_visible_at"), "ready_visible_at").isAfter(now)) {
            return DerivedState.unowned(ExecutionState.RETRY_WAITING);
        }
        // A single statement sees either side of the committed transition. An uncommitted
        // completion never justifies fabricating queue membership for a broken pending row.
        return DerivedState.unowned(ExecutionState.ENQUEUED);
    }

    /** What the joined row says beyond the {@code state} column: only a RUNNING execution has an owner and a firing instant. */
    private record DerivedState(ExecutionState state, @Nullable String owner, @Nullable Instant firedAt) {

        static DerivedState unowned(ExecutionState state) {
            return new DerivedState(state, null, null);
        }
    }

    @Override
    public Map<ExecutionState, Long> countActiveByState(Instant now) {
        Objects.requireNonNull(now, "now");
        Map<ExecutionState, Long> counts = new LinkedHashMap<>();
        jdbcTemplate.query(delegate.countActiveInQueue(),
                new MapSqlParameterSource("now", delegate.splitTimestamp(now)), rs -> {
            long waiting = rs.getLong("waiting");
            counts.put(ExecutionState.ENQUEUED, rs.getLong("queued") - waiting);
            counts.put(ExecutionState.RETRY_WAITING, waiting);
        });
        Long running = jdbcTemplate.queryForObject(delegate.countRunning(), new MapSqlParameterSource(), Long.class);
        counts.put(ExecutionState.RUNNING, running == null ? 0L : running);
        return counts;
    }

    @Override
    public Map<ExecutionState, Long> countTerminalOutcomesSince(Instant since) {
        Objects.requireNonNull(since, "since");
        Map<ExecutionState, Long> counts = new LinkedHashMap<>();
        jdbcTemplate.query(delegate.countTerminalOutcomesSince(),
                new MapSqlParameterSource("since", delegate.splitTimestamp(since)), rs -> {
            counts.put(ExecutionState.valueOf(rs.getString("outcome")), rs.getLong("finished"));
        });
        return counts;
    }

    private Attempt mapAttempt(ResultSet rs) throws SQLException {
        return new Attempt(
                rs.getInt("last_attempt_number"),
                Objects.requireNonNull(delegate.readSplitTimestamp(rs, "last_attempt_started_at"), "last_attempt_started_at"),
                delegate.readSplitTimestamp(rs, "last_attempt_finished_at"),
                ExecutionState.valueOf(rs.getString("last_attempt_outcome")),
                rs.getString("last_attempt_error"));
    }
}
