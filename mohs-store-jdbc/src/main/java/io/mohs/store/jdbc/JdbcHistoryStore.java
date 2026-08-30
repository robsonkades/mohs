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
import org.springframework.util.ClassUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.engine.HistoryStore;
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * {@link HistoryStore} over {@code mohs_execution}/{@code mohs_attempt}/{@code mohs_idempotency}.
 *
 * <p>Deliberately WITHOUT a {@code TransactionTemplate}: {@link #record} takes part in the caller's
 * active transaction (the enqueue joins the host's; execution, queue and idempotency are the same unit
 * of work, composed by the caller and never here); the reads are autocommit like any store's. The
 * terminal write does NOT live here — it belongs to the completion transaction
 * ({@code JdbcLeaseStore.complete}).
 */
public final class JdbcHistoryStore implements HistoryStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate pruneTemplate;
    private final JsonMapper objectMapper;
    private final JdbcDialect dialect;

    /**
     * Shorter than any sensible {@code node-lease-ttl}, so a prune waiting on another node's locks
     * can never cost this node the lease it is supposed to be renewing. It is a ceiling on waiting,
     * not a budget for working — see {@link #pruneIdempotencyBefore}.
     */
    private static final int PRUNE_TIMEOUT_SECONDS = 5;

    public JdbcHistoryStore(DataSource dataSource, JsonMapper objectMapper, JdbcDialect dialect) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        // A template of its own: the timeout must not reach the read model or the payload read, which
        // run on the hot path and have no business being cancelled mid-flight
        JdbcTemplate pruneOperations = new JdbcTemplate(dataSource);
        pruneOperations.setQueryTimeout(PRUNE_TIMEOUT_SECONDS);
        this.pruneTemplate = new NamedParameterJdbcTemplate(pruneOperations);
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    private static final String RECORD = """
            INSERT INTO mohs_execution (
                execution_id, job_key, shard, priority, state, scheduled_at, created_at, actor,
                correlation_id, idempotency_key, payload, payload_type)
            VALUES (:executionId, :jobKey, :shard, :priority, 'PENDING', :scheduledAt, :createdAt, :actor,
                :correlationId, :idempotencyKey, :payload, :payloadType)
            """;

    private static final String IDEMPOTENCY_INSERT = """
            INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at)
            VALUES (:jobKey, :idempotencyKey, :executionId, :createdAt)
            """;

    @Override
    public void record(List<NewExecution> executions) {
        if (executions.isEmpty()) {
            return;
        }
        // Idempotency goes in BEFORE history: the primary-key conflict IS the deduplication check
        // (Idempotent Receiver, EIP) and has to abort the whole unit before any history row is born
        MapSqlParameterSource[] keys = executions.stream()
                .filter(e -> e.idempotencyKey() != null)
                .map(this::idempotencyParams)
                .toArray(MapSqlParameterSource[]::new);
        if (keys.length > 0) {
            jdbcTemplate.batchUpdate(IDEMPOTENCY_INSERT, keys);
        }
        jdbcTemplate.batchUpdate(RECORD, executions.stream()
                .map(this::recordParams)
                .toArray(MapSqlParameterSource[]::new));
    }

    private MapSqlParameterSource idempotencyParams(NewExecution execution) {
        return new MapSqlParameterSource()
                .addValue("jobKey", execution.jobKey().value())
                .addValue("idempotencyKey", execution.idempotencyKey())
                .addValue("executionId", execution.executionId().value())
                .addValue("createdAt", dialect.splitTimestamp(execution.createdAt()));
    }

    private MapSqlParameterSource recordParams(NewExecution execution) {
        return new MapSqlParameterSource()
                .addValue("executionId", execution.executionId().value())
                .addValue("jobKey", execution.jobKey().value())
                .addValue("shard", execution.shard())
                .addValue("priority", execution.priority())
                .addValue("scheduledAt", dialect.splitTimestamp(execution.scheduledAt()))
                .addValue("createdAt", dialect.splitTimestamp(execution.createdAt()))
                .addValue("actor", execution.actor())
                .addValue("correlationId", execution.correlationId())
                .addValue("idempotencyKey", execution.idempotencyKey())
                .addValue("payload", writePayload(execution.payload()))
                .addValue("payloadType", execution.payload().getClass().getName());
    }

    private String writePayload(Object payload) {
        return objectMapper.writeValueAsString(payload);
    }

    @Override
    public Optional<ExecutionId> findByIdempotencyKey(JobKey jobKey, String idempotencyKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        return JdbcSupport.findOne(jdbcTemplate, """
                SELECT execution_id FROM mohs_idempotency
                WHERE job_key = :jobKey AND idempotency_key = :idempotencyKey
                """,
                new MapSqlParameterSource().addValue("jobKey", jobKey.value()).addValue("idempotencyKey", idempotencyKey),
                rs -> ExecutionId.of(rs.getString("execution_id")));
    }

    private static final String HEAD_COLUMNS =
            "execution_id, job_key, scheduled_at, created_at, actor, priority, correlation_id";

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
            jdbcTemplate.query("SELECT " + HEAD_COLUMNS + ", payload, payload_type FROM mohs_execution WHERE execution_id IN (:ids)",
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
            heads.addAll(jdbcTemplate.query("SELECT " + HEAD_COLUMNS + " FROM mohs_execution WHERE execution_id IN (:ids)",
                    new MapSqlParameterSource("ids", chunk), (rs, _) -> mapHead(rs)));
        }
        return heads;
    }

    private ExecutionHead mapHead(ResultSet rs) throws SQLException {
        return new ExecutionHead(
                ExecutionId.of(rs.getString("execution_id")),
                JobKey.of(rs.getString("job_key")),
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "scheduled_at"), "scheduled_at"),
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "created_at"), "created_at"),
                rs.getString("actor"),
                rs.getInt("priority"),
                rs.getString("correlation_id"));
    }

    private Object readPayload(ResultSet rs) throws SQLException {
        String payloadJson = rs.getString("payload");
        String payloadType = rs.getString("payload_type");
        Class<?> type;
        try {
            type = ClassUtils.forName(payloadType, ClassUtils.getDefaultClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("payload type '" + payloadType + "' not found on classpath", e);
        }
        return objectMapper.readValue(payloadJson, type);
    }

    @Override
    public List<Attempt> findAttempts(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        return jdbcTemplate.query("""
                SELECT number, started_at, finished_at, outcome, error FROM mohs_attempt
                WHERE execution_id = :executionId ORDER BY number
                """, new MapSqlParameterSource("executionId", executionId.value()), (rs, _) -> mapAttempt(rs));
    }

    /**
     * Bounded in TIME, not in rows, and the bound is the point: this runs on the engine's loop
     * thread, ahead of the firing and the claim. Every node issues the same DELETE, so the node that
     * gets there second waits on the first one's row locks — and a node blocked here is a node that
     * is heartbeating, owning its share of the shards, and claiming nothing. That is the exact
     * failure mode the tick's own comment describes, and {@code runMaintenance} does not shield
     * against it: it catches exceptions, and waiting throws nothing.
     *
     * <p>A five-second ceiling ({@code PRUNE_TIMEOUT_SECONDS}) therefore caps the wait rather than
     * the work. A prune cut short rolls
     * back whole and makes no progress, which is the right trade at this cadence — the loser of the
     * race would have deleted nothing anyway, since the winner is removing the same rows. What it
     * costs is the pathological case: a first prune over a window that was never enforced can be
     * large enough to time out on every node, forever. That surfaces as
     * {@code mohs.tick.failed{step=idempotency-prune}} climbing hourly, and the answer is a one-off
     * manual DELETE in batches — not a longer timeout, which would only move the stall into the
     * claim.
     */
    @Override
    public int pruneIdempotencyBefore(Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        return pruneTemplate.update("DELETE FROM mohs_idempotency WHERE created_at < :cutoff",
                new MapSqlParameterSource("cutoff", dialect.splitTimestamp(cutoff)));
    }

    private static final String READ_MODEL_COLUMNS = """
            e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
            e.correlation_id, e.idempotency_key,
            l.node_id AS lease_node, l.claimed_at AS lease_claimed_at,
            r.execution_id AS ready_id, r.attempt AS ready_attempt, r.visible_at AS ready_visible_at
            """;

    private static final String READ_MODEL_FROM = """
            FROM mohs_execution e
            LEFT JOIN mohs_lease l ON l.execution_id = e.execution_id
            LEFT JOIN mohs_ready r ON r.execution_id = e.execution_id
            """;

    @Override
    public Optional<Execution> find(ExecutionId id, Instant now) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(now, "now");
        Optional<Execution> summary = JdbcSupport.findOne(jdbcTemplate,
                "SELECT " + READ_MODEL_COLUMNS + READ_MODEL_FROM + "WHERE e.execution_id = :executionId",
                new MapSqlParameterSource("executionId", id.value()),
                rs -> mapDerived(rs, now, List.of()));
        // The detail view carries attempts (the earlier era's contract preserved); two reads, not an NxM
        // join — attempts would multiply the row
        return summary.map(execution -> withAttempts(execution, findAttempts(id)));
    }

    private static Execution withAttempts(Execution execution, List<Attempt> attempts) {
        return new Execution(execution.id(), execution.jobKey(), execution.state(), execution.scheduledAt(),
                execution.firedAt(), attempts, execution.actor(), execution.priority(), execution.idempotencyKey(),
                execution.batchId(), execution.owner());
    }

    @Override
    public List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit, Instant now) {
        Objects.requireNonNull(now, "now");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("limit", limit)
                .addValue("now", dialect.splitTimestamp(now));
        List<String> conditions = new ArrayList<>();
        if (jobKey != null) {
            conditions.add("e.job_key = :jobKey");
            params.addValue("jobKey", jobKey.value());
        }
        if (from != null) {
            conditions.add("e.scheduled_at >= :from");
            params.addValue("from", dialect.splitTimestamp(from));
        }
        if (to != null) {
            conditions.add("e.scheduled_at <= :to");
            params.addValue("to", dialect.splitTimestamp(to));
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
                case RUNNING -> conditions.add("l.node_id IS NOT NULL");
                case RETRY_WAITING -> conditions.add("r.attempt > 1 AND r.visible_at > :now");
                case ENQUEUED -> conditions.add("r.execution_id IS NOT NULL AND (r.attempt = 1 OR r.visible_at <= :now)");
            }
        }
        String where = conditions.isEmpty() ? "" : "WHERE " + String.join(" AND ", conditions) + "\n";
        String sql = "SELECT " + dialect.topClause() + READ_MODEL_COLUMNS + READ_MODEL_FROM + where
                + "ORDER BY e.execution_id DESC " + dialect.limitClause();
        // A SUMMARY by contract (the earlier era preserved): empty attempts in the listing
        return jdbcTemplate.query(sql, params, (rs, _) -> mapDerived(rs, now, List.of()));
    }

    /** The derivation — see {@link HistoryStore#find}'s Javadoc. */
    private Execution mapDerived(ResultSet rs, Instant now, List<Attempt> attempts) throws SQLException {
        ExecutionId id = ExecutionId.of(rs.getString("execution_id"));
        String column = rs.getString("state");
        String leaseNode = rs.getString("lease_node");
        boolean queued = rs.getString("ready_id") != null;

        ExecutionState state;
        String owner = null;
        Instant firedAt = null;
        if (!"PENDING".equals(column)) {
            state = ExecutionState.valueOf(column);
        } else if (leaseNode != null) {
            state = ExecutionState.RUNNING;
            owner = leaseNode;
            firedAt = dialect.readSplitTimestamp(rs, "lease_claimed_at");
        } else if (queued && rs.getInt("ready_attempt") > 1
                && Objects.requireNonNull(dialect.readSplitTimestamp(rs, "ready_visible_at"), "ready_visible_at").isAfter(now)) {
            state = ExecutionState.RETRY_WAITING;
        } else {
            // queued and visible — or inside the window of a completion flush in
            // progress (an orphan PENDING): ENQUEUED, the read model's accepted staleness
            state = ExecutionState.ENQUEUED;
        }
        return new Execution(id, JobKey.of(rs.getString("job_key")), state,
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "scheduled_at"), "scheduled_at"),
                firedAt, attempts, rs.getString("actor"), Priority.fromValue(rs.getInt("priority")),
                rs.getString("idempotency_key"), rs.getString("correlation_id"), owner);
    }

    @Override
    public Map<ExecutionState, Long> countActiveByState(Instant now) {
        Objects.requireNonNull(now, "now");
        Map<ExecutionState, Long> counts = new LinkedHashMap<>();
        // Two small queries, with a cost equal to the live work by construction: the queue IS the backlog
        // and the ownership IS what is executing — history does not enter
        jdbcTemplate.query("""
                SELECT COUNT(*) AS queued,
                       SUM(CASE WHEN attempt > 1 AND visible_at > :now THEN 1 ELSE 0 END) AS waiting
                FROM mohs_ready
                """, new MapSqlParameterSource("now", dialect.splitTimestamp(now)), rs -> {
            long waiting = rs.getLong("waiting");
            counts.put(ExecutionState.ENQUEUED, rs.getLong("queued") - waiting);
            counts.put(ExecutionState.RETRY_WAITING, waiting);
        });
        Long running = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_lease", new MapSqlParameterSource(), Long.class);
        counts.put(ExecutionState.RUNNING, running == null ? 0L : running);
        return counts;
    }

    @Override
    public Map<ExecutionState, Long> countTerminalOutcomesSince(Instant since) {
        Objects.requireNonNull(since, "since");
        Map<ExecutionState, Long> counts = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT outcome, COUNT(*) AS finished FROM mohs_attempt
                WHERE finished_at >= :since AND outcome IN ('SUCCEEDED', 'FAILED')
                GROUP BY outcome
                """, new MapSqlParameterSource("since", dialect.splitTimestamp(since)), rs -> {
            counts.put(ExecutionState.valueOf(rs.getString("outcome")), rs.getLong("finished"));
        });
        return counts;
    }

    private Attempt mapAttempt(ResultSet rs) throws SQLException {
        return new Attempt(
                rs.getInt("number"),
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "started_at"), "started_at"),
                dialect.readSplitTimestamp(rs, "finished_at"),
                ExecutionState.valueOf(rs.getString("outcome")),
                rs.getString("error"));
    }
}
