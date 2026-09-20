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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.BatchCounters;
import io.mohs.engine.LeaseStore;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * Fenced completion and recovery over LEASED mohs_work. Latest attempt, terminal execution,
 * batch counters and matching fixed-delay rearm commit with the work transition.
 */
public final class JdbcLeaseStore implements LeaseStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    /** The reads the engine's loop thread issues — the reaper's, the reconcile's, the cancel poll's, the admission's. */
    private final NamedParameterJdbcTemplate tickTemplate;
    private final TransactionTemplate transactionTemplate;
    private final JdbcDelegate delegate;

    /**
     * Creates a {@code JdbcLeaseStore} with the supplied values.
     *
     * @param dataSource the configured database connection source
     * @param delegate the database-specific SQL and timestamp adapter
     */
    public JdbcLeaseStore(DataSource dataSource, JdbcDelegate delegate) {
        Objects.requireNonNull(dataSource, "dataSource");
        // Transactions bound writes; the tick template also bounds reads outside transactions.
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.tickTemplate = JdbcSupport.tickTemplate(dataSource);
        this.transactionTemplate = completionTransaction(dataSource);
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /** Every completion has a deadline, including the flusher: session locks must not wait indefinitely. */
    private static TransactionTemplate completionTransaction(DataSource dataSource) {
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // A completion is ALWAYS its own transaction — see the REQUIRES_NEW comment in JdbcWorkQueue
        // (same reason, same hazard of silently inherited isolation)
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setTimeout(JdbcSupport.TICK_STATEMENT_TIMEOUT_SECONDS);
        // Row locks and conditional mutations arbitrate ownership at READ COMMITTED.
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        return transaction;
    }

    @Override
    public Map<ExecutionId, Completion> complete(List<CompletionResult> results) {
        if (results.isEmpty()) {
            return Map.of();
        }
        // requireNonNull documents the invariant for @NullMarked
        return Objects.requireNonNull(transactionTemplate.execute(_ -> completeWithinTransaction(results, false)));
    }

    @Override
    public Map<ExecutionId, Completion> reclaim(List<CompletionResult> results) {
        if (results.isEmpty()) {
            return Map.of();
        }
        return Objects.requireNonNull(transactionTemplate.execute(_ -> completeWithinTransaction(results, true)));
    }

    private Map<ExecutionId, Completion> completeWithinTransaction(List<CompletionResult> results,
            boolean recovery) {
        if (results.stream().map(CompletionResult::executionId).distinct().count() != results.size()) {
            throw new IllegalArgumentException("a completion batch must contain at most one result per execution");
        }
        var sessions = new JdbcSessions(jdbcTemplate, delegate);
        Set<String> authorized = new LinkedHashSet<>();
        for (String nodeId : results.stream().map(CompletionResult::nodeId).distinct().sorted().toList()) {
            var session = sessions.lock(nodeId);
            if (session.isPresent() && (recovery ? session.orElseThrow().revoked() : sessions.active(session.orElseThrow()))) {
                authorized.add(nodeId);
            }
        }
        // Lock ordering (JCIP ch. 10, applied to row locks): a flusher (arrival order) and a reaper
        // (claimed_at) running concurrently over overlapping sets — exactly zombie versus reclaim — would
        // lock rows in opposite orders; the canonical order by executionId eliminates the whole class of
        // deadlock
        List<CompletionResult> ordered = results.stream().filter(result -> authorized.contains(result.nodeId()))
                .sorted(Comparator.comparing(result -> result.executionId().value()))
                .toList();
        var transitions = new JdbcExecutionTransitions(jdbcTemplate, delegate);
        var metadata = transitions.metadata(ordered.stream().map(CompletionResult::executionId).toList());
        transitions.lockJobs(metadata.values().stream().map(JdbcExecutionTransitions.Metadata::jobKey).toList());
        var retired = transitions.retiredJobs(metadata.values().stream().map(JdbcExecutionTransitions.Metadata::jobKey).toList());
        var cancelled = transitions.cancellationRequests(ordered.stream().map(CompletionResult::executionId).toList());
        ordered = ordered.stream().map(result -> cancelled.contains(result.executionId()) ? result.honourCancellation()
                : retired.contains(result.jobKey()) ? result.honourRetirement() : result).toList();
        List<CompletionResult> winners = transitionWorkKeepingFenceWinners(ordered);
        Map<ExecutionId, Completion> verdicts = LinkedHashMap.newLinkedHashMap(results.size());
        for (CompletionResult result : results) {
            verdicts.put(result.executionId(), Completion.FENCED_OUT);
        }
        var winningIds = winners.stream().map(CompletionResult::executionId).collect(java.util.stream.Collectors.toSet());
        Map<String, CompletionResult> unowned = new LinkedHashMap<>();
        results.stream().filter(result -> !winningIds.contains(result.executionId()))
                .forEach(result -> unowned.put(result.executionId().value(), result));
        for (List<String> ids : JdbcSupport.chunksOf(List.copyOf(unowned.keySet()))) {
            jdbcTemplate.query("SELECT execution_id, last_claim_token FROM mohs_execution WHERE execution_id IN (:ids)",
                    new MapSqlParameterSource("ids", ids), rs -> {
                var result = unowned.get(rs.getString("execution_id"));
                if (result.claimToken().equals(rs.getString("last_claim_token"))) {
                    verdicts.put(result.executionId(), Completion.ALREADY_APPLIED);
                }
            });
        }
        if (winners.isEmpty()) {
            return verdicts;
        }
        for (var winner : winners) {
            var stored = Objects.requireNonNull(metadata.get(winner.executionId()), "leased execution has no history");
            if (!stored.jobKey().equals(winner.jobKey())) {
                throw new IllegalArgumentException("completion job differs from the accepted execution");
            }
        }
        completeExecutions(winners);
        // Job locks precede work/history locks, and batch counters are updated in a stable order.
        for (var winner : winners) {
            if (winner.terminalState() != null && metadata.get(winner.executionId()).scheduled()) {
                transitions.rearmTerminal(winner.executionId(), winner.finishedAt());
            }
        }
        var closedBatches = countIntoBatches(winners, metadata, transitions);
        for (CompletionResult winner : winners) {
            verdicts.put(winner.executionId(), Completion.owned(winner,
                    closedBatches.get(winner.executionId())));
        }
        return verdicts;
    }

    /**
     * Only terminal winners count; cancelled members count as failures. Aggregate increments retain
     * the batch lock through the counters read, so exactly one transaction can elect its closer.
     */
    private Map<ExecutionId, BatchCounters> countIntoBatches(List<CompletionResult> winners,
            Map<ExecutionId, JdbcExecutionTransitions.Metadata> metadata, JdbcExecutionTransitions transitions) {
        var groups = new java.util.TreeMap<String, List<CompletionResult>>();
        for (var winner : winners) {
            String batchId = metadata.get(winner.executionId()).batchId();
            if (batchId != null && winner.terminalState() != null) {
                groups.computeIfAbsent(batchId, _ -> new ArrayList<>()).add(winner);
            }
        }
        Map<ExecutionId, BatchCounters> closed = new LinkedHashMap<>();
        for (var group : groups.entrySet()) {
            var members = group.getValue();
            long succeeded = members.stream().filter(result -> result.terminalState() == ExecutionState.SUCCEEDED).count();
            BatchCounters counters = transitions.completeBatchMembers(group.getKey(), succeeded, members.size() - succeeded);
            // Winners arrive in execution-id order. The same last terminal member closes the batch
            // as with individual increments, while the locked counters are read only once per batch.
            if (counters.pending() == 0) closed.put(members.getLast().executionId(), counters);
        }
        return closed;
    }

    private List<CompletionResult> transitionWorkKeepingFenceWinners(List<CompletionResult> results) {
        List<CompletionResult> winners = new ArrayList<>(results.size());
        // Every supported active-work writer first locks its jobs in canonical order.
        // Overlapping execution sets therefore serialize before reaching these statements;
        // disjoint job sets cannot share an execution. Keep order within each SQL batch.
        for (boolean retry : new boolean[]{false, true}) {
            var group = results.stream().filter(result -> (result.retryAt() != null) == retry).toList();
            if (group.isEmpty()) continue;
            int[] changed = jdbcTemplate.batchUpdate(retry ? delegate.fencedReleaseWork() : delegate.fencedLeaseDelete(),
                    group.stream().map(result -> {
                        var params = JdbcSupport.fencedLeaseDeleteParams(result.executionId().value(), result.nodeId(),
                                result.claimToken(), result.attemptNumber());
                        if (retry) params.addValue("nextAttempt", Math.addExact(result.attemptNumber(), 1))
                                .addValue("visibleAt", delegate.splitTimestamp(Objects.requireNonNull(result.retryAt())))
                                .addValue("cancelRequested", false);
                        return params;
                    }).toArray(MapSqlParameterSource[]::new));
            for (int i = 0; i < changed.length; i++) {
                if (changed[i] == Statement.SUCCESS_NO_INFO) throw new IllegalStateException(
                        "driver returned SUCCESS_NO_INFO for fenced work transition; exact counts are required");
                if (changed[i] == 1) winners.add(group.get(i));
            }
        }
        winners.sort(Comparator.comparing(result -> result.executionId().value()));
        return winners;
    }

    private void completeExecutions(List<CompletionResult> winners) {
        jdbcTemplate.batchUpdate(delegate.completeExecution(), winners.stream()
                .map(this::completionParams)
                .toArray(MapSqlParameterSource[]::new));
    }

    private MapSqlParameterSource completionParams(CompletionResult result) {
        return new MapSqlParameterSource()
                .addValue("executionId", result.executionId().value())
                .addValue("number", result.attemptNumber())
                .addValue("nodeId", result.nodeId())
                .addValue("claimToken", result.claimToken())
                .addValue("startedAt", delegate.splitTimestamp(result.startedAt()))
                .addValue("finishedAt", delegate.splitTimestamp(result.finishedAt()))
                .addValue("outcome", result.outcome().name())
                .addValue("errorType", result.errorType())
                .addValue("error", result.error())
                .addValue("terminalState", result.terminalState() == null ? null : result.terminalState().name(), java.sql.Types.VARCHAR)
                .addValue("executionFinishedAt", result.terminalState() == null ? null : delegate.splitTimestamp(result.finishedAt()));
    }



    @Override
    public List<Lease> findByNodes(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return List.of();
        }
        List<Lease> leases = new ArrayList<>();
        for (List<String> chunk : JdbcSupport.chunksOf(List.copyOf(nodeIds))) {
            leases.addAll(tickTemplate.query(delegate.findLeasesByNodes(),
                    new MapSqlParameterSource("nodeIds", chunk), (rs, _) -> mapLease(rs)));
        }
        return leases;
    }

    @Override
    public List<Lease> findByNodePage(String nodeId, @Nullable LeaseCursor after, int limit) {
        io.mohs.core.GeneratedId.require(nodeId, "nodeId");
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("grant page limit must be in [1, 1000]");
        Instant time = after == null ? Instant.parse("1000-01-01T00:00:00Z") : after.claimedAt();
        return tickTemplate.query(delegate.nodeLeasePage(), new MapSqlParameterSource("nodeId", nodeId)
                .addValue("afterTime", delegate.splitTimestamp(time))
                .addValue("afterId", after == null ? "" : after.executionId().value())
                .addValue("limit", (long) limit, java.sql.Types.BIGINT), (rs, _) -> mapLease(rs));
    }

    @Override
    public List<Lease> findOrphaned(int limit) {
        return tickTemplate.query(delegate.findOrphanedLeases(),
                new MapSqlParameterSource("limit", (long) limit).addValue("revoked", true), (rs, _) -> mapLease(rs));
    }

    @Override
    public Set<ExecutionId> findCancelRequested(Collection<ExecutionId> ids) {
        if (ids.isEmpty()) {
            return Set.of();
        }
        Set<ExecutionId> flagged = new LinkedHashSet<>();
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            flagged.addAll(tickTemplate.query(delegate.findCancelRequestedLeases(),
                    new MapSqlParameterSource().addValue("ids", chunk).addValue("flag", true),
                    (rs, _) -> ExecutionId.of(rs.getString("execution_id"))));
        }
        return flagged;
    }

    private Lease mapLease(ResultSet rs) throws SQLException {
        return new Lease(
                ExecutionId.of(rs.getString("execution_id")),
                JobKey.of(rs.getString("job_key")),
                rs.getString("node_id"),
                rs.getString("claim_token"),
                rs.getInt("attempt"),
                rs.getInt("priority"),
                Objects.requireNonNull(delegate.readSplitTimestamp(rs, "claimed_at"), "claimed_at"),
                rs.getBoolean("cancel_requested"));
    }
}
