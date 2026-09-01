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
import io.mohs.engine.BatchStore;
import io.mohs.engine.JobStore;
import io.mohs.engine.LeaseStore;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/**
 * {@link LeaseStore} over {@code mohs_lease}.
 *
 * <p>The completion is the transaction that performs: a {@code DELETE} fenced by
 * {@code (node_id, epoch)} — deleting the lease IS releasing the slot, so there is no counter left to
 * decrement — an {@code INSERT} of the confirmed attempts, the advisory terminal {@code UPDATE} of
 * history (matched by the primary key, {@code execution_id}) and, for non-terminal results, the rebirth
 * in the queue in the SAME transaction (see {@link LeaseStore.CompletionResult#retry}'s Javadoc).
 */
public final class JdbcLeaseStore implements LeaseStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final JdbcDelegate delegate;
    private final BatchStore batchStore;

    public JdbcLeaseStore(DataSource dataSource, JdbcDelegate delegate, BatchStore batchStore) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // A completion is ALWAYS its own transaction — see the REQUIRES_NEW comment in JdbcWorkQueue
        // (same reason, same hazard of silently inherited isolation)
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        // The fence assumes "last write wins" under an explicit READ COMMITTED
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
    }

    @Override
    public Map<ExecutionId, Completion> complete(List<CompletionResult> results, JobStore jobStore) {
        Objects.requireNonNull(jobStore, "jobStore");
        if (results.isEmpty()) {
            return Map.of();
        }
        // requireNonNull documents the invariant for @NullMarked
        return Objects.requireNonNull(transactionTemplate.execute(_ -> completeWithinTransaction(results, jobStore)));
    }

    private Map<ExecutionId, Completion> completeWithinTransaction(List<CompletionResult> results, JobStore jobStore) {
        // Lock ordering (JCIP ch. 10, applied to row locks): a flusher (arrival order) and a reaper
        // (claimed_at) running concurrently over overlapping sets — exactly zombie versus reclaim — would
        // lock rows in opposite orders; the canonical order by executionId eliminates the whole class of
        // deadlock
        List<CompletionResult> ordered = results.stream()
                .sorted(Comparator.comparing(result -> result.executionId().value()))
                .toList();
        List<CompletionResult> winners = deleteLeasesKeepingFenceWinners(ordered);
        Map<ExecutionId, Completion> verdicts = LinkedHashMap.newLinkedHashMap(results.size());
        for (CompletionResult result : results) {
            verdicts.put(result.executionId(), Completion.FENCED_OUT);
        }
        if (winners.isEmpty()) {
            return verdicts;
        }
        insertAttempts(winners);
        updateTerminalStates(winners);
        offerRetries(winners);
        for (CompletionResult winner : winners) {
            if (winner.rearmNextFireAt() != null) {
                // The rearm lands in the SAME transaction — jobStore takes part by sharing the DataSource;
                // the IS NULL guard is its own
                jobStore.armNextFire(winner.jobKey(), winner.rearmNextFireAt());
            }
            verdicts.put(winner.executionId(), Completion.owned(countIntoBatch(winner)));
        }
        return verdicts;
    }

    /**
     * The batch contract intact on the new layout: a member only counts when it FINISHES (a retry does
     * not count — it would close the batch early), a cancelled one counts as a failure (the batch answers
     * "how many succeeded"), and the closer is elected by THIS transaction's balance, never by a re-read.
     */
    private @Nullable BatchCounters countIntoBatch(CompletionResult result) {
        if (result.batchId() == null || result.terminalState() == null) {
            return null;
        }
        BatchCounters counters = result.terminalState() == ExecutionState.SUCCEEDED
                ? batchStore.incrementSucceeded(result.batchId())
                : batchStore.incrementFailed(result.batchId());
        return counters.pending() == 0 ? counters : null;
    }

    private List<CompletionResult> deleteLeasesKeepingFenceWinners(List<CompletionResult> results) {
        int[] deleted = jdbcTemplate.batchUpdate(delegate.fencedLeaseDelete(), results.stream()
                .map(r -> JdbcSupport.fencedLeaseDeleteParams(r.executionId().value(), r.nodeId(), r.epoch()))
                .toArray(MapSqlParameterSource[]::new));
        List<CompletionResult> winners = new ArrayList<>(results.size());
        for (int i = 0; i < deleted.length; i++) {
            if (deleted[i] == Statement.SUCCESS_NO_INFO) {
                // Without a count there is no way to know what the fence discarded — and a presence-based
                // fallback is ambiguous (an absent row may be OUR win or somebody else's already completed
                // re-claim). No driver of the four databases does this for a DELETE; if one starts to, the
                // per-row path gets implemented deliberately, not improvised.
                throw new IllegalStateException("driver returned SUCCESS_NO_INFO for the fenced lease delete batch — "
                        + "completion cannot tell fence winners apart; implement the per-row path for this driver");
            }
            if (deleted[i] == 1) {
                winners.add(results.get(i));
            }
        }
        return winners;
    }

    private void insertAttempts(List<CompletionResult> winners) {
        jdbcTemplate.batchUpdate(delegate.insertAttempt(), winners.stream()
                .map(this::attemptParams)
                .toArray(MapSqlParameterSource[]::new));
    }

    private MapSqlParameterSource attemptParams(CompletionResult result) {
        return new MapSqlParameterSource()
                .addValue("executionId", result.executionId().value())
                .addValue("number", result.attemptNumber())
                .addValue("nodeId", result.nodeId())
                .addValue("startedAt", delegate.splitTimestamp(result.startedAt()))
                .addValue("finishedAt", delegate.splitTimestamp(result.finishedAt()))
                .addValue("outcome", result.outcome().name())
                .addValue("errorType", result.errorType())
                .addValue("error", result.error());
    }

    private void updateTerminalStates(List<CompletionResult> winners) {
        MapSqlParameterSource[] updates = winners.stream()
                .filter(r -> r.terminalState() != null)
                .map(this::terminalUpdateParams)
                .toArray(MapSqlParameterSource[]::new);
        if (updates.length > 0) {
            jdbcTemplate.batchUpdate(delegate.terminalStateUpdate(), updates);
        }
    }

    private MapSqlParameterSource terminalUpdateParams(CompletionResult result) {
        return new MapSqlParameterSource()
                .addValue("state", Objects.requireNonNull(result.terminalState()).name())
                .addValue("finishedAt", delegate.splitTimestamp(result.finishedAt()))
                .addValue("executionId", result.executionId().value());
    }

    private void offerRetries(List<CompletionResult> winners) {
        MapSqlParameterSource[] retries = winners.stream()
                .filter(r -> r.retry() != null)
                .map(r -> JdbcSupport.readyEntryParams(Objects.requireNonNull(r.retry()), delegate))
                .toArray(MapSqlParameterSource[]::new);
        if (retries.length > 0) {
            jdbcTemplate.batchUpdate(delegate.readyInsert(), retries);
        }
    }

    @Override
    public List<Lease> findByNodes(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return List.of();
        }
        List<Lease> leases = new ArrayList<>();
        for (List<String> chunk : JdbcSupport.chunksOf(List.copyOf(nodeIds))) {
            leases.addAll(jdbcTemplate.query(delegate.findLeasesByNodes(),
                    new MapSqlParameterSource("nodeIds", chunk), (rs, _) -> mapLease(rs)));
        }
        return leases;
    }

    @Override
    public List<Lease> findOrphaned(Collection<String> aliveNodeIds, int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("limit", limit);
        String sql;
        if (aliveNodeIds.isEmpty()) {
            sql = delegate.findOrphanedLeases();
        } else {
            sql = delegate.findOrphanedLeasesExceptAlive();
            params.addValue("aliveNodeIds", aliveNodeIds);
        }
        return jdbcTemplate.query(sql, params, (rs, _) -> mapLease(rs));
    }

    @Override
    public Map<JobKey, Integer> countByJob(Collection<JobKey> jobKeys) {
        if (jobKeys.isEmpty()) {
            return Map.of();
        }
        // Chunked for the same reason as findCancelRequested (the asymmetry between the two was an
        // accident): jobKeys is every definition with allowConcurrentExecutions=false, with no ceiling.
        // Above ~2100 on SQL Server, Admission.compute threw — and since the claim runs in the tick's
        // same try, the node stayed alive, heartbeating and claiming nothing
        Map<JobKey, Integer> counts = LinkedHashMap.newLinkedHashMap(jobKeys.size());
        for (List<String> chunk : JdbcSupport.chunksOf(jobKeys.stream().map(JobKey::value).toList())) {
            jdbcTemplate.query(delegate.countLeasesByJob(), new MapSqlParameterSource("jobKeys", chunk),
                    rs -> {
                        counts.put(JobKey.of(rs.getString("job_key")), rs.getInt("leases"));
                    });
        }
        return counts;
    }

    @Override
    public boolean requestCancellation(ExecutionId id) {
        Objects.requireNonNull(id, "id");
        return jdbcTemplate.update(delegate.requestLeaseCancellation(),
                new MapSqlParameterSource().addValue("flag", true).addValue("executionId", id.value())) == 1;
    }

    @Override
    public Set<ExecutionId> findCancelRequested(Collection<ExecutionId> ids) {
        if (ids.isEmpty()) {
            return Set.of();
        }
        Set<ExecutionId> flagged = new LinkedHashSet<>();
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            flagged.addAll(jdbcTemplate.query(delegate.findCancelRequestedLeases(),
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
                rs.getLong("epoch"),
                rs.getInt("attempt_number"),
                rs.getInt("priority"),
                Objects.requireNonNull(delegate.readSplitTimestamp(rs, "claimed_at"), "claimed_at"),
                rs.getBoolean("cancel_requested"));
    }
}
