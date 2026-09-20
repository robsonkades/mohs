/*
 * Copyright 2026 The Mohs Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package io.mohs.store.jdbc;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;
import io.mohs.engine.BatchCounters;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.store.jdbc.delegate.JdbcDelegate;

/** Shared rules for multi-table transitions; callers own the transaction and acquire jobs first. */
final class JdbcExecutionTransitions {
    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcDelegate dialect;

    JdbcExecutionTransitions(NamedParameterJdbcTemplate jdbc, JdbcDelegate dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    Optional<JobKey> jobOf(ExecutionId id) {
        return jdbc.query("SELECT job_key FROM mohs_execution WHERE execution_id = :executionId",
                new MapSqlParameterSource("executionId", id.value()),
                (rs, _) -> JobKey.of(rs.getString(1))).stream().findFirst();
    }

    void lockJobs(Collection<JobKey> keys) {
        for (String key : keys.stream().map(JobKey::value).distinct().sorted().toList()) {
            jdbc.query(dialect.lockBindingOwner(), new MapSqlParameterSource("jobKey", key), rs -> {
                while (rs.next()) {
                    // Exhaust the locking projection; the row values are read after acquiring the lock.
                }
                return Boolean.TRUE;
            });
        }
    }

    Set<ExecutionId> cancellationRequests(Collection<ExecutionId> ids) {
        Set<ExecutionId> cancelled = new LinkedHashSet<>();
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            cancelled.addAll(jdbc.query(dialect.findCancelRequestedLeases(),
                    new MapSqlParameterSource("ids", chunk).addValue("flag", true),
                    (rs, _) -> ExecutionId.of(rs.getString("execution_id"))));
        }
        return cancelled;
    }

    /** Jobs and work are already locked. History is changed before batches, whose keys are sorted. */
    void finishCancellations(Map<ExecutionId, Instant> cancelled) {
        if (cancelled.isEmpty()) return;
        var batchIds = new java.util.ArrayList<String>();
        for (var entry : cancelled.entrySet().stream().sorted(Map.Entry.comparingByKey(
                java.util.Comparator.comparing(ExecutionId::value))).toList()) {
            var parameters = new MapSqlParameterSource("executionId", entry.getKey().value())
                    .addValue("finishedAt", dialect.splitTimestamp(entry.getValue()));
            jdbc.update(dialect.cancelExecution(), parameters);
            String batchId = jdbc.queryForObject(dialect.findBatchIdByExecution(), parameters, String.class);
            if (batchId != null) batchIds.add(batchId);
            rearmTerminal(entry.getKey(), entry.getValue());
        }
        var counts = new java.util.TreeMap<String, Long>();
        batchIds.forEach(id -> counts.merge(id, 1L, Math::addExact));
        countFailedMembers(counts);
    }

    /** Caller holds the member's session, job and history locks in the same transaction. */
    BatchCounters completeBatchMembers(String batchId, long succeeded, long failed) {
        var params = new MapSqlParameterSource("id", batchId).addValue("succeeded", succeeded).addValue("failed", failed);
        if (jdbc.update("UPDATE mohs_batches SET succeeded = succeeded + :succeeded, failed = failed + :failed WHERE id = :id",
                params) != 1) {
            throw new IllegalStateException("no batch '" + batchId + "' to count members into");
        }
        // UPDATE retains the batch lock: the re-read identifies exactly this transaction's closer.
        return JdbcSupport.findOne(jdbc, dialect.findBatch(), params, JdbcBatchStore::mapRow)
                .orElseThrow(() -> new IllegalStateException("batch vanished while its row lock was held: " + batchId));
    }

    void countFailedMembers(Map<String, Long> counts) {
        for (var entry : new java.util.TreeMap<>(counts).entrySet()) {
            if (jdbc.update(dialect.countCancelledBatchMembers(), new MapSqlParameterSource("id", entry.getKey())
                    .addValue("pending", entry.getValue())) != 1) {
                throw new IllegalStateException("no batch '" + entry.getKey() + "' to count cancelled members into");
            }
        }
    }

    Map<ExecutionId, Metadata> metadata(Collection<ExecutionId> ids) {
        Map<ExecutionId, Metadata> result = new java.util.LinkedHashMap<>();
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            jdbc.query("SELECT execution_id, job_key, batch_id, shard, priority, origin FROM mohs_execution WHERE execution_id IN (:ids)",
                    new MapSqlParameterSource("ids", chunk), rs -> {
                result.put(ExecutionId.of(rs.getString("execution_id")),
                        new Metadata(JobKey.of(rs.getString("job_key")), rs.getString("batch_id"), rs.getInt("shard"),
                                rs.getInt("priority"), "SCHEDULED".equals(rs.getString("origin"))));
            });
        }
        return result;
    }

    record Metadata(JobKey jobKey, @org.jspecify.annotations.Nullable String batchId, int shard, int priority, boolean scheduled) {}

    /** Read only after locking the definitions; retirement then cannot race the chosen transition. */
    Set<JobKey> retiredJobs(Collection<JobKey> keys) {
        Set<JobKey> retired = new LinkedHashSet<>();
        for (var chunk : JdbcSupport.chunksOf(keys.stream().map(JobKey::value).distinct().toList())) {
            retired.addAll(jdbc.query("SELECT job_key FROM mohs_job_definitions WHERE job_key IN (:keys) AND retired = :retired",
                    new MapSqlParameterSource("keys", chunk).addValue("retired", true),
                    (rs, _) -> JobKey.of(rs.getString("job_key"))));
        }
        return retired;
    }

    void rearmTerminal(ExecutionId id, Instant finishedAt) {
        var next = jdbc.query("""
                SELECT e.job_key, e.schedule_revision, d.interval_micros
                FROM mohs_execution e JOIN mohs_job_definitions d ON d.job_key = e.job_key
                WHERE e.execution_id = :executionId
                  AND e.origin = 'SCHEDULED' AND e.schedule_revision = d.schedule_revision
                  AND d.retired = :retired AND d.schedule_type = 'INTERVAL' AND d.interval_after_finish = :afterFinish
                """, new MapSqlParameterSource("executionId", id.value())
                        .addValue("retired", false).addValue("afterFinish", true), rs -> {
            if (!rs.next()) {
                return Optional.<MapSqlParameterSource>empty();
            }
            Instant at = finishedAt.plus(io.mohs.core.TimePrecision.duration(rs.getLong("interval_micros")));
            return Optional.of(new MapSqlParameterSource("jobKey", rs.getString("job_key"))
                    .addValue("nextFireAt", JdbcTimestamps.toUtcLocalDateTime(io.mohs.core.TimePrecision.eligible(at))).addValue("retired", false)
                    .addValue("scheduleRevision", rs.getLong("schedule_revision")));
        });
        if (next != null) {
            next.ifPresent(parameters -> jdbc.update(dialect.armNextFire(), parameters));
        }
    }
}
