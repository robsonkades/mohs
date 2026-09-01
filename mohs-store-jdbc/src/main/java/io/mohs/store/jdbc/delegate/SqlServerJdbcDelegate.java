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
package io.mohs.store.jdbc.delegate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;

import org.jspecify.annotations.Nullable;

import io.mohs.store.jdbc.JdbcTimestamps;

/**
 * SQL Server's SQL. The rationale for each statement is on {@link JdbcDelegate}; what is here is the
 * T-SQL — including the three places where T-SQL is not merely different text but a different
 * STRUCTURE: {@code TOP} sits right after {@code SELECT} (never at the end, like {@code LIMIT}),
 * {@code FOR UPDATE SKIP LOCKED} does not exist and is emulated by
 * {@code WITH (UPDLOCK, ROWLOCK, READPAST)} (the emulation jOOQ confirms), and a read that must not
 * take locks needs an explicit hint under the default {@code READ COMMITTED} without RCSI.
 */
public final class SqlServerJdbcDelegate implements JdbcDelegate {

    /**
     * {@code CURRENT_TIMESTAMP} is a zoneless {@code DATETIME} read back in the JVM's zone, so it
     * measured the distance between two zones rather than between two clocks. {@code SYSUTCDATETIME}
     * states UTC, and is a {@code datetime2} besides — {@code DATETIME} rounds to about 3.3 ms.
     */
    @Override
    public String nowQuery() {
        return "SELECT SYSUTCDATETIME()";
    }

    @Override
    public Instant readNow(ResultSet rs) throws SQLException {
        return JdbcTimestamps.fromUtcLocalDateTime(rs.getObject(1, LocalDateTime.class));
    }

    @Override
    public Object splitTimestamp(Instant instant) {
        return JdbcTimestamps.toUtcLocalDateTime(instant);
    }

    @Override
    public @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        LocalDateTime value = rs.getObject(column, LocalDateTime.class);
        return value == null ? null : JdbcTimestamps.fromUtcLocalDateTime(value);
    }

    // --- the claim ---------------------------------------------------------------------------------

    @Override
    public String readyCandidates() {
        return """
                SELECT TOP (:limit) execution_id, job_key, attempt, priority
                FROM mohs_ready WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE shard = :shard AND visible_at <= :now
                ORDER BY priority, visible_at
                """;
    }

    @Override
    public String readyCandidatesFiltered() {
        return """
                SELECT TOP (:limit) execution_id, job_key, attempt, priority
                FROM mohs_ready WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)
                ORDER BY priority, visible_at
                """;
    }

    @Override
    public String readyDelete() {
        return "DELETE FROM mohs_ready WHERE execution_id IN (:ids)";
    }

    @Override
    public String leaseInsert() {
        return """
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at)
                VALUES (:executionId, :jobKey, :nodeId, :epoch, :attempt, :priority, :now)
                """;
    }

    // --- the queue ---------------------------------------------------------------------------------

    @Override
    public String readyInsert() {
        return """
                INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
                VALUES (:executionId, :jobKey, :shard, :priority, :attempt, :visibleAt)
                """;
    }

    @Override
    public String fencedLeaseDelete() {
        return """
                DELETE FROM mohs_lease
                WHERE execution_id = :executionId AND node_id = :nodeId AND epoch = :epoch
                """;
    }

    @Override
    public String deleteReadyById() {
        return "DELETE FROM mohs_ready WHERE execution_id = :executionId";
    }

    // batch-counted: incrementFailed, in the same transaction as this write
    @Override
    public String cancelExecution() {
        return """
                UPDATE mohs_execution SET state = 'CANCELLED', finished_at = :finishedAt
                WHERE execution_id = :executionId
                """;
    }

    @Override
    public String findBatchIdByExecution() {
        return "SELECT correlation_id FROM mohs_execution WHERE execution_id = :executionId";
    }

    @Override
    public String rearmExecutionByCas() {
        return """
                UPDATE mohs_execution SET state = 'PENDING', finished_at = NULL
                WHERE execution_id = :executionId AND state = 'FAILED'
                  AND correlation_id IS NULL
                  AND EXISTS (SELECT 1 FROM mohs_job_definitions j
                              WHERE j.job_key = mohs_execution.job_key AND j.retired = :retired)
                """;
    }

    @Override
    public String rearmReadyFromHistory() {
        return """
                INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
                SELECT e.execution_id, e.job_key, e.shard, e.priority,
                       (SELECT COUNT(*) + 1 FROM mohs_attempt a WHERE a.execution_id = e.execution_id),
                       :visibleAt
                FROM mohs_execution e WHERE e.execution_id = :executionId
                """;
    }

    /**
     * {@code NOLOCK} (read uncommitted), not {@code READPAST}: skipping a locked row systematically
     * undercounts under load.
     *
     * <p>The accepted error is the mechanism's worst case, not merely "±1 in transition": with no
     * required order the optimiser may choose an allocation-order scan, which under a concurrent page
     * split reads a row twice or loses it; and the scan may fail with error 601 ("data movement"), which
     * here becomes a transient read failure that falls into the {@code Engine}'s fail-open fallback.
     *
     * <p>A deployment with RCSI ({@code READ_COMMITTED_SNAPSHOT ON}) makes the hint redundant — the
     * operator's decision, not the library's.
     */
    @Override
    public String visibleWorkExists() {
        return """
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM mohs_ready WITH (NOLOCK) WHERE shard IN (:shards) AND visible_at <= :now
                ) THEN 1 ELSE 0 END
                """;
    }

    /** The same {@code NOLOCK} as the probe, with the anomalies costing an approximate gauge rather than a lap — see {@link #visibleWorkExists()}. */
    @Override
    public String visibleWorkCount() {
        return """
                SELECT COUNT(*) FROM mohs_ready WITH (NOLOCK) WHERE visible_at <= :now
                """;
    }

    // --- history -----------------------------------------------------------------------------------

    @Override
    public String recordExecution() {
        return """
                INSERT INTO mohs_execution (
                    execution_id, job_key, shard, priority, state, scheduled_at, created_at, actor,
                    correlation_id, idempotency_key, payload, payload_type)
                VALUES (:executionId, :jobKey, :shard, :priority, 'PENDING', :scheduledAt, :createdAt, :actor,
                    :correlationId, :idempotencyKey, :payload, :payloadType)
                """;
    }

    @Override
    public String insertIdempotency() {
        return """
                INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at)
                VALUES (:jobKey, :idempotencyKey, :executionId, :createdAt)
                """;
    }

    @Override
    public String findExecutionIdByIdempotencyKey() {
        return """
                SELECT execution_id FROM mohs_idempotency
                WHERE job_key = :jobKey AND idempotency_key = :idempotencyKey
                """;
    }

    @Override
    public String findPayloads() {
        return """
                SELECT execution_id, job_key, scheduled_at, created_at, actor, priority, correlation_id,
                       payload, payload_type
                FROM mohs_execution WHERE execution_id IN (:ids)
                """;
    }

    @Override
    public String findHeads() {
        return """
                SELECT execution_id, job_key, scheduled_at, created_at, actor, priority, correlation_id
                FROM mohs_execution WHERE execution_id IN (:ids)
                """;
    }

    @Override
    public String findAttempts() {
        return """
                SELECT number, started_at, finished_at, outcome, error FROM mohs_attempt
                WHERE execution_id = :executionId ORDER BY number
                """;
    }

    @Override
    public String pruneIdempotencyBefore() {
        return "DELETE FROM mohs_idempotency WHERE created_at < :cutoff";
    }

    @Override
    public String findExecutionById() {
        return """
                SELECT e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
                       e.correlation_id, e.idempotency_key,
                       l.node_id AS lease_node, l.claimed_at AS lease_claimed_at,
                       r.execution_id AS ready_id, r.attempt AS ready_attempt, r.visible_at AS ready_visible_at
                FROM mohs_execution e
                LEFT JOIN mohs_lease l ON l.execution_id = e.execution_id
                LEFT JOIN mohs_ready r ON r.execution_id = e.execution_id
                WHERE e.execution_id = :executionId
                """;
    }

    @Override
    public String findExecutionPage(String whereClause) {
        return """
                SELECT TOP (:limit) e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
                       e.correlation_id, e.idempotency_key,
                       l.node_id AS lease_node, l.claimed_at AS lease_claimed_at,
                       r.execution_id AS ready_id, r.attempt AS ready_attempt, r.visible_at AS ready_visible_at
                FROM mohs_execution e
                LEFT JOIN mohs_lease l ON l.execution_id = e.execution_id
                LEFT JOIN mohs_ready r ON r.execution_id = e.execution_id
                """
                + whereClause
                + """
                ORDER BY e.execution_id DESC
                """;
    }

    @Override
    public String countActiveInQueue() {
        return """
                SELECT COUNT(*) AS queued,
                       SUM(CASE WHEN attempt > 1 AND visible_at > :now THEN 1 ELSE 0 END) AS waiting
                FROM mohs_ready
                """;
    }

    @Override
    public String countRunning() {
        return "SELECT COUNT(*) FROM mohs_lease";
    }

    @Override
    public String countTerminalOutcomesSince() {
        return """
                SELECT outcome, COUNT(*) AS finished FROM mohs_attempt
                WHERE finished_at >= :since AND outcome IN ('SUCCEEDED', 'FAILED')
                GROUP BY outcome
                """;
    }

    // --- ownership ---------------------------------------------------------------------------------

    @Override
    public String findLeasesByNodes() {
        return """
                SELECT execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested
                FROM mohs_lease WHERE node_id IN (:nodeIds)
                """;
    }

    @Override
    public String findOrphanedLeases() {
        return """
                SELECT TOP (:limit) execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested
                FROM mohs_lease
                ORDER BY claimed_at
                """;
    }

    @Override
    public String findOrphanedLeasesExceptAlive() {
        return """
                SELECT TOP (:limit) execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested
                FROM mohs_lease
                WHERE node_id NOT IN (:aliveNodeIds)
                ORDER BY claimed_at
                """;
    }

    @Override
    public String countLeasesByJob() {
        return """
                SELECT job_key, COUNT(*) AS leases FROM mohs_lease
                WHERE job_key IN (:jobKeys) GROUP BY job_key
                """;
    }

    @Override
    public String requestLeaseCancellation() {
        return "UPDATE mohs_lease SET cancel_requested = :flag WHERE execution_id = :executionId";
    }

    @Override
    public String findCancelRequestedLeases() {
        return """
                SELECT execution_id FROM mohs_lease
                WHERE execution_id IN (:ids) AND cancel_requested = :flag
                """;
    }

    @Override
    public String insertAttempt() {
        return """
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
                VALUES (:executionId, :number, :nodeId, :startedAt, :finishedAt, :outcome, :errorType, :error)
                """;
    }

    // batch-counted: countIntoBatch, in the same transaction as the complete
    @Override
    public String terminalStateUpdate() {
        return """
                UPDATE mohs_execution SET state = :state, finished_at = :finishedAt
                WHERE execution_id = :executionId
                """;
    }

    // --- definitions -------------------------------------------------------------------------------

    @Override
    public String upsertJobUpdate(boolean writeNextFire) {
        if (writeNextFire) {
            return """
                    UPDATE mohs_job_definitions SET
                        name = :name, handler_type = :handlerType, schedule_type = :scheduleType,
                        cron_expression = :cronExpression, cron_zone = :cronZone,
                        interval_duration = :intervalDuration, interval_after_finish = :intervalAfterFinish,
                        runner = :runner, window_name = :windowName, rate_limit = :rateLimit,
                        misfire = :misfire, start_paused = :startPaused,
                        allow_concurrent_executions = :allowConcurrentExecutions,
                        max_concurrent_executions = :maxConcurrentExecutions,
                        retries = :retries, timeout = :timeout, retry_policy = :retryPolicy,
                        source = :source, orphaned = :orphaned, retired = :retired,
                        next_fire_at = :nextFireAt, updated_at = :updatedAt
                    WHERE job_key = :jobKey
                    """;
        }
        return """
                UPDATE mohs_job_definitions SET
                    name = :name, handler_type = :handlerType, schedule_type = :scheduleType,
                    cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_duration = :intervalDuration, interval_after_finish = :intervalAfterFinish,
                    runner = :runner, window_name = :windowName, rate_limit = :rateLimit,
                    misfire = :misfire, start_paused = :startPaused,
                    allow_concurrent_executions = :allowConcurrentExecutions,
                    max_concurrent_executions = :maxConcurrentExecutions,
                    retries = :retries, timeout = :timeout, retry_policy = :retryPolicy,
                    source = :source, orphaned = :orphaned, retired = :retired,
                    updated_at = :updatedAt
                WHERE job_key = :jobKey
                """;
    }

    @Override
    public String insertJob() {
        return """
                INSERT INTO mohs_job_definitions (
                    id, job_key, name, handler_type, schedule_type, cron_expression, cron_zone,
                    interval_duration, interval_after_finish, runner, window_name, rate_limit,
                    misfire, start_paused, allow_concurrent_executions, max_concurrent_executions, retries,
                    timeout, retry_policy, source,
                    orphaned, retired, paused, next_fire_at, created_at, updated_at)
                VALUES (
                    :id, :jobKey, :name, :handlerType, :scheduleType, :cronExpression, :cronZone,
                    :intervalDuration, :intervalAfterFinish, :runner, :windowName, :rateLimit,
                    :misfire, :startPaused, :allowConcurrentExecutions, :maxConcurrentExecutions, :retries,
                    :timeout, :retryPolicy, :source,
                    :orphaned, :retired, :paused, :nextFireAt, :createdAt, :updatedAt)
                """;
    }

    @Override
    public String rescheduleJob() {
        return """
                UPDATE mohs_job_definitions SET
                    schedule_type = :scheduleType, cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_duration = :intervalDuration, interval_after_finish = :intervalAfterFinish,
                    next_fire_at = :nextFireAt, updated_at = :updatedAt
                WHERE job_key = :jobKey AND retired = :retired
                """;
    }

    @Override
    public String countLiveSchedulerOccurrences() {
        return """
                SELECT COUNT(*) FROM mohs_execution
                WHERE job_key = :jobKey AND actor = :actor AND state = 'PENDING'
                """;
    }

    // batch-counted: countCancelledMembers, over the set that was ACTUALLY drained
    @Override
    public String cancelDrainedExecutions() {
        return """
                UPDATE mohs_execution SET state = 'CANCELLED', finished_at = :now
                WHERE execution_id IN (:ids)
                """;
    }

    @Override
    public String markJobOrphaned() {
        return "UPDATE mohs_job_definitions SET orphaned = :orphaned WHERE job_key = :jobKey";
    }

    @Override
    public String setJobPaused() {
        return "UPDATE mohs_job_definitions SET paused = :paused WHERE job_key = :jobKey";
    }

    @Override
    public String retireJob() {
        return "UPDATE mohs_job_definitions SET retired = :retired, updated_at = :now WHERE job_key = :jobKey";
    }

    @Override
    public String drainedBatchMembers() {
        return """
                SELECT correlation_id AS batch_id, COUNT(*) AS pending
                FROM mohs_execution
                WHERE execution_id IN (:ids) AND correlation_id IS NOT NULL
                GROUP BY correlation_id
                """;
    }

    @Override
    public String countCancelledBatchMembers() {
        return "UPDATE mohs_batches SET failed = failed + :pending WHERE id = :id";
    }

    @Override
    public String findTriggerSnapshot() {
        return """
                SELECT schedule_type, cron_expression, cron_zone, interval_duration, interval_after_finish, next_fire_at
                FROM mohs_job_definitions WHERE job_key = :jobKey
                """;
    }

    @Override
    public String findJobByKey() {
        return "SELECT * FROM mohs_job_definitions WHERE job_key = :jobKey AND retired = :retired";
    }

    @Override
    public String findAllJobs() {
        return "SELECT * FROM mohs_job_definitions WHERE retired = :retired";
    }

    @Override
    public String findAllAnnotationSourcedJobs() {
        return "SELECT * FROM mohs_job_definitions WHERE source = :source AND retired = :retired";
    }

    @Override
    public String findDueRecurringJobs() {
        return """
                SELECT * FROM mohs_job_definitions
                WHERE retired = :retired AND paused = :paused AND orphaned = :orphaned
                  AND next_fire_at IS NOT NULL AND next_fire_at <= :now
                ORDER BY next_fire_at
                """;
    }

    @Override
    public String armNextFire() {
        return "UPDATE mohs_job_definitions SET next_fire_at = :nextFireAt WHERE job_key = :jobKey AND next_fire_at IS NULL";
    }

    @Override
    public String findQueuedExecutionIdsByJob() {
        return "SELECT execution_id FROM mohs_ready WHERE job_key = :jobKey";
    }

    // --- nodes -------------------------------------------------------------------------------------

    @Override
    public String insertNode() {
        return """
                INSERT INTO mohs_nodes (node_id, state, last_heartbeat_at, epoch, expires_at)
                VALUES (:nodeId, :state, :lastHeartbeatAt, :epoch, :expiresAt)
                """;
    }

    @Override
    public String findAllNodes() {
        return "SELECT * FROM mohs_nodes";
    }

    @Override
    public String deleteHeartbeatsBefore() {
        return "DELETE FROM mohs_nodes WHERE last_heartbeat_at < :cutoff";
    }

    @Override
    public String heartbeatUpdate() {
        return """
                UPDATE mohs_nodes SET state = :state, last_heartbeat_at = :lastHeartbeatAt,
                    epoch = :epoch, expires_at = :expiresAt
                WHERE node_id = :nodeId
                """;
    }

    // --- rate limits -------------------------------------------------------------------------------

    @Override
    public String findRateLimitByName() {
        return "SELECT * FROM mohs_rate_limits WHERE name = :name";
    }

    @Override
    public String findAllRateLimits() {
        return "SELECT * FROM mohs_rate_limits";
    }

    @Override
    public String readRateLimitBucket() {
        return "SELECT max_count, window_duration, tokens, refilled_at FROM mohs_rate_limits WHERE name = :name";
    }

    @Override
    public String updateRateLimitSpec() {
        return """
                UPDATE mohs_rate_limits
                   SET max_count = :maxCount, window_duration = :windowDuration,
                       tokens = CASE WHEN tokens > :maxCount THEN :maxCount ELSE tokens END
                 WHERE name = :name
                """;
    }

    @Override
    public String insertFullRateLimitBucket() {
        return """
                INSERT INTO mohs_rate_limits (name, max_count, window_duration, tokens, refilled_at)
                VALUES (:name, :maxCount, :windowDuration, :maxCount, :refilledAt)
                """;
    }

    @Override
    public String chargeRateLimitByCas() {
        return """
                UPDATE mohs_rate_limits
                   SET tokens = :tokens, refilled_at = :refilledAt
                 WHERE name = :name AND tokens = :expectedTokens AND refilled_at = :expectedRefilledAt
                   AND max_count = :expectedMax AND window_duration = :expectedWindow
                """;
    }

    // --- batches -----------------------------------------------------------------------------------

    @Override
    public String insertBatch() {
        return """
                INSERT INTO mohs_batches (id, name, total, succeeded, failed, created_at)
                VALUES (:id, :name, :total, 0, 0, :createdAt)
                """;
    }

    @Override
    public String findBatch() {
        return "SELECT id, name, total, succeeded, failed FROM mohs_batches WHERE id = :id";
    }

    @Override
    public String incrementBatchSucceeded() {
        return "UPDATE mohs_batches SET succeeded = succeeded + 1 WHERE id = :id";
    }

    @Override
    public String incrementBatchFailed() {
        return "UPDATE mohs_batches SET failed = failed + 1 WHERE id = :id";
    }

    // --- the firing --------------------------------------------------------------------------------

    @Override
    public String advanceTriggerByCas() {
        return """
                UPDATE mohs_job_definitions SET next_fire_at = :newNextFireAt
                WHERE job_key = :jobKey AND next_fire_at = :observedNextFireAt AND retired = :retired
                """;
    }
}
