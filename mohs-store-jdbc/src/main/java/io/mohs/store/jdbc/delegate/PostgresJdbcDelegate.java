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
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/**
 * PostgreSQL's SQL. The rationale for each statement is on {@link JdbcDelegate}; what is here is the
 * text — plus the one place where the SHAPE, not merely the text, is different: the claim is a single
 * statement here.
 */
public final class PostgresJdbcDelegate implements JdbcDelegate {

    /**
     * Creates a {@code PostgresJdbcDelegate} instance.
     */
    public PostgresJdbcDelegate() {
    }

    /** {@code TIMESTAMPTZ}: the server answers with a zone, so the offset is exact without asking for UTC. */
    @Override
    public String nowQuery() {
        return "SELECT CURRENT_TIMESTAMP";
    }

    @Override
    public Instant readNow(ResultSet rs) throws SQLException {
        return JdbcTimestamps.fromUtcOffsetDateTime(rs.getObject(1, OffsetDateTime.class));
    }

    /** The split tables are {@code TIMESTAMPTZ} here: UTC {@code OffsetDateTime} — a {@code LocalDateTime} would be read in the SESSION's zone (see the interface's Javadoc). */
    @Override
    public Object splitTimestamp(Instant instant) {
        return JdbcTimestamps.toUtcOffsetDateTime(instant);
    }

    @Override
    public @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : JdbcTimestamps.fromUtcOffsetDateTime(value);
    }

    // --- the claim ---------------------------------------------------------------------------------

    @Override
    public String readyCandidates() {
        return """
                SELECT execution_id, job_key, attempt, priority
                FROM mohs_ready
                WHERE shard = :shard AND visible_at <= :now
                ORDER BY priority, visible_at
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """;
    }

    @Override
    public String readyCandidatesFiltered() {
        return """
                SELECT execution_id, job_key, attempt, priority
                FROM mohs_ready
                WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)
                ORDER BY priority, visible_at
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
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

    /**
     * The claim in the form only Postgres has: ONE statement — the CTE selects with
     * {@code SKIP LOCKED}, the {@code DELETE … USING} consumes the queue, the {@code INSERT} (also a
     * CTE) writes the ownership, and the final SELECT returns IN {@code (priority, visible_at)} ORDER —
     * an INSERT's {@code RETURNING} order is not guaranteed, and the other delegates' portable form
     * returns ordered: the port's contract is one and the same across all four.
     *
     * <p>The INSERT reads from {@code picked} — identical to reading from {@code gone}, which deletes
     * exactly {@code picked}.
     *
     * <p>Because this overrides the whole claim, the four portable statements above
     * ({@link #readyCandidates()}, {@link #readyCandidatesFiltered()}, {@link #readyDelete()},
     * {@link #leaseInsert()}) are the form Postgres does NOT take. They stay because a delegate answers
     * for all of its own SQL, and because they are the text this database would run if the single
     * statement ever had to be backed out.
     */
    public static final String CLAIM_READY = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt, priority, visible_at
                  FROM mohs_ready
                 WHERE shard = :shard AND visible_at <= :now
                 ORDER BY priority, visible_at
                 LIMIT :limit
                   FOR UPDATE SKIP LOCKED
            ),
            gone AS (
                DELETE FROM mohs_ready r USING picked p
                 WHERE r.execution_id = p.execution_id
            ),
            leased AS (
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at)
                SELECT execution_id, job_key, :nodeId, :epoch, attempt, priority, :now FROM picked
            )
            SELECT execution_id, job_key, attempt, priority FROM picked
            ORDER BY priority, visible_at
            """;

    /**
     * Written out in full rather than derived from {@link #CLAIM_READY} by {@code replace}: the derived
     * form needed a static initialiser to prove the anchor had not drifted, and a statement that has to
     * be guarded against its own construction is one that should have been written down. What guards it
     * now is {@code JdbcDelegateStatementDriftTest}, which asserts the predicate is here.
     */
    public static final String CLAIM_READY_FILTERED = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt, priority, visible_at
                  FROM mohs_ready
                 WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)
                 ORDER BY priority, visible_at
                 LIMIT :limit
                   FOR UPDATE SKIP LOCKED
            ),
            gone AS (
                DELETE FROM mohs_ready r USING picked p
                 WHERE r.execution_id = p.execution_id
            ),
            leased AS (
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at)
                SELECT execution_id, job_key, :nodeId, :epoch, attempt, priority, :now FROM picked
            )
            SELECT execution_id, job_key, attempt, priority FROM picked
            ORDER BY priority, visible_at
            """;

    @Override
    public List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, long epoch,
            int limit, Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit)
                .addValue("nodeId", nodeId)
                .addValue("epoch", epoch);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(CLAIM_READY, params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(CLAIM_READY_FILTERED,
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
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
                WHERE execution_id = :executionId AND node_id = :nodeId AND epoch = :epoch AND attempt_number = :attemptNumber
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

    @Override
    public String visibleWorkExists() {
        return """
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM mohs_ready WHERE shard IN (:shards) AND visible_at <= :now
                ) THEN 1 ELSE 0 END
                """;
    }

    @Override
    public String visibleWorkCount() {
        return """
                SELECT COUNT(*) FROM mohs_ready WHERE visible_at <= :now
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

    // The terminal guard appears TWICE on purpose: the subquery's copy selects, and the outer one
    // decides the race — a predicate in a subquery evaluates against a snapshot and serialises
    // nothing, so a row a concurrent manual retry just rearmed to PENDING would still be deleted by
    // its id alone. Only the OUTER predicate is re-evaluated under the row lock.
    @Override
    public String pruneTerminalExecutionsBefore() {
        return """
                DELETE FROM mohs_execution WHERE execution_id IN (
                    SELECT execution_id FROM mohs_execution
                    WHERE execution_id < :cutoffId AND finished_at < :cutoff
                      AND state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                    LIMIT :limit
                ) AND state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                """;
    }

    @Override
    public String pruneOrphanedAttemptsBefore() {
        return """
                DELETE FROM mohs_attempt WHERE (execution_id, number) IN (
                    SELECT execution_id, number FROM mohs_attempt a
                    WHERE a.finished_at < :cutoff
                      AND NOT EXISTS (SELECT 1 FROM mohs_execution e WHERE e.execution_id = a.execution_id)
                    LIMIT :limit
                )
                """;
    }

    @Override
    public String pruneEmptyBatchesBefore() {
        return """
                DELETE FROM mohs_batches WHERE id IN (
                    SELECT id FROM mohs_batches b
                    WHERE b.id < :cutoffId
                      AND NOT EXISTS (SELECT 1 FROM mohs_execution e WHERE e.correlation_id = b.id)
                    LIMIT :limit
                )
                """;
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
                SELECT e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
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
                LIMIT :limit
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
                SELECT execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested
                FROM mohs_lease
                ORDER BY claimed_at
                LIMIT :limit
                """;
    }

    @Override
    public String findOrphanedLeasesExceptAlive() {
        return """
                SELECT execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested
                FROM mohs_lease
                WHERE node_id NOT IN (:aliveNodeIds)
                ORDER BY claimed_at
                LIMIT :limit
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
                LIMIT :limit
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
