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
 * MySQL's SQL. The rationale for each statement is on {@link JdbcDelegate}; what is here is the text.
 */
public final class MySqlJdbcDelegate implements JdbcDelegate {
    @Override
    public String pruneExpiredIdempotency() {
        return """
                DELETE FROM mohs_idempotency WHERE expires_at <= :now
                ORDER BY expires_at, job_key, idempotency_key LIMIT :limit
                """;
    }

    @Override
    public String lockBindingOwner() {
        return """
                SELECT source, executable_key, input_contract, retired,
                    schedule_type, cron_expression, cron_zone, interval_micros, interval_after_finish
                FROM mohs_job_definitions
                WHERE job_key = :jobKey FOR UPDATE
                """;
    }

    /**
     * Creates a {@code MySqlJdbcDelegate} instance.
     */
    public MySqlJdbcDelegate() {
    }

    /**
     * {@code CURRENT_TIMESTAMP} is evaluated in the session's {@code time_zone} and materialised by
     * Connector/J in the JVM's zone, so it measured the distance between two zones rather than between
     * two clocks. {@code UTC_TIMESTAMP} states the zone instead of implying one; the explicit {@code (6)}
     * is not decoration — without it the answer is truncated to whole seconds, a resolution a clock
     * cannot use.
     */
    @Override
    public String nowQuery() {
        return "SELECT UTC_TIMESTAMP(6)";
    }

    @Override
    public Instant readNow(ResultSet rs) throws SQLException {
        return JdbcTimestamps.fromUtcLocalDateTime(rs.getObject(1, LocalDateTime.class));
    }

    @Override
    public String readyCandidates() {
        return """
                SELECT execution_id, job_key, attempt, priority
                FROM mohs_work
                WHERE work_state = 'READY' AND shard = :shard AND visible_at <= :now AND execution_id IN (:ids)
                ORDER BY priority, visible_at
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """;
    }

    @Override
    public String visibleWorkCount() {
        return """
                SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY' AND visible_at <= :now
                """;
    }

    // The two sweep statements bound with a bare LIMIT: MySQL forbids a subquery on the table being
    // deleted from (error 1093), and needs none — DELETE ... LIMIT is native here.
    @Override
    public String pruneTerminalExecutionsBefore() {
        return """
                DELETE FROM mohs_execution
                WHERE execution_id < :cutoffId AND finished_at < :cutoff
                  AND state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                LIMIT :limit
                """;
    }

    @Override
    public String pruneEmptyBatchesBefore() {
        return """
                DELETE FROM mohs_batches
                WHERE id < :cutoffId
                  AND NOT EXISTS (SELECT 1 FROM mohs_execution e WHERE e.batch_id = mohs_batches.id)
                LIMIT :limit
                """;
    }

    @Override
    public String findExecutionPage(String whereClause) {
        return """
                SELECT e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
                       e.batch_id, e.idempotency_key,
                       w.node_id AS lease_node, w.claimed_at AS lease_claimed_at,
                       CASE WHEN w.work_state = 'READY' THEN w.execution_id END AS ready_id, w.attempt AS ready_attempt, w.visible_at AS ready_visible_at
                FROM mohs_execution e
                LEFT JOIN mohs_work w ON w.execution_id = e.execution_id
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
                FROM mohs_work WHERE work_state = 'READY'
                """;
    }

    @Override
    public String countRunning() {
        return "SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'";
    }

    @Override
    public String countTerminalOutcomesSince() {
        return """
                SELECT state AS outcome, COUNT(*) AS finished FROM mohs_execution
                WHERE finished_at >= :since AND state IN ('SUCCEEDED', 'FAILED')
                GROUP BY state
                """;
    }

    @Override
    public String findOrphanedLeases() {
        return """
                SELECT execution_id, job_key, node_id, claim_token, attempt, priority, claimed_at, cancel_requested
                FROM mohs_work WHERE work_state = 'LEASED' AND EXISTS (SELECT 1 FROM mohs_nodes n WHERE n.node_id = mohs_work.node_id AND n.revoked = :revoked)
                ORDER BY claimed_at
                LIMIT :limit
                """;
    }

    @Override
    public String countLeasesByJob() {
        return """
                SELECT job_key, COUNT(*) AS leases FROM mohs_work
                WHERE work_state = 'LEASED' AND job_key IN (:jobKeys) GROUP BY job_key
                """;
    }

    @Override
    public String drainedBatchMembers() {
        return """
                SELECT batch_id AS batch_id, COUNT(*) AS pending
                FROM mohs_execution
                WHERE execution_id IN (:ids) AND batch_id IS NOT NULL
                GROUP BY batch_id
                """;
    }

    @Override
    public String findDueRecurringJobs() {
        return """
                SELECT job_key, executable_key, input_contract, name, schedule_type, cron_expression, cron_zone,
                    interval_micros, interval_after_finish, runner, window_name, rate_limit,
                    misfire, start_paused, max_concurrent_executions,
                    retries, timeout_micros, retry_policy, source, paused, retired,
                    next_fire_at, schedule_revision, created_at, updated_at
                FROM mohs_job_definitions
                WHERE retired = :retired AND paused = :paused
                  AND next_fire_at IS NOT NULL AND next_fire_at <= :now
                ORDER BY next_fire_at
                LIMIT :limit
                """;
    }

}
