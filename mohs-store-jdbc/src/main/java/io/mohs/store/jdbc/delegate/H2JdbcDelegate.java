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
import java.time.OffsetDateTime;

import org.jspecify.annotations.Nullable;

import io.mohs.store.jdbc.JdbcTimestamps;

/**
 * H2's SQL. The rationale for each statement is on {@link JdbcDelegate}; what is here is the text.
 */
public final class H2JdbcDelegate implements JdbcDelegate {
    @Override
    public String pruneExpiredIdempotency() {
        return """
                DELETE FROM mohs_idempotency
                WHERE (job_key, idempotency_key) IN (
                    SELECT job_key, idempotency_key FROM mohs_idempotency WHERE expires_at <= :now
                    ORDER BY expires_at, job_key, idempotency_key LIMIT :limit
                ) AND expires_at <= :now
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
     * Creates a {@code H2JdbcDelegate} instance.
     */
    public H2JdbcDelegate() {
    }

    /** H2 answers {@code CURRENT_TIMESTAMP} with a zone, so the offset is exact without asking for UTC. */
    @Override
    public String nowQuery() {
        return "SELECT mohs_clock_now()";
    }

    @Override
    public Instant readNow(ResultSet rs) throws SQLException {
        return JdbcTimestamps.fromUtcOffsetDateTime(rs.getObject(1, OffsetDateTime.class));
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

    // The doubled terminal and age guards mirror the Postgres shape: in a
    // DELETE ... WHERE pk IN (self-subquery), only the OUTER predicate is re-evaluated against the
    // current row under the lock — the subquery's copy saw a snapshot.
    @Override
    public String pruneTerminalExecutionsBefore() {
        return """
                DELETE FROM mohs_execution WHERE execution_id IN (
                    SELECT execution_id FROM mohs_execution
                    WHERE execution_id < :cutoffId AND finished_at < :cutoff
                      AND state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                    LIMIT :limit
                ) AND state IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND finished_at < :cutoff
                """;
    }

    @Override
    public String pruneEmptyBatchesBefore() {
        return """
                DELETE FROM mohs_batches WHERE id IN (
                    SELECT id FROM mohs_batches b
                    WHERE b.id < :cutoffId
                      AND NOT EXISTS (SELECT 1 FROM mohs_execution e WHERE e.batch_id = b.id)
                    LIMIT :limit
                )
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
