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
    @Override
    public String pruneExpiredIdempotency() {
        return """
                WITH expired AS (
                    SELECT job_key, idempotency_key FROM mohs_idempotency WHERE expires_at <= :now
                    ORDER BY expires_at, job_key, idempotency_key LIMIT :limit FOR UPDATE SKIP LOCKED
                ) DELETE FROM mohs_idempotency i USING expired e
                WHERE i.job_key = e.job_key AND i.idempotency_key = e.idempotency_key AND i.expires_at <= :now
                """;
    }

    @Override
    public String lockBindingOwner() {
        return """
                SELECT source, executable_key, input_contract, retired,
                    schedule_type, cron_expression, cron_zone, interval_micros, interval_after_finish
                FROM mohs_job_definitions
                WHERE job_key = :jobKey FOR NO KEY UPDATE
                """;
    }

    /**
     * Creates a {@code PostgresJdbcDelegate} instance.
     */
    public PostgresJdbcDelegate() {
    }

    /** {@code TIMESTAMPTZ}: the server answers with a zone, so the offset is exact without asking for UTC. */
    @Override
    public String nowQuery() {
        return "SELECT clock_timestamp()";
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

    /**
 * One guarded READY-to-LEASED update over picked work. Selection order is restored explicitly
 * in the returned result. Session and job admission locks belong to the caller's transaction.
 * Claim eligibility and the fresh persisted grant observation use separate parameters.
 */
    public static final String CLAIM_READY = """
            WITH picked AS (
                SELECT execution_id, job_key, attempt, priority, visible_at
                FROM mohs_work
                WHERE work_state = 'READY' AND shard = :shard AND visible_at <= :now AND execution_id IN (:ids)
                ORDER BY priority, visible_at, execution_id
                LIMIT :limit FOR UPDATE SKIP LOCKED
            ), leased AS (
                UPDATE mohs_work w SET work_state = 'LEASED', visible_at = NULL,
                    node_id = :nodeId, claim_token = :claimToken, claimed_at = :grantedAt
                FROM picked p WHERE w.execution_id = p.execution_id AND w.work_state = 'READY'
                RETURNING w.execution_id
            )
            SELECT p.execution_id, p.job_key, p.attempt, p.priority
            FROM picked p JOIN leased l ON l.execution_id = p.execution_id
            ORDER BY p.priority, p.visible_at, p.execution_id
            """;

    @Override
    public List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, String claimToken,
            int limit, Collection<String> executionIds, Instant now, Instant grantedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", (long) limit, java.sql.Types.BIGINT).addValue("ids", executionIds)
                .addValue("nodeId", nodeId)
                .addValue("claimToken", claimToken).addValue("grantedAt", splitTimestamp(grantedAt));
        return jdbcTemplate.query(CLAIM_READY, params, ClaimedReady::fromReadyRow);
    }

    @Override
    public String visibleWorkCount() {
        return """
                SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY' AND visible_at <= :now
                """;
    }

    // The terminal and age guards appear twice: the subquery's copies select, and the outer ones
    // decides the race — a predicate in a subquery evaluates against a snapshot and serialises
    // nothing, so a concurrently retried row could now be PENDING or a recent terminal result.
    // Only the OUTER predicates are re-evaluated under the row lock.
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
