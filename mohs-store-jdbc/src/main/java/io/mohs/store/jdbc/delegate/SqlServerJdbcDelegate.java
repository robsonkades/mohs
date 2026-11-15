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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/**
 * SQL Server statement differences: TOP limits, lock hints and UPDATE OUTPUT acquisition.
 * Plain joined reads require READ_COMMITTED_SNAPSHOT, checked by {@link SqlServerRcsiRequirement}.
 * The active-work self-join retains target READPAST and an explicit join direction; verify actual
 * plans under representative cardinalities instead of assuming a particular seek is guaranteed.
 */
public final class SqlServerJdbcDelegate implements JdbcDelegate {
    @Override
    public String findQueuedExecutionIdsByJob() {
        return "SELECT execution_id FROM mohs_work WITH (UPDLOCK, ROWLOCK) WHERE work_state = 'READY' AND job_key = :jobKey ORDER BY execution_id";
    }

    @Override
    public String pruneExpiredIdempotency() {
        return """
                DELETE TOP (:limit) FROM mohs_idempotency WHERE expires_at <= :now
                """;
    }

    @Override
    public String lockNode() {
        return "SELECT node_id, state, last_heartbeat_at, expires_at, revoked FROM mohs_nodes WITH (UPDLOCK, ROWLOCK, HOLDLOCK) WHERE node_id = :nodeId";
    }

    @Override
    public String lockBindingOwner() {
        return """
                SELECT source, executable_key, input_contract, retired,
                    schedule_type, cron_expression, cron_zone, interval_micros, interval_after_finish
                FROM mohs_job_definitions WITH (UPDLOCK, ROWLOCK) WHERE job_key = :jobKey
                """;
    }

    @Override
    public String lockCatalog() {
        return "SELECT fingerprint FROM mohs_catalog WITH (UPDLOCK, ROWLOCK) WHERE singleton = 1";
    }

    /**
     * Creates a {@code SqlServerJdbcDelegate} instance.
     */
    public SqlServerJdbcDelegate() {
    }

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
    public String nodeLeasePage() {
        return """
                SELECT TOP (:limit) execution_id, job_key, node_id, claim_token, attempt, priority, claimed_at, cancel_requested
                FROM mohs_work WHERE work_state = 'LEASED' AND node_id = :nodeId
                  AND (claimed_at > :afterTime OR (claimed_at = :afterTime AND execution_id > :afterId))
                ORDER BY claimed_at, execution_id
                """;
    }

    @Override
    public String candidateJobKeys() {
        return """
                SELECT DISTINCT TOP (:limit) job_key FROM mohs_work
                WHERE work_state = 'READY' AND shard = :shard AND job_key > :afterKey AND visible_at <= :now
                ORDER BY job_key
                """;
    }

    @Override
    public String admissionCandidates(boolean restrictJobs) {
        return """
                SELECT TOP (:limit) r.execution_id, r.job_key, r.attempt, r.priority
                FROM mohs_work r WITH (READPAST, READCOMMITTEDLOCK) JOIN mohs_job_definitions d ON d.job_key = r.job_key
                WHERE r.work_state = 'READY' AND r.shard = :shard AND r.visible_at <= :now
                  AND d.retired = :inactive
                  AND (d.max_concurrent_executions = 0 OR d.max_concurrent_executions >
                       (SELECT COUNT_BIG(*) FROM mohs_work l WHERE l.work_state = 'LEASED' AND l.job_key = d.job_key))
                """ + (restrictJobs ? "AND r.job_key IN (:candidateJobs)\n" : "") + """
                ORDER BY r.priority, r.visible_at, r.execution_id
                """;
    }

    @Override
    public String readyCandidates() {
        return """
                SELECT TOP (:limit) execution_id, job_key, attempt, priority
                FROM mohs_work WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE work_state = 'READY' AND shard = :shard AND visible_at <= :now AND execution_id IN (:ids)
                ORDER BY priority, visible_at
                """;
    }

    /**
 * One guarded READY-to-LEASED update over picked work. Selection order is restored explicitly
 * in the returned result. Session and job admission locks belong to the caller's transaction.
 * Claim eligibility and the fresh persisted grant observation use separate parameters.
 */
    public static final String CLAIM_READY = """
            UPDATE w SET work_state = 'LEASED', visible_at = NULL,
                node_id = :nodeId, claim_token = :claimToken, claimed_at = :grantedAt
            OUTPUT inserted.execution_id, inserted.job_key, inserted.attempt, inserted.priority, deleted.visible_at
            FROM (SELECT TOP (:limit) execution_id
                  FROM mohs_work WITH (UPDLOCK, ROWLOCK, READPAST)
                  WHERE work_state = 'READY' AND shard = :shard AND visible_at <= :now AND execution_id IN (:ids)
                  ORDER BY priority, visible_at, execution_id) p
            INNER LOOP JOIN mohs_work w WITH (ROWLOCK, READPAST) ON w.execution_id = p.execution_id
            """;

    /** One {@code OUTPUT} row: the claimed entry plus the sort key {@code OUTPUT} does not preserve. */
    private record Picked(ClaimedReady row, Instant visibleAt) {

        int priority() {
            return row.priority();
        }
    }

    @Override
    public List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, String claimToken,
            int limit, Collection<String> executionIds, Instant now, Instant grantedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard).addValue("now", splitTimestamp(now))
                .addValue("limit", (long) limit, java.sql.Types.BIGINT).addValue("ids", executionIds)
                .addValue("nodeId", nodeId).addValue("claimToken", claimToken).addValue("grantedAt", splitTimestamp(grantedAt));
        return jdbcTemplate.query(CLAIM_READY, params, this::pickedRow).stream()
                .sorted(Comparator.comparingInt(Picked::priority).thenComparing(Picked::visibleAt)
                        .thenComparing(picked -> picked.row().executionId()))
                .map(Picked::row).toList();
    }



    private Picked pickedRow(ResultSet rs, int rowNum) throws SQLException {
        return new Picked(ClaimedReady.fromReadyRow(rs, rowNum),
                Objects.requireNonNull(readSplitTimestamp(rs, "visible_at"), "visible_at"));
    }

    @Override
    public String visibleWorkCount() {
        return """
                SELECT COUNT_BIG(*) FROM mohs_work WHERE work_state = 'READY' AND visible_at <= :now
                """;
    }

    @Override
    public String pruneTerminalExecutionsBefore() {
        return """
                DELETE TOP (:limit) FROM mohs_execution
                WHERE execution_id < :cutoffId AND finished_at < :cutoff
                  AND state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                """;
    }

    @Override
    public String pruneEmptyBatchesBefore() {
        return """
                DELETE TOP (:limit) FROM mohs_batches
                WHERE id < :cutoffId
                  AND NOT EXISTS (SELECT 1 FROM mohs_execution e WHERE e.batch_id = mohs_batches.id)
                """;
    }

    @Override
    public String findExecutionPage(String whereClause) {
        return """
                SELECT TOP (:limit) e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
                       e.batch_id, e.idempotency_key,
                       w.node_id AS lease_node, w.claimed_at AS lease_claimed_at,
                       CASE WHEN w.work_state = 'READY' THEN w.execution_id END AS ready_id, w.attempt AS ready_attempt, w.visible_at AS ready_visible_at
                FROM mohs_execution e
                LEFT JOIN mohs_work w ON w.execution_id = e.execution_id
                """
                + whereClause
                + """
                ORDER BY e.execution_id DESC
                """;
    }

    @Override
    public String countActiveInQueue() {
        return """
                SELECT COUNT_BIG(*) AS queued,
                       COUNT_BIG(CASE WHEN attempt > 1 AND visible_at > :now THEN 1 END) AS waiting
                FROM mohs_work WHERE work_state = 'READY'
                """;
    }

    @Override
    public String countRunning() {
        return "SELECT COUNT_BIG(*) FROM mohs_work WHERE work_state = 'LEASED'";
    }

    @Override
    public String countTerminalOutcomesSince() {
        return """
                SELECT state AS outcome, COUNT_BIG(*) AS finished FROM mohs_execution
                WHERE finished_at >= :since AND state IN ('SUCCEEDED', 'FAILED')
                GROUP BY state
                """;
    }

    @Override
    public String findOrphanedLeases() {
        return """
                SELECT TOP (:limit) execution_id, job_key, node_id, claim_token, attempt, priority, claimed_at, cancel_requested
                FROM mohs_work WHERE work_state = 'LEASED' AND EXISTS (SELECT 1 FROM mohs_nodes n WHERE n.node_id = mohs_work.node_id AND n.revoked = :revoked)
                ORDER BY claimed_at
                """;
    }

    @Override
    public String countLeasesByJob() {
        return """
                SELECT job_key, COUNT_BIG(*) AS leases FROM mohs_work
                WHERE work_state = 'LEASED' AND job_key IN (:jobKeys) GROUP BY job_key
                """;
    }

    @Override
    public String drainedBatchMembers() {
        return """
                SELECT batch_id AS batch_id, COUNT_BIG(*) AS pending
                FROM mohs_execution
                WHERE execution_id IN (:ids) AND batch_id IS NOT NULL
                GROUP BY batch_id
                """;
    }

    @Override
    public String findDueRecurringJobs() {
        return """
                SELECT TOP (:limit) job_key, executable_key, input_contract, name, schedule_type, cron_expression, cron_zone,
                    interval_micros, interval_after_finish, runner, window_name, rate_limit,
                    misfire, start_paused, max_concurrent_executions,
                    retries, timeout_micros, retry_policy, source, paused, retired,
                    next_fire_at, schedule_revision, created_at, updated_at
                FROM mohs_job_definitions
                WHERE retired = :retired AND paused = :paused
                  AND next_fire_at IS NOT NULL AND next_fire_at <= :now
                ORDER BY next_fire_at
                """;
    }

}
