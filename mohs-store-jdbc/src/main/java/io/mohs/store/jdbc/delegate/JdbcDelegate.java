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
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/**
 * Shared SQL and database-specific locking, limiting, clock and claim shapes.
 * Common statements have defaults; concrete delegates override actual database differences.
 * Named-parameter checks and real four-database protocol tests verify the contract together.
 * Transaction-owning stores may keep SQL specific to their complete operation locally.
 */
public interface JdbcDelegate {

    /**
     * Reads identity, active state and schedule while holding the definition's update lock until transaction end.
     * Bound writers lock distinct definitions in canonical key order; retirement uses the same
     * lock before checking dependents. Missing rows are resolved by the unique-key/savepoint retry.
     * PostgreSQL uses NO KEY UPDATE so foreign-key key-share locks remain compatible; SQL Server uses
     * UPDLOCK. The query excludes JSON and does not rewrite a row merely to acquire a lock.
     *
     * @return the complete locking SELECT
     */
    String lockBindingOwner();

    /** Current catalog fingerprint under its singleton mutex, independent of an older host snapshot. */
    default String lockCatalog() {
        return "SELECT fingerprint FROM mohs_catalog WHERE singleton = 1 FOR UPDATE";
    }

    /** findJobBinding statement for the durable executable binding. */
    default String findJobBinding() {
        return """
                SELECT job_key, executable_key, recurring_payload, input_contract
                FROM mohs_job_definitions WHERE job_key = :jobKey AND executable_key IS NOT NULL
                """;
    }

    /** Indexed identity lookup; deliberately excludes potentially large recurring inputs. */
    default String findExecutableKey() {
        return """
                SELECT executable_key FROM mohs_job_definitions WHERE job_key = :jobKey AND executable_key IS NOT NULL
                """;
    }

    /** Returns 1 when another active job depends on the executable, otherwise 0. */
    default String hasActiveJobDependents() {
        return """
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM mohs_job_definitions
                    WHERE executable_key = :jobKey AND job_key <> :jobKey AND retired = :retired
                ) THEN 1 ELSE 0 END AS present
                """;
    }

    /** findActiveJobBindings statement for the durable executable binding. */
    default String findActiveJobBindings() {
        return """
                SELECT job_key, executable_key, recurring_payload, input_contract
                FROM mohs_job_definitions WHERE retired = :retired AND executable_key IS NOT NULL
                """;
    }

    /**
     * Updates definition policy and its executable/input in one statement, under the owner lock.
     * An unchanged schedule excludes next_fire_at entirely. Legacy definition updates use
     * {@link #upsertJobUpdate} and preserve the binding columns.
     *
     * @param writeNextFire whether this statement replaces the trigger time
     * @return the complete SQL for the bound definition update
     */
    default String upsertBoundJobUpdate(boolean writeNextFire) {
        if (writeNextFire) {
            return """
                    UPDATE mohs_job_definitions SET
                        name = :name, schedule_type = :scheduleType,
                        cron_expression = :cronExpression, cron_zone = :cronZone,
                        interval_micros = :intervalMicros, interval_after_finish = :intervalAfterFinish,
                        runner = :runner, window_name = :windowName, rate_limit = :rateLimit,
                        misfire = :misfire, start_paused = :startPaused,

                        max_concurrent_executions = :maxConcurrentExecutions,
                        retries = :retries, timeout_micros = :timeout_micros, retry_policy = :retryPolicy,
                        executable_key = :executableKey, recurring_payload = :recurringPayload,
                        input_contract = :inputContract,
                        source = :source, retired = :retired,
                        next_fire_at = :nextFireAt, schedule_revision = schedule_revision + 1, updated_at = :updatedAt
                    WHERE job_key = :jobKey
                    """;
        }
        return """
                UPDATE mohs_job_definitions SET
                    name = :name, schedule_type = :scheduleType,
                    cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_micros = :intervalMicros, interval_after_finish = :intervalAfterFinish,
                    runner = :runner, window_name = :windowName, rate_limit = :rateLimit,
                    misfire = :misfire, start_paused = :startPaused,

                    max_concurrent_executions = :maxConcurrentExecutions,
                    retries = :retries, timeout_micros = :timeout_micros, retry_policy = :retryPolicy,
                    executable_key = :executableKey, recurring_payload = :recurringPayload,
                    input_contract = :inputContract,
                    source = :source, retired = :retired,
                    updated_at = :updatedAt
                WHERE job_key = :jobKey
                """;
    }

    // ---------------------------------------------------------------------------------------------
    // The database itself: clock and the temporal crossing
    // ---------------------------------------------------------------------------------------------

    /**
     * The statement {@code DatabaseClock} samples the server's clock with, paired with
     * {@link #readNow(ResultSet)}, which crosses its result back into an {@link Instant}.
     *
     * <p><b>The pair is the contract, and neither half is portable.</b> Two things vary together and a
     * delegate has to get both right: what the server answers {@code now} with, and whether that answer
     * carries a zone. PostgreSQL and H2 answer {@code TIMESTAMPTZ}, so plain {@code CURRENT_TIMESTAMP}
     * is exact once read as an {@code OffsetDateTime}. SQL Server's {@code CURRENT_TIMESTAMP} is a
     * zoneless {@code DATETIME} and MySQL's is evaluated in the session's {@code time_zone}: read as a
     * {@code java.sql.Timestamp} both are re-interpreted in the JVM's zone, so a node three hours from
     * the server sampled a three-hour "clock offset" that was nothing but the distance between two
     * zones. Both therefore ask for UTC explicitly ({@code SYSUTCDATETIME()},
     * {@code UTC_TIMESTAMP(6)}) and read the answer as a {@code LocalDateTime} that is stated to be UTC
     * — never converted into it.
     *
     * <p>The statement is the delegate's for a second reason: Oracle needs {@code FROM DUAL} and Db2
     * {@code FROM SYSIBM.SYSDUMMY1}, so this shape does not even parse everywhere.
     *
     * <p>Both halves are abstract, and that is the safety property. The question used to be a boolean
     * with a fail-safe default, which a delegate could answer by never thinking about it; an abstract
     * crossing cannot be inherited by accident — an implementation that never considered the zone does
     * not compile.
     *
     * @return the SQL statement for reading the database clock
     */
    String nowQuery();

    /** Exclusive session lock shared by claim, completion, renewal and revocation. */
    default String lockNode() {
        return "SELECT node_id, state, last_heartbeat_at, expires_at, revoked FROM mohs_nodes WHERE node_id = :nodeId FOR UPDATE";
    }


    /**
     * {@link #nowQuery()}'s crossing: the single column it selected, as an instant on the server's clock.
     *
     * @param rs the result set positioned on the row to read
     * @return the database instant in the current result row
     * @throws java.sql.SQLException if the JDBC driver cannot read or write the requested value
     */
    Instant readNow(ResultSet rs) throws SQLException;

    /** Operational timestamps are UTC local JDBC 4.2 values at microsecond precision. */
    default Object splitTimestamp(Instant instant) {
        return JdbcTimestamps.toUtcLocalDateTime(instant);
    }

    /** Reads a nullable operational timestamp without consulting JVM or session time zones. */
    default @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        return JdbcTimestamps.fromUtcLocalDateTimeOrNull(rs.getObject(column, java.time.LocalDateTime.class));
    }

    // ---------------------------------------------------------------------------------------------
    // The claim: mohs_work to mohs_work
    // ---------------------------------------------------------------------------------------------

    /**
     * The claim's candidate sweep: single-shard per statement (a measured lesson — a multi-shard
     * predicate kills the index's ordering), ordered by {@code (priority, visible_at)}, which the
     * {@code idx_mohs_work_claim} index supplies without a Sort. The rows must not be visible to a peer
     * doing the same thing at the same instant: {@code FOR UPDATE SKIP LOCKED} where the database has
     * it, the {@code (UPDLOCK, ROWLOCK, READPAST)} emulation on SQL Server.
     *
     * @return the SQL statement for locking ready candidates in claim order
     */
    String readyCandidates();

    /** Visible work identities in stable key order; one bounded page per shard probe. */
    default String candidateJobKeys() {
        return """
                SELECT DISTINCT job_key FROM mohs_work
                WHERE work_state = 'READY' AND shard = :shard AND job_key > :afterKey AND visible_at <= :now
                ORDER BY job_key LIMIT :limit
                """;
    }

    /** Bounded bulk policy read, using the same explicit projection as single-definition reads. */
    default String findJobsByKeys() {
        return findAllJobs() + " AND job_key IN (:jobKeys)";
    }

    /** Advisory candidates only. Job locks and a fresh occupancy read decide admission afterwards. */
    default String admissionCandidates(boolean restrictJobs) {
        return """
                SELECT r.execution_id, r.job_key, r.attempt, r.priority
                FROM mohs_work r JOIN mohs_job_definitions d ON d.job_key = r.job_key
                WHERE r.work_state = 'READY' AND r.shard = :shard AND r.visible_at <= :now
                  AND d.retired = :inactive
                  AND (d.max_concurrent_executions = 0 OR d.max_concurrent_executions >
                       (SELECT COUNT(*) FROM mohs_work l WHERE l.work_state = 'LEASED' AND l.job_key = d.job_key))
                """ + (restrictJobs ? "AND r.job_key IN (:candidateJobs)\n" : "") + """
                ORDER BY r.priority, r.visible_at, r.execution_id
                LIMIT :limit
                """;
    }

    /**
     * The queue side of the claim: what was swept stops being queued.
     *
     * @return the SQL statement for removing claimed entries from the ready queue
     */
    default String readyDelete() {
        return "DELETE FROM mohs_work WHERE work_state = 'READY' AND execution_id IN (:ids)";
    }

    /**
     * The ownership side of the claim: what was swept becomes this node's, fenced by {@code (node_id, claim_token, attempt_number)}.
     *
     * @return the SQL statement for inserting fenced execution ownership
     */
    default String fencedReleaseWork() {
        return """
                UPDATE mohs_work SET work_state = 'READY', visible_at = :visibleAt, attempt = :nextAttempt,
                    node_id = NULL, claim_token = NULL, claimed_at = NULL, cancel_requested = :cancelRequested
                WHERE execution_id = :executionId AND work_state = 'LEASED'
                  AND node_id = :nodeId AND claim_token = :claimToken AND attempt = :attemptNumber
                """;
    }

    /**
     * The claim's candidate sweep, executed. Overridden only where the SQL's SHAPE forces a different
     * call (Postgres runs the whole claim as one statement and never reaches here).
     *
     * @param jdbcTemplate the template used to execute database operations
     * @param shard the deterministic queue partition
     * @param limit the maximum number of results in one batch
     * @param executionIds the execution identities admitted under the authoritative job locks
     * @param now the current instant from the configured time source
     * @return the locked ready candidates matching the claim
     */
    default List<ClaimedReady> selectReadyCandidates(NamedParameterJdbcTemplate jdbcTemplate, int shard, int limit,
            Collection<String> executionIds, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", (long) limit, java.sql.Types.BIGINT).addValue("ids", executionIds);
        return jdbcTemplate.query(readyCandidates(), params, ClaimedReady::fromReadyRow);
    }

    /**
 * Acquires selected ready identities inside the caller's locked session/job transaction.
 * MySQL/H2 use a locking pick and guarded update; PostgreSQL/SQL Server fold this into one statement.
 * @param jdbcTemplate transaction-bound template
 * @param shard selected shard
 * @param nodeId locked session identity
 * @param claimToken fresh acquisition identity
 * @param limit maximum result count
 * @param executionIds identities admitted under job locks
 * @param now database-bounded eligibility horizon
 * @param grantedAt fresh database observation after admission locks
 * @return acquired rows in priority, visibility and identity order
 */
    default List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, String claimToken,
            int limit, Collection<String> executionIds, Instant now, Instant grantedAt) {
        List<ClaimedReady> picked = selectReadyCandidates(jdbcTemplate, shard, limit, executionIds, now);
        if (picked.isEmpty()) return picked;
        int changed = jdbcTemplate.update(claimPickedWork(), new MapSqlParameterSource()
                .addValue("ids", picked.stream().map(ClaimedReady::executionId).toList())
                .addValue("nodeId", nodeId).addValue("claimToken", claimToken)
                .addValue("grantedAt", splitTimestamp(grantedAt)));
        if (changed != picked.size()) throw new IllegalStateException("locked work changed during claim");
        return picked;
    }



    // ---------------------------------------------------------------------------------------------
    default String claimPickedWork() {
        return """
                UPDATE mohs_work SET work_state = 'LEASED', visible_at = NULL,
                    node_id = :nodeId, claim_token = :claimToken, claimed_at = :grantedAt
                WHERE execution_id IN (:ids) AND work_state = 'READY'
                """;
    }

    // The queue: mohs_work and the fenced lease (JdbcWorkQueue)
    // ---------------------------------------------------------------------------------------------

    /**
 * Initial READY work INSERT owned by atomic acceptance. Retry/requeue update the existing row.
 * @return complete work-birth SQL
 */
    default String readyInsert() {
        return """
                INSERT INTO mohs_work (execution_id, job_key, shard, priority, attempt, visible_at, work_state)
                VALUES (:executionId, :jobKey, :shard, :priority, :attempt, :visibleAt, 'READY')
                """;
    }

    /**
     * Deletes only the exact observed acquisition. The token changes even when a requeue retains
     * the same node and attempt number.
     *
     * <p>The SAME statement decides the fence in the requeue ({@code JdbcWorkQueue}) and in the
     * completion ({@code JdbcLeaseStore}) — one statement so the semantics can never diverge between the
     * two.
     *
     * @return the SQL statement for deleting only the observed lease incarnation
     */
    default String fencedLeaseDelete() {
        return """
                DELETE FROM mohs_work
                WHERE execution_id = :executionId AND work_state = 'LEASED'
                  AND node_id = :nodeId AND claim_token = :claimToken AND attempt = :attemptNumber
                """;
    }

    /**
     * Removing one entry from the queue by its primary key — the DELETE's row lock is what decides the race against a concurrent claim.
     *
     * @return the SQL statement for deleting a queued execution by identity
     */
    default String deleteReadyById() {
        return "DELETE FROM mohs_work WHERE work_state = 'READY' AND execution_id = :executionId";
    }

    /**
     * The advisory terminal for a cancellation, matched by execution id.
     *
     * @return the SQL statement for recording terminal cancellation
     */
    default String cancelExecution() {
        // batch-counted: JdbcExecutionTransitions.finishCancellations increments the accepted batch in this transaction.
        return """
                UPDATE mohs_execution SET state = 'CANCELLED', finished_at = :finishedAt
                WHERE execution_id = :executionId
                """;
    }

    /**
     * The batch a cancelled execution belongs to, so the cancellation counts into it in the same transaction.
     *
     * @return the SQL statement for looking up an execution's batch identity
     */
    default String findBatchIdByExecution() {
        return "SELECT batch_id FROM mohs_execution WHERE execution_id = :executionId";
    }

    /**
     * The manual retry's CAS: it only wins with the advisory {@code FAILED} and the job alive.
     *
     * <p>{@code batch_id IS NULL} because a batch member does not rearm: the batch already counted
     * this failure, and re-running would count the outcome TWICE in a batch that may already be closed
     * (a negative pending, a second {@code BatchCompleted}). The {@code EXISTS} narrows the window
     * against a concurrent {@code remove}.
     *
     * @return the SQL statement for conditionally marking a failed execution pending
     */
    default String rearmExecutionByCas() {
        return """
                UPDATE mohs_execution SET state = 'PENDING', finished_at = NULL
                WHERE execution_id = :executionId AND state = 'FAILED'
                  AND origin = 'MANUAL' AND batch_id IS NULL
                  AND EXISTS (SELECT 1 FROM mohs_job_definitions j
                              WHERE j.job_key = mohs_execution.job_key AND j.retired = :retired)
                """;
    }

    /**
     * The manual retry's rebirth in the queue — the attempt and the priority are derived from history itself, so the caller carries nothing.
     *
     * @return the SQL statement for rebuilding a ready entry for manual retry
     */
    default String rearmReadyFromHistory() {
        return """
                INSERT INTO mohs_work (execution_id, job_key, shard, priority, attempt, visible_at, work_state)
                SELECT e.execution_id, e.job_key, e.shard, e.priority, e.last_attempt_number + 1, :visibleAt, 'READY'
                FROM mohs_execution e WHERE e.execution_id = :executionId
                """;
    }

    /**
     * The idle gate's probe: is there anything visible in THIS node's shards?
     *
     * <p>{@code EXISTS} rather than {@code LIMIT 1}/{@code TOP 1}: it short-circuits the same way and is
     * the same shape in all four delegates — including the locking, which is the load-bearing part. The
     * probe must never take shared locks on the system's hottest table, because what would block is the
     * tick's thread, which carries the heartbeat. All four get that from row versioning, not from a
     * hint: PostgreSQL and MySQL natively, SQL Server because {@code READ_COMMITTED_SNAPSHOT} is a boot
     * requirement of the dialect (see {@code SqlServerRcsiRequirement}).
     *
     * @return the SQL statement for checking for visible queue work
     */
    default String visibleWorkExists() {
        return """
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM mohs_work WHERE work_state = 'READY' AND shard IN (:shards) AND visible_at <= :now
                ) THEN 1 ELSE 0 END
                """;
    }

    /**
     * The backlog gauge. No shard predicate: the backlog is the queue's, not this node's.
     *
     * <p>The non-blocking read matters here for the same reason as the probe above — a metric must
     * never take a shared lock on the hot path it is measuring — and versioned reads keep the number
     * CORRECT besides: the last committed state, not an uncommitted delete's.
     *
     * <p>No index leads with {@code visible_at}, and none is added: a second index on the system's
     * hottest table, paid by every enqueue, claim and requeue, is a bad trade for a number sampled once
     * every ten seconds. The count therefore SCANS, and what it scans is {@code mohs_work} — the
     * queue, never history — which also means its cost grows with the very backlog it is reporting.
     *
     * @return the SQL statement for counting visible queue entries
     */
    String visibleWorkCount();

    // ---------------------------------------------------------------------------------------------
    // History: mohs_execution, mohs_idempotency (JdbcHistoryStore)
    // ---------------------------------------------------------------------------------------------

    /**
     * The execution's birth row, always {@code PENDING} — the derived states live in the queue and in the ownership, never in this column.
     *
     * @return the SQL statement for inserting execution birth records
     */
    default String recordExecution() {
        return """
                INSERT INTO mohs_execution (
                    execution_id, job_key, shard, priority, state, scheduled_at, created_at, actor,
                    batch_id, idempotency_key, payload, input_contract, origin, schedule_revision)
                VALUES (:executionId, :jobKey, :shard, :priority, 'PENDING', :scheduledAt, :createdAt, :actor,
                    :batchId, :idempotencyKey, :payload, :inputContract, :origin, :scheduleRevision)
                """;
    }

    /**
     * The Idempotent Receiver (EIP): the primary-key conflict IS the deduplication check, and it has to abort the unit before any history row is born.
     *
     * @return the SQL statement for claiming an idempotency key
     */
    default String insertIdempotency() {
        return """
                INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at, expires_at, scheduled_at, actor)
                VALUES (:jobKey, :idempotencyKey, :executionId, :createdAt, :expiresAt, :scheduledAt, :actor)
                """;
    }

    /** The receipt retained with the deduplication key, falling back to legacy history. */
    default String findIdempotencyReceipt() {
        return """
                SELECT execution_id, scheduled_at, actor FROM mohs_idempotency
                WHERE job_key = :jobKey AND idempotency_key = :idempotencyKey
                  AND (expires_at IS NULL OR expires_at > :now)
                """;
    }

    /**
     * The receipt lookup that answers a repeated {@code Idempotency-Key} with the execution it already produced.
     *
     * @return the SQL statement for looking up the retained idempotency winner
     */


    /**
     * The dispatcher's batched read: the head plus the payload, for the executions a claim round just took.
     *
     * @return the SQL statement for reading execution payloads
     */
    default String findPayloads() {
        return """
                SELECT execution_id, job_key, scheduled_at, created_at, actor, priority, batch_id,
                       payload, input_contract
                FROM mohs_execution WHERE execution_id IN (:ids)
                """;
    }

    /**
     * The head alone, for the paths that never deserialise a payload.
     *
     * @return the SQL statement for reading execution headers without payloads
     */
    default String findHeads() {
        return """
                SELECT execution_id, job_key, scheduled_at, created_at, actor, priority, batch_id
                FROM mohs_execution WHERE execution_id IN (:ids)
                """;
    }


    /**
     * One page of expired receipts, bounded by :limit and the caller's transaction deadline.
     *
     * @return the SQL statement for deleting expired idempotency keys
     */
    String pruneExpiredIdempotency();

    /**
     * The history sweep's first statement: at most {@code :limit} TERMINAL executions finished before
     * {@code :cutoff}, and born before {@code :cutoffId}. The id bound is the load-bearing part —
     * ids are UUIDv7, time-ordered and lexicographically sortable, so {@code execution_id < :cutoffId}
     * (the smallest id an execution created at the cutoff could have) turns the candidate read into a
     * range scan of the PRIMARY KEY, on every dialect, with no index added to the hottest history
     * table. {@code finished_at} then refines: an old execution that finished recently (a long retry)
     * stays until its finish leaves the window. The terminal-state predicate sits in the DELETE's
     * OWN {@code WHERE} — in the subquery shapes (PostgreSQL/H2) that means it appears twice, because
     * a predicate inside a subquery evaluates against a snapshot and serialises nothing; only the
     * outer predicate is re-evaluated under the row lock, and it is what spares a row a concurrent
     * manual retry rearmed to {@code PENDING} between read and write.
     *
     * @return the SQL statement for deleting a bounded batch of expired terminal executions
     */
    String pruneTerminalExecutionsBefore();


    /**
     * The sweep's last statement: at most {@code :limit} batches born before {@code :cutoffId} with
     * no remaining member. No closed-counter check: an open batch always has a live (non-terminal,
     * therefore unpruned) member, so "no member remains" already spares it — and it spares equally
     * the closed batch whose members' history is still inside the window, keeping
     * {@code GET /batches/{id}} consistent with the members a reader can still see.
     *
     * <p>Unlike the terminal sweep, the emptiness predicate is NOT repeated in the DELETE's own
     * {@code WHERE}: it reads another table, and a subplan over another table keeps the statement's
     * snapshot even when the target row is re-evaluated under its lock (measured on PostgreSQL 17 —
     * the outer copy never saw a member committed concurrently). What closes that race is the
     * write path, not the statement: a batch and all of its members are inserted in one
     * transaction, and nothing adds a member to an existing batch later (a retry reuses the same
     * execution row; the CAS that rearms one requires {@code batch_id IS NULL}). A visible
     * batch with no member is therefore one whose members the previous statement already pruned.
     *
     * @return the SQL statement for deleting a bounded batch of empty batches
     */
    String pruneEmptyBatchesBefore();

    /**
     * The read model by id: the advisory joined to the queue and to the ownership, from which
     * {@code JdbcHistoryStore} derives the state (terminal lives in the column; RUNNING lives in the
     * ownership; ENQUEUED/RETRY_WAITING live in the queue, separated by the visibility rule).
     *
     * @return the SQL statement for reading an execution by identity
     */
    default String findExecutionById() {
        return """
                SELECT e.execution_id, e.job_key, e.state, e.scheduled_at, e.created_at, e.actor, e.priority,
                       e.batch_id, e.idempotency_key,
                       e.last_attempt_number, e.last_attempt_started_at, e.last_attempt_finished_at,
                       e.last_attempt_outcome, e.last_attempt_error,
                       w.node_id AS lease_node, w.claimed_at AS lease_claimed_at,
                       CASE WHEN w.work_state = 'READY' THEN w.execution_id END AS ready_id, w.attempt AS ready_attempt, w.visible_at AS ready_visible_at
                FROM mohs_execution e
                LEFT JOIN mohs_work w ON w.execution_id = e.execution_id
                WHERE e.execution_id = :executionId
                """;
    }

    /**
     * The read model's page over the same three tables, ordered by {@code execution_id DESC} (UUIDv7 —
     * time-ordered by construction, which is what makes the cursor a keyset).
     *
     * @param whereClause the filters the caller assembled, already prefixed with {@code WHERE} and
     *        terminated by a newline, or empty for an unfiltered page. The PREDICATES are the caller's
     *        (they are driven by which filters the request carried); where the row ceiling goes in the
     *        statement is the delegate's, which is the whole reason this is a method and not a constant.
     * @return the SQL statement for reading a filtered page of execution summaries
     */
    String findExecutionPage(String whereClause);

    /**
     * The live counts, whose cost equals the LIVE work by construction: the queue IS the backlog, so
     * history never enters it.
     *
     * @return the SQL statement for counting queued work by derived state
     */
    String countActiveInQueue();

    /**
     * The other half of the live counts: ownership IS what is executing.
     *
     * @return the SQL statement for counting running executions
     */
    String countRunning();

    /**
     * The throughput window: terminal execution outcomes grouped by outcome since an instant.
     *
     * @return the SQL statement for counting terminal outcomes over a time window
     */
    String countTerminalOutcomesSince();

    // ---------------------------------------------------------------------------------------------
    // Ownership: mohs_work and the completion (JdbcLeaseStore)
    // ---------------------------------------------------------------------------------------------

    /** One session's bounded grant page, using the node/claimed-time index. */
    default String nodeLeasePage() {
        return """
                SELECT execution_id, job_key, node_id, claim_token, attempt, priority, claimed_at, cancel_requested
                FROM mohs_work WHERE work_state = 'LEASED' AND node_id = :nodeId
                  AND (claimed_at > :afterTime OR (claimed_at = :afterTime AND execution_id > :afterId))
                ORDER BY claimed_at, execution_id LIMIT :limit
                """;
    }

    /**
     * What a set of nodes owns — the engine's own view of its in-flight work.
     *
     * @return the SQL statement for reading leases owned by the specified nodes
     */
    default String findLeasesByNodes() {
        return """
                SELECT execution_id, job_key, node_id, claim_token, attempt, priority, claimed_at, cancel_requested
                FROM mohs_work WHERE work_state = 'LEASED' AND node_id IN (:nodeIds)
                """;
    }

    /**
     * Grants of durably revoked sessions, oldest first, up to a ceiling.
     * {@code claimed_at} leads so a mass death drains oldest first.
     *
     * @return the SQL statement for finding leases with expired ownership
     */
    String findOrphanedLeases();

    /**
     * {@link #findOrphanedLeases()} restricted to the leases whose owner is NOT among the living.
     *
     * @return the SQL statement for finding orphaned leases while excluding live nodes
     */

    /**
     * How many leases each job holds — the admission control's input for {@code allowConcurrentExecutions=false}.
     *
     * @return the SQL statement for counting live leases by job
     */
    String countLeasesByJob();

    /**
     * The cooperative cancellation flag, raised on the lease the execution is running under.
     *
     * @return the SQL statement for setting a lease's cancellation flag
     */
    default String requestLeaseCancellation() {
        return """
                UPDATE mohs_work SET cancel_requested = :flag
                WHERE execution_id = :executionId AND work_state = 'LEASED'
                """;
    }

    /**
     * Which of the given executions have had cancellation requested — polled by the dispatcher.
     *
     * @return the SQL statement for reading cancellation requests for owned work
     */
    default String findCancelRequestedLeases() {
        return """
                SELECT execution_id FROM mohs_work
                WHERE work_state = 'LEASED' AND execution_id IN (:ids) AND cancel_requested = :flag
                """;
    }



    // ---------------------------------------------------------------------------------------------
    /** SQL for the fenced winner's latest attempt and optional terminal state, in one update. */
    default String completeExecution() {
        return """
                UPDATE mohs_execution SET
                    last_claim_token = :claimToken, last_attempt_number = :number, last_attempt_node_id = :nodeId,
                    last_attempt_started_at = :startedAt, last_attempt_finished_at = :finishedAt,
                    last_attempt_outcome = :outcome, last_attempt_error_type = :errorType,
                    last_attempt_error = :error,
                    state = COALESCE(:terminalState, state), finished_at = :executionFinishedAt
                WHERE execution_id = :executionId
                """;
    }

    // Definitions: mohs_job_definitions (JdbcJobStore)
    // ---------------------------------------------------------------------------------------------

    /**
     * The upsert's UPDATE arm, tried before the INSERT (0 rows affected means a new key) — which avoids
     * the extra round trip, and the time-of-check/time-of-use race, of a prior {@code SELECT COUNT}.
     *
     * @param writeNextFire whether {@code next_fire_at} is part of the SET. Preserving it means NOT
     *        writing the column, because rewriting the value that was read would be a lost update
     *        (DDIA ch. 7) against the firing CAS and the completion's guarded rearm.
     * @return the SQL statement for updating an existing job definition
     */
    default String upsertJobUpdate(boolean writeNextFire) {
        if (writeNextFire) {
            return """
                    UPDATE mohs_job_definitions SET
                        name = :name, schedule_type = :scheduleType,
                        cron_expression = :cronExpression, cron_zone = :cronZone,
                        interval_micros = :intervalMicros, interval_after_finish = :intervalAfterFinish,
                        runner = :runner, window_name = :windowName, rate_limit = :rateLimit,
                        misfire = :misfire, start_paused = :startPaused,

                        max_concurrent_executions = :maxConcurrentExecutions,
                        retries = :retries, timeout_micros = :timeout_micros, retry_policy = :retryPolicy,
                        source = :source, retired = :retired,
                        next_fire_at = :nextFireAt, schedule_revision = schedule_revision + 1, updated_at = :updatedAt
                    WHERE job_key = :jobKey
                    """;
        }
        return """
                UPDATE mohs_job_definitions SET
                    name = :name, schedule_type = :scheduleType,
                    cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_micros = :intervalMicros, interval_after_finish = :intervalAfterFinish,
                    runner = :runner, window_name = :windowName, rate_limit = :rateLimit,
                    misfire = :misfire, start_paused = :startPaused,

                    max_concurrent_executions = :maxConcurrentExecutions,
                    retries = :retries, timeout_micros = :timeout_micros, retry_policy = :retryPolicy,
                    source = :source, retired = :retired,
                    updated_at = :updatedAt
                WHERE job_key = :jobKey
                """;
    }

    /**
     * The upsert's INSERT arm. {@code paused} is written only here: {@code startPaused} applies at birth, and the UPDATE never touches it.
     *
     * @return the SQL statement for inserting a new job definition
     */
    default String insertJob() {
        return """
                INSERT INTO mohs_job_definitions (
                    job_key, name, schedule_type, cron_expression, cron_zone,
                    interval_micros, interval_after_finish, runner, window_name, rate_limit,
                    misfire, start_paused, max_concurrent_executions, retries,
                    timeout_micros, retry_policy, source, executable_key, recurring_payload, input_contract,
                    retired, paused, next_fire_at, created_at, updated_at)
                VALUES (
                    :jobKey, :name, :scheduleType, :cronExpression, :cronZone,
                    :intervalMicros, :intervalAfterFinish, :runner, :windowName, :rateLimit,
                    :misfire, :startPaused, :maxConcurrentExecutions, :retries,
                    :timeout_micros, :retryPolicy, :source, :executableKey, :recurringPayload, :inputContract,
                    :retired, :paused, :nextFireAt, :createdAt, :updatedAt)
                """;
    }

    /**
     * A reconfiguration: the schedule and the recomputed trigger in a single UPDATE — an explicit reconfiguration beats a concurrent firing.
     *
     * @return the SQL statement for replacing a job's schedule and trigger
     */
    default String rescheduleJob() {
        return """
                UPDATE mohs_job_definitions SET
                    schedule_type = :scheduleType, cron_expression = :cronExpression, cron_zone = :cronZone,
                    interval_micros = :intervalMicros, interval_after_finish = :intervalAfterFinish,
                    next_fire_at = :nextFireAt, schedule_revision = schedule_revision + 1, updated_at = :updatedAt
                WHERE job_key = :jobKey AND retired = :retired
                """;
    }

    /**
     * The retirement's cancellation of what the drain actually took out of the queue.
     *
     * @return the SQL statement for marking drained queued executions cancelled
     */
    default String cancelDrainedExecutions() {
        // batch-counted: JdbcJobStore.cancelDrained/countCancelledMembers aggregate and count the drained set in this transaction.
        return """
                UPDATE mohs_execution SET state = 'CANCELLED', finished_at = :now
                WHERE execution_id IN (:ids)
                """;
    }


    /**
     * One statement for pause and resume: the flag is the parameter, so the two differ only in the value bound.
     *
     * @return the SQL statement for changing a job's paused flag
     */
    default String setJobPaused() {
        return "UPDATE mohs_job_definitions SET paused = :paused WHERE job_key = :jobKey";
    }

    /**
     * The soft retire — never a {@code DELETE}: "preserve history" requires the row to stay alive, since history keeps pointing at it.
     *
     * @return the SQL statement for retiring a programmatic job definition
     */
    default String retireJob() {
        return "UPDATE mohs_job_definitions SET retired = :retired, next_fire_at = NULL, schedule_revision = schedule_revision + 1, updated_at = :now WHERE job_key = :jobKey AND retired <> :retired";
    }

    /**
     * The batches the drained executions belonged to, grouped — the retirement counts them in bulk.
     *
     * @return the SQL statement for reading batch members affected by a queue drain
     */
    String drainedBatchMembers();

    /**
     * The bulk count ({@code + :pending} per batch, not N calls) — a shape {@code BatchStore} does not expose.
     *
     * @return the SQL statement for counting cancelled members of a batch
     */
    default String countCancelledBatchMembers() {
        return "UPDATE mohs_batches SET failed = failed + :pending WHERE id = :id";
    }

    /**
     * Read-only trigger snapshot. Mutating decisions consume {@link #lockBindingOwner()} so a host
     * REPEATABLE_READ snapshot cannot hide a concurrent reconfiguration.
     *
     * @return the SQL statement for reading a trigger's current state
     */
    default String findTriggerSnapshot() {
        return """
                SELECT schedule_type, cron_expression, cron_zone, interval_micros, interval_after_finish, next_fire_at, schedule_revision, retired
                FROM mohs_job_definitions WHERE job_key = :jobKey
                """;
    }

    /**
     * One definition by key. {@code retired} stays out of every read as a PARAMETER, not a literal:
     * SQL Server's {@code BIT} does not accept {@code FALSE}.
     *
     * @return the SQL statement for looking up a stored job
     */
    default String findJobByKey() {
        return """
                SELECT job_key, executable_key, input_contract, name, schedule_type, cron_expression, cron_zone,
                    interval_micros, interval_after_finish, runner, window_name, rate_limit,
                    misfire, start_paused, max_concurrent_executions,
                    retries, timeout_micros, retry_policy, source, paused, retired,
                    next_fire_at, schedule_revision, created_at, updated_at
                FROM mohs_job_definitions WHERE job_key = :jobKey AND retired = :retired
                """;
    }

    /**
     * Every live definition — streamed, with the fetch size the cursor needs.
     *
     * @return the SQL statement for streaming all stored jobs
     */
    default String findAllJobs() {
        return """
                SELECT job_key, executable_key, input_contract, name, schedule_type, cron_expression, cron_zone,
                    interval_micros, interval_after_finish, runner, window_name, rate_limit,
                    misfire, start_paused, max_concurrent_executions,
                    retries, timeout_micros, retry_policy, source, paused, retired,
                    next_fire_at, schedule_revision, created_at, updated_at
                FROM mohs_job_definitions WHERE retired = :retired
                """;
    }


    /**
     * The due recurring triggers, oldest first, at most {@code :limit} — the ceiling is in the SQL so
     * the planner walks {@code idx_mohs_job_next_fire} in order and stops, instead of the driver
     * materialising every due row before Java can cut the list (see {@code JdbcJobStore#findDueRecurring}
     * for the measurement that moved it).
     *
     * @return the SQL statement for reading active recurring jobs with due triggers
     */
    String findDueRecurringJobs();

    /**
     * The completion's guarded rearm: the cure only arms a DISARMED trigger.
     *
     * @return the SQL statement for arming a disarmed trigger
     */
    default String armNextFire() {
        return "UPDATE mohs_job_definitions SET next_fire_at = :nextFireAt WHERE job_key = :jobKey AND next_fire_at IS NULL AND schedule_revision = :scheduleRevision AND retired = :retired";
    }

    /**
     * Current queued work, locked after its job for atomic bulk retirement.
     *
     * @return the SQL statement for reading a job's queued execution identities
     */
    default String findQueuedExecutionIdsByJob() {
        return "SELECT execution_id FROM mohs_work WHERE work_state = 'READY' AND job_key = :jobKey ORDER BY execution_id FOR UPDATE";
    }

    // ---------------------------------------------------------------------------------------------
    // Nodes: mohs_nodes (JdbcNodeStore)
    // ---------------------------------------------------------------------------------------------

    /**
     * A node's first heartbeat, reached only when the UPDATE found no row.
     *
     * @return the SQL statement for registering a node heartbeat
     */
    default String insertNode() {
        return """
                INSERT INTO mohs_nodes (node_id, state, last_heartbeat_at, expires_at, revoked)
                VALUES (:nodeId, :state, :lastHeartbeatAt, :expiresAt, :revoked)
                """;
    }

    /**
     * The cluster view behind {@code GET /nodes}.
     *
     * @return the SQL statement for reading the node heartbeat registry
     */
    default String findAllNodes() {
        return "SELECT node_id, state, last_heartbeat_at, expires_at, revoked FROM mohs_nodes";
    }

    /**
     * The reaper's cleanup of nodes that stopped heartbeating long enough ago to be gone for good.
     *
     * @return the SQL statement for deleting stale node heartbeats
     */
    default String deleteHeartbeatsBefore() {
        return "DELETE FROM mohs_nodes WHERE last_heartbeat_at < :cutoff AND revoked = :revoked AND NOT EXISTS (SELECT 1 FROM mohs_work l WHERE l.work_state = 'LEASED' AND l.node_id = mohs_nodes.node_id)";
    }

    /**
     * Renewal of an active session after its row lock and database-time validation.
     *
     * @return the SQL statement for refreshing a node's liveness promise
     */
    default String heartbeatUpdate() {
        return """
                UPDATE mohs_nodes SET state = :state, last_heartbeat_at = :lastHeartbeatAt,
                    expires_at = :expiresAt
                WHERE node_id = :nodeId
                """;
    }

    // ---------------------------------------------------------------------------------------------
    // Rate limits: mohs_rate_limits (JdbcRateLimitStore)
    // ---------------------------------------------------------------------------------------------

    /**
     * One limit's full row, for the dashboard read.
     *
     * @return the SQL statement for looking up a rate-limit bucket
     */
    default String findRateLimitByName() {
        return """
                SELECT name, max_count, window_micros, tokens, refilled_at, refill_remainder FROM mohs_rate_limits WHERE name = :name
                """;
    }

    /**
     * Every limit, streamed.
     *
     * @return the SQL statement for streaming declared rate-limit buckets
     */
    default String findAllRateLimits() {
        return """
                SELECT name, max_count, window_micros, tokens, refilled_at, refill_remainder FROM mohs_rate_limits
                """;
    }

    /**
     * The token bucket's four columns — the balance and everything that entered its calculation.
     *
     * @return the SQL statement for reading a bucket's balance and refill instant
     */
    default String readRateLimitBucket() {
        return """
                SELECT max_count, window_micros, tokens, refilled_at, refill_remainder FROM mohs_rate_limits WHERE name = :name
                """;
    }

    /**
     * A spec change, clamping a balance that the new ceiling no longer allows.
     *
     * @return the SQL statement for updating a rate-limit specification
     */
    default String updateRateLimitSpec() {
        return """
                UPDATE mohs_rate_limits
                   SET tokens = :tokens, refilled_at = :refilledAt, refill_remainder = 0,
                       max_count = :maxCount, window_micros = :windowMicros
                 WHERE name = :name AND tokens = :expectedTokens AND refilled_at = :expectedRefilledAt
                   AND max_count = :expectedMax AND window_micros = :expectedWindow AND refill_remainder = :expectedRemainder
                """;
    }

    /**
     * A limit's birth, with a full bucket.
     *
     * @return the SQL statement for creating a full rate-limit bucket
     */
    default String insertFullRateLimitBucket() {
        return """
                INSERT INTO mohs_rate_limits (name, max_count, window_micros, tokens, refilled_at, refill_remainder)
                VALUES (:name, :maxCount, :windowMicros, :maxCount, :refilledAt, 0)
                """;
    }

    /**
     * The charge: a CAS over the {@code (tokens, refilled_at)} pair — atomic by construction, like every
     * guarded {@code UPDATE} in the engine, rather than through a specialised lock. The refill and the
     * charge travel in the SAME statement: applying the refill separately would open a window for
     * another node to charge on top of an already refilled balance, which is over-delivery.
     *
     * <p>The guard covers the FOUR columns that entered the calculation, not only the two it writes:
     * {@code max_count} and {@code window_micros} define the refill interval, and a PATCH widening the
     * window mid-round would refill the balance at the old rate — a silent burst above the new limit.
     *
     * @return the SQL statement for conditionally charging a rate-limit bucket
     */
    default String chargeRateLimitByCas() {
        return """
                UPDATE mohs_rate_limits
                   SET tokens = :tokens, refilled_at = :refilledAt, refill_remainder = :remainder
                 WHERE name = :name AND tokens = :expectedTokens AND refilled_at = :expectedRefilledAt
                   AND max_count = :expectedMax AND window_micros = :expectedWindow AND refill_remainder = :expectedRemainder
                """;
    }

    // ---------------------------------------------------------------------------------------------
    // Batches: mohs_batches (JdbcBatchStore)
    // ---------------------------------------------------------------------------------------------

    /**
     * A batch's receipt row, born with both counters at zero.
     *
     * @return the SQL statement for creating batch counters
     */
    default String insertBatch() {
        return """
                INSERT INTO mohs_batches (id, name, total, succeeded, failed, created_at)
                VALUES (:id, :name, :total, 0, 0, :createdAt)
                """;
    }

    /**
     * Explicit columns rather than {@code SELECT *}: {@code created_at} is operator forensics, never read here, and this re-read runs on EVERY member completion.
     *
     * @return the SQL statement for reading batch counters
     */
    default String findBatch() {
        return "SELECT id, name, total, succeeded, failed FROM mohs_batches WHERE id = :id";
    }

    /**
     * A member that succeeded.
     *
     * @return the SQL statement for incrementing successful batch members
     */
    default String incrementBatchSucceeded() {
        return "UPDATE mohs_batches SET succeeded = succeeded + 1 WHERE id = :id";
    }

    /**
     * A member that failed — or was cancelled, which the batch counts as a failure (it answers "how many succeeded").
     *
     * @return the SQL statement for incrementing failed batch members
     */
    default String incrementBatchFailed() {
        return "UPDATE mohs_batches SET failed = failed + 1 WHERE id = :id";
    }

    // ---------------------------------------------------------------------------------------------
    // The firing: mohs_job_definitions' trigger (JdbcSubmissionStore)
    // ---------------------------------------------------------------------------------------------

    /**
     * The firing CAS — exactly one node in the cluster advances a given trigger.
     *
     * <p>{@code retired} sits in the predicate because a {@code Mohs.remove} between the sweep and this
     * CAS has already cancelled what was in the queue — inserting occurrences AFTER that sweep would
     * leave them as zombies until an eventual resurrection.
     *
     * @return the SQL statement for advancing the observed trigger instant
     */
    default String advanceTriggerByCas() {
        return """
                UPDATE mohs_job_definitions SET next_fire_at = :newNextFireAt
                WHERE job_key = :jobKey AND next_fire_at = :observedNextFireAt AND schedule_revision = :observedRevision
                  AND retired = :retired AND paused = :paused
                """;
    }
}
