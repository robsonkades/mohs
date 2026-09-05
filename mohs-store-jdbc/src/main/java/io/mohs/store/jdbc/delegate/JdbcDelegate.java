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
 * Every statement {@code io.mohs.store.jdbc} issues, answered by the delegate of the database it is
 * issued against — the shape Quartz uses ({@code StdJDBCDelegate}/{@code MSSQLDelegate}: each Delegate
 * holds the COMPLETE SQL for each operation), not Hibernate's fragment handlers.
 *
 * <p><b>No fragment is ever concatenated at runtime.</b> A delegate returns the whole statement, so what
 * a reader sees in {@code SqlServerJdbcDelegate} is literally the T-SQL the server receives. That was the
 * point of the shape even when only the claim lived here: SQL Server's {@code TOP} changes POSITION in
 * the query (right after {@code SELECT}, not at the end like {@code LIMIT}), so a composition of generic
 * fragments does not close cleanly — which is also how Hibernate implements {@code LimitHandler}
 * underneath (it receives the SQL and returns the rewritten SQL, not a fragment).
 *
 * <p><b>Every statement method is abstract on purpose.</b> A default implementation would let a delegate
 * inherit text written for another database in silence; without one, adding a statement breaks
 * compilation in all four delegates at once and each has to answer for its own SQL. The price is
 * deliberate and accepted: text that is identical today exists four times, and a correction has to be
 * applied four times — {@code JdbcDelegateStatementDriftTest} guards the parameter contract, and nothing
 * guards the rest but review.
 *
 * <p>The rationale for each statement's SHAPE lives here, on the method, and is not repeated in the
 * implementations: what is per-delegate is the SQL, not the argument for it.
 *
 * <p>Which delegate to use is an explicit choice, never auto-detection — the same pattern as Quartz's
 * ({@code org.quartz.jobStore.driverDelegateClass}): detecting through {@code Connection.getMetaData()}
 * is fragile across driver forks and versions.
 */
public interface JdbcDelegate {

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
     */
    String nowQuery();

    /** {@link #nowQuery()}'s crossing: the single column it selected, as an instant on the server's clock. */
    Instant readNow(ResultSet rs) throws SQLException;

    /**
     * The temporal crossing for the split tables: they were born {@code TIMESTAMPTZ} on Postgres and
     * remain zoneless in the functional equivalents.
     *
     * <p>The usual crossing is a UTC {@code LocalDateTime}; Postgres uses {@code OffsetDateTime} — a
     * {@code LocalDateTime} against a tz-aware column would be interpreted in the SESSION's zone, the
     * class of bug that crossing was introduced to kill. The control-plane tables
     * ({@code mohs_job_definitions}/{@code mohs_nodes}/{@code mohs_batches}) still go through
     * {@link JdbcTimestamps} directly — reshaping them is a phase of its own.
     */
    Object splitTimestamp(Instant instant);

    /** {@link #splitTimestamp}'s inverse, reading the column back out of a {@code ResultSet}. */
    @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException;

    // ---------------------------------------------------------------------------------------------
    // The claim: mohs_ready to mohs_lease
    // ---------------------------------------------------------------------------------------------

    /**
     * The claim's candidate sweep: single-shard per statement (a measured lesson — a multi-shard
     * predicate kills the index's ordering), ordered by {@code (priority, visible_at)}, which the
     * {@code idx_mohs_ready_claim} index supplies without a Sort. The rows must not be visible to a peer
     * doing the same thing at the same instant: {@code FOR UPDATE SKIP LOCKED} where the database has
     * it, the {@code (UPDLOCK, ROWLOCK, READPAST)} emulation on SQL Server.
     */
    String readyCandidates();

    /**
     * {@link #readyCandidates()} with the inadmissible keys excluded — a separate statement rather than a
     * concatenation because a {@code NOT IN} over an empty list does not expand.
     */
    String readyCandidatesFiltered();

    /** The queue side of the claim: what was swept stops being queued. */
    String readyDelete();

    /** The ownership side of the claim: what was swept becomes this node's, fenced by {@code (node_id, epoch, attempt_number)}. */
    String leaseInsert();

    /**
     * The claim's candidate sweep, executed. Overridden only where the SQL's SHAPE forces a different
     * call (Postgres runs the whole claim as one statement and never reaches here).
     */
    default List<ClaimedReady> selectReadyCandidates(NamedParameterJdbcTemplate jdbcTemplate, int shard, int limit,
            Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(readyCandidates(), params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(readyCandidatesFiltered(),
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
    }

    /**
     * The claim: it removes from the queue and inserts the ownership — the CALLER
     * ({@code JdbcWorkQueue}) guarantees the transaction that makes both effects atomic (there is no
     * instant of "neither queued nor owned"). The portable form is three statements (a locking SELECT, a
     * DELETE, then a batched INSERT); Postgres overrides it with a single
     * {@code WITH picked … DELETE … USING → INSERT … SELECT} statement, and SQL Server folds the SELECT
     * and the DELETE into one {@code DELETE … OUTPUT} and keeps the batched INSERT
     * ({@link #insertLeases}).
     */
    default List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, long epoch,
            int limit, Collection<String> inadmissibleJobKeys, Instant now) {
        List<ClaimedReady> picked = selectReadyCandidates(jdbcTemplate, shard, limit, inadmissibleJobKeys, now);
        if (picked.isEmpty()) {
            return picked;
        }
        jdbcTemplate.update(readyDelete(), new MapSqlParameterSource()
                .addValue("ids", picked.stream().map(ClaimedReady::executionId).toList()));
        insertLeases(jdbcTemplate, picked, nodeId, epoch, now);
        return picked;
    }

    /** The ownership side of the claim, executed: one {@link #leaseInsert()} per picked row, batched. */
    default void insertLeases(NamedParameterJdbcTemplate jdbcTemplate, List<ClaimedReady> picked, String nodeId, long epoch,
            Instant now) {
        Object claimedAt = splitTimestamp(now);
        MapSqlParameterSource[] leases = picked.stream()
                .map(row -> new MapSqlParameterSource()
                        .addValue("executionId", row.executionId())
                        .addValue("jobKey", row.jobKey())
                        .addValue("nodeId", nodeId)
                        .addValue("epoch", epoch)
                        .addValue("attempt", row.attempt())
                        .addValue("priority", row.priority())
                        .addValue("now", claimedAt))
                .toArray(MapSqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(leaseInsert(), leases);
    }

    // ---------------------------------------------------------------------------------------------
    // The queue: mohs_ready and the fenced lease (JdbcWorkQueue)
    // ---------------------------------------------------------------------------------------------

    /**
     * The {@code INSERT} into {@code mohs_ready} — enqueue, retry and requeue are the SAME operation with
     * a different {@code visibleAt}: {@code JdbcWorkQueue#offer}/{@code #requeue} and the retry's rebirth
     * inside {@code JdbcLeaseStore#complete}'s completion transaction share this statement.
     */
    String readyInsert();

    /**
     * The {@code DELETE} from {@code mohs_lease} fenced by {@code (node_id, epoch, attempt_number)} — the
     * fencing token: the lease only drops if it still belongs to the observed incarnation. The attempt
     * is part of it because a node's epoch only moves when its own lease expires: after a Watchdog
     * Bound released an incarnation, the SAME node re-claims the retry with the same {@code (node_id,
     * epoch)}, and the zombie's completion would otherwise pass the fence and delete the new
     * incarnation's lease. The attempt number is what tells the two apart.
     *
     * <p>The SAME statement decides the fence in the requeue ({@code JdbcWorkQueue}) and in the
     * completion ({@code JdbcLeaseStore}) — one statement so the semantics can never diverge between the
     * two.
     */
    String fencedLeaseDelete();

    /** Removing one entry from the queue by its primary key — the DELETE's row lock is what decides the race against a concurrent claim. */
    String deleteReadyById();

    /** The advisory terminal for a cancellation, matched by id — the same match {@link #terminalStateUpdate()} makes. */
    String cancelExecution();

    /** The batch a cancelled execution belongs to, so the cancellation counts into it in the same transaction. */
    String findBatchIdByExecution();

    /**
     * The manual retry's CAS: it only wins with the advisory {@code FAILED} and the job alive.
     *
     * <p>{@code correlation_id IS NULL} because a batch member does not rearm: the batch already counted
     * this failure, and re-running would count the outcome TWICE in a batch that may already be closed
     * (a negative pending, a second {@code BatchCompleted}). The {@code EXISTS} narrows the window
     * against a concurrent {@code remove}.
     */
    String rearmExecutionByCas();

    /** The manual retry's rebirth in the queue — the attempt and the priority are derived from history itself, so the caller carries nothing. */
    String rearmReadyFromHistory();

    /**
     * The idle gate's probe: is there anything visible in THIS node's shards?
     *
     * <p>{@code EXISTS} rather than {@code LIMIT 1}/{@code TOP 1}: it short-circuits the same way and is
     * the same shape in all four delegates — including the locking, which is the load-bearing part. The
     * probe must never take shared locks on the system's hottest table, because what would block is the
     * tick's thread, which carries the heartbeat. All four get that from row versioning, not from a
     * hint: PostgreSQL and MySQL natively, SQL Server because {@code READ_COMMITTED_SNAPSHOT} is a boot
     * requirement of the dialect (see {@code SqlServerRcsiRequirement}).
     */
    String visibleWorkExists();

    /**
     * The backlog gauge. No shard predicate: the backlog is the queue's, not this node's.
     *
     * <p>The non-blocking read matters here for the same reason as the probe above — a metric must
     * never take a shared lock on the hot path it is measuring — and versioned reads keep the number
     * CORRECT besides: the last committed state, not an uncommitted delete's.
     *
     * <p>No index leads with {@code visible_at}, and none is added: a second index on the system's
     * hottest table, paid by every enqueue, claim and requeue, is a bad trade for a number sampled once
     * every ten seconds. The count therefore SCANS, and what it scans is {@code mohs_ready} — the
     * queue, never history — which also means its cost grows with the very backlog it is reporting.
     */
    String visibleWorkCount();

    // ---------------------------------------------------------------------------------------------
    // History: mohs_execution, mohs_attempt, mohs_idempotency (JdbcHistoryStore)
    // ---------------------------------------------------------------------------------------------

    /** The execution's birth row, always {@code PENDING} — the derived states live in the queue and in the ownership, never in this column. */
    String recordExecution();

    /** The Idempotent Receiver (EIP): the primary-key conflict IS the deduplication check, and it has to abort the unit before any history row is born. */
    String insertIdempotency();

    /** The receipt lookup that answers a repeated {@code Idempotency-Key} with the execution it already produced. */
    String findExecutionIdByIdempotencyKey();

    /** The dispatcher's batched read: the head plus the payload, for the executions a claim round just took. */
    String findPayloads();

    /** The head alone, for the paths that never deserialise a payload. */
    String findHeads();

    /** An execution's attempts, in order — the detail view's second read (attempts in a join would multiply the row). */
    String findAttempts();

    /** The idempotency window's prune, bounded in TIME by the caller's query timeout rather than in rows. */
    String pruneIdempotencyBefore();

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
     */
    String pruneTerminalExecutionsBefore();

    /**
     * The sweep's second statement: at most {@code :limit} attempts finished before {@code :cutoff}
     * whose execution no longer exists. Orphanhood, not a carried id list, is the predicate on
     * purpose: it makes the statement self-sufficient across crashes (whatever the previous sweep
     * deleted, this one's own predicate finds the leftover attempts) and race-free (an execution that
     * survived the guarded delete keeps its attempts, and the attempt COUNT a rearm derives from
     * stays correct).
     *
     * <p>The measured cost of that self-sufficiency: on PostgreSQL and SQL Server the planner answers
     * this with a hash ANTI-JOIN over both tables — the {@code finished_at} predicate matches nearly
     * every attempt, so no index makes it selective — which is a pass proportional to the tables'
     * TOTAL size, not to {@code :limit} (~80 ms at half a million rows; MySQL probes the parent PK
     * per row and is O(limit) already). That fits the 1s statement timeout until the tables reach the
     * low millions; the O(limit) redesign for beyond that — the executions delete returning its ids,
     * this one probing them, orphanhood kept as the crash backstop — is recorded with its measured
     * trigger in the retention decision record.
     */
    String pruneOrphanedAttemptsBefore();

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
     * execution row; the CAS that rearms one requires {@code correlation_id IS NULL}). A visible
     * batch with no member is therefore one whose members the previous statement already pruned.
     */
    String pruneEmptyBatchesBefore();

    /**
     * The read model by id: the advisory joined to the queue and to the ownership, from which
     * {@code JdbcHistoryStore} derives the state (terminal lives in the column; RUNNING lives in the
     * ownership; ENQUEUED/RETRY_WAITING live in the queue, separated by the visibility rule).
     */
    String findExecutionById();

    /**
     * The read model's page over the same three tables, ordered by {@code execution_id DESC} (UUIDv7 —
     * time-ordered by construction, which is what makes the cursor a keyset).
     *
     * @param whereClause the filters the caller assembled, already prefixed with {@code WHERE} and
     *        terminated by a newline, or empty for an unfiltered page. The PREDICATES are the caller's
     *        (they are driven by which filters the request carried); where the row ceiling goes in the
     *        statement is the delegate's, which is the whole reason this is a method and not a constant.
     */
    String findExecutionPage(String whereClause);

    /**
     * The live counts, whose cost equals the LIVE work by construction: the queue IS the backlog, so
     * history never enters it.
     */
    String countActiveInQueue();

    /** The other half of the live counts: ownership IS what is executing. */
    String countRunning();

    /** The throughput window: terminal outcomes grouped by outcome since an instant. */
    String countTerminalOutcomesSince();

    // ---------------------------------------------------------------------------------------------
    // Ownership: mohs_lease and the completion (JdbcLeaseStore)
    // ---------------------------------------------------------------------------------------------

    /** What a set of nodes owns — the engine's own view of its in-flight work. */
    String findLeasesByNodes();

    /**
     * Every lease, oldest first, up to a ceiling: the reaper's sweep when no node is known to be alive.
     * {@code claimed_at} leads so a mass death drains oldest first.
     */
    String findOrphanedLeases();

    /** {@link #findOrphanedLeases()} restricted to the leases whose owner is NOT among the living. */
    String findOrphanedLeasesExceptAlive();

    /** How many leases each job holds — the admission control's input for {@code allowConcurrentExecutions=false}. */
    String countLeasesByJob();

    /** The cooperative cancellation flag, raised on the lease the execution is running under. */
    String requestLeaseCancellation();

    /** Which of the given executions have had cancellation requested — polled by the dispatcher. */
    String findCancelRequestedLeases();

    /** The confirmed attempt, written in the completion transaction. */
    String insertAttempt();

    /**
     * The advisory terminal, matched by the primary key — one statement for both completion paths, the
     * dispatcher's and the reaper's.
     *
     * <p>There used to be two, differing only in an extra {@code created_at = :createdAt}: on the
     * partitioned schema the hot path carried the value so Postgres could prune partitions, and the
     * reaper, which never loaded it, needed a version without. Neither the pruning nor the second
     * statement outlived the partitioning — the plans measured identical (4 buffers, 0.047 ms, with
     * {@code created_at} demoted to a {@code Filter} even when present). What kept them apart was a
     * guarantee rather than a plan: {@code execution_id} alone was not unique by the schema. Now that
     * it is the primary key, it is.
     */
    String terminalStateUpdate();

    // ---------------------------------------------------------------------------------------------
    // Definitions: mohs_job_definitions (JdbcJobStore)
    // ---------------------------------------------------------------------------------------------

    /**
     * The upsert's UPDATE arm, tried before the INSERT (0 rows affected means a new key) — which avoids
     * the extra round trip, and the time-of-check/time-of-use race, of a prior {@code SELECT COUNT}.
     *
     * @param writeNextFire whether {@code next_fire_at} is part of the SET. Preserving it means NOT
     *        writing the column, because rewriting the value that was read would be a lost update
     *        (DDIA ch. 7) against the firing CAS and the completion's guarded rearm.
     */
    String upsertJobUpdate(boolean writeNextFire);

    /** The upsert's INSERT arm. {@code paused} is written only here: {@code startPaused} applies at birth, and the UPDATE never touches it. */
    String insertJob();

    /** A reconfiguration: the schedule and the recomputed trigger in a single UPDATE — an explicit reconfiguration beats a concurrent firing. */
    String rescheduleJob();

    /**
     * Whether the scheduler still has a live occurrence of this job — "live" in the split layout is the
     * advisory still {@code PENDING}: queued, running or in backoff, all of them PENDING until the
     * terminal outcome.
     */
    String countLiveSchedulerOccurrences();

    /** The retirement's cancellation of what the drain actually took out of the queue. */
    String cancelDrainedExecutions();

    /** The self-healing mark for a definition whose {@code handler_type} no longer resolves. */
    String markJobOrphaned();

    /** One statement for pause and resume: the flag is the parameter, so the two differ only in the value bound. */
    String setJobPaused();

    /** The soft retire — never a {@code DELETE}: "preserve history" requires the row to stay alive, since history keeps pointing at it. */
    String retireJob();

    /** The batches the drained executions belonged to, grouped — the retirement counts them in bulk. */
    String drainedBatchMembers();

    /** The bulk count ({@code + :pending} per batch, not N calls) — a shape {@code BatchStore} does not expose. */
    String countCancelledBatchMembers();

    /** The persisted schedule plus {@code next_fire_at} — what the upsert compares to decide preserve versus recompute. */
    String findTriggerSnapshot();

    /**
     * One definition by key. {@code retired} stays out of every read as a PARAMETER, not a literal:
     * SQL Server's {@code BIT} does not accept {@code FALSE}.
     */
    String findJobByKey();

    /** Every live definition — streamed, with the fetch size the cursor needs. */
    String findAllJobs();

    /** Every live definition born from an annotation — the scan's reconciliation reads this. */
    String findAllAnnotationSourcedJobs();

    /**
     * The due recurring triggers, oldest first, at most {@code :limit} — the ceiling is in the SQL so
     * the planner walks {@code idx_mohs_job_next_fire} in order and stops, instead of the driver
     * materialising every due row before Java can cut the list (see {@code JdbcJobStore#findDueRecurring}
     * for the measurement that moved it).
     */
    String findDueRecurringJobs();

    /** The completion's guarded rearm: the cure only arms a DISARMED trigger. */
    String armNextFire();

    /** What a job still has queued, read before the retirement drains it row by row. */
    String findQueuedExecutionIdsByJob();

    // ---------------------------------------------------------------------------------------------
    // Nodes: mohs_nodes (JdbcNodeStore)
    // ---------------------------------------------------------------------------------------------

    /** A node's first heartbeat, reached only when the UPDATE found no row. */
    String insertNode();

    /** The cluster view behind {@code GET /nodes}. */
    String findAllNodes();

    /** The reaper's cleanup of nodes that stopped heartbeating long enough ago to be gone for good. */
    String deleteHeartbeatsBefore();

    /** The heartbeat itself — state, instant, epoch and lease expiry in one write. */
    String heartbeatUpdate();

    // ---------------------------------------------------------------------------------------------
    // Rate limits: mohs_rate_limits (JdbcRateLimitStore)
    // ---------------------------------------------------------------------------------------------

    /** One limit's full row, for the dashboard read. */
    String findRateLimitByName();

    /** Every limit, streamed. */
    String findAllRateLimits();

    /** The token bucket's four columns — the balance and everything that entered its calculation. */
    String readRateLimitBucket();

    /** A spec change, clamping a balance that the new ceiling no longer allows. */
    String updateRateLimitSpec();

    /** A limit's birth, with a full bucket. */
    String insertFullRateLimitBucket();

    /**
     * The charge: a CAS over the {@code (tokens, refilled_at)} pair — atomic by construction, like every
     * guarded {@code UPDATE} in the engine, rather than through a specialised lock. The refill and the
     * charge travel in the SAME statement: applying the refill separately would open a window for
     * another node to charge on top of an already refilled balance, which is over-delivery.
     *
     * <p>The guard covers the FOUR columns that entered the calculation, not only the two it writes:
     * {@code max_count} and {@code window_duration} define the refill interval, and a PATCH widening the
     * window mid-round would refill the balance at the old rate — a silent burst above the new limit.
     */
    String chargeRateLimitByCas();

    // ---------------------------------------------------------------------------------------------
    // Batches: mohs_batches (JdbcBatchStore)
    // ---------------------------------------------------------------------------------------------

    /** A batch's receipt row, born with both counters at zero. */
    String insertBatch();

    /** Explicit columns rather than {@code SELECT *}: {@code created_at} is operator forensics, never read here, and this re-read runs on EVERY member completion. */
    String findBatch();

    /** A member that succeeded. */
    String incrementBatchSucceeded();

    /** A member that failed — or was cancelled, which the batch counts as a failure (it answers "how many succeeded"). */
    String incrementBatchFailed();

    // ---------------------------------------------------------------------------------------------
    // The firing: mohs_job_definitions' trigger (JdbcTriggerFirer)
    // ---------------------------------------------------------------------------------------------

    /**
     * The firing CAS — exactly one node in the cluster advances a given trigger.
     *
     * <p>{@code retired} sits in the predicate because a {@code Mohs.remove} between the sweep and this
     * CAS has already cancelled what was in the queue — inserting occurrences AFTER that sweep would
     * leave them as zombies until an eventual resurrection.
     */
    String advanceTriggerByCas();
}
