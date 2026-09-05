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
package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * HISTORY ({@code mohs_execution}/{@code mohs_attempt}/{@code mohs_idempotency}).
 *
 * <p>Append plus ONE terminal update per execution; the tables are flat in every database, and future
 * retention is a batched DELETE. The {@code state} here is an ADVISORY read model: while work is in
 * flight, the truth is the lease — reads that need truth join {@link LeaseStore}, reads that need
 * speed (the dashboard) use the column and accept the bounded staleness of one flush.
 *
 * <p>The terminal UPDATE and the attempt INSERT do NOT go through here — they belong to
 * {@link LeaseStore#complete}'s completion transaction: the ports follow the CONCEPTS (queue,
 * ownership, history, control), and a completion is a concept of ownership that touches history,
 * not the other way round.
 */
public interface HistoryStore {

    /**
     * An accepted execution, ready for its birth record.
     *
     * <p>{@code createdAt} is the enqueue instant — a column of the row, no longer part of its
     * identity: the primary key is {@code execution_id} on every database, so nothing about it needs
     * to travel to the completion. {@code correlationId} carries the batch until it is generalised.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param shard the deterministic queue partition
     * @param priority the ordering priority used when claiming work
     * @param scheduledAt the intended firing instant
     * @param createdAt the enqueue instant
     * @param actor the identity attributed to the operation
     * @param correlationId the batch identity, or {@code null} for an independent execution
     * @param idempotencyKey the optional key used to deduplicate scheduling requests
     * @param payload the input passed to the job handler
     */
    record NewExecution(ExecutionId executionId, JobKey jobKey, int shard, int priority, Instant scheduledAt,
            Instant createdAt, String actor, @Nullable String correlationId, @Nullable String idempotencyKey,
            Object payload) {
        /**
         * Creates a {@code NewExecution} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param jobKey the stable identity of the job
         * @param shard the deterministic queue partition
         * @param priority the ordering priority used when claiming work
         * @param scheduledAt the intended firing instant
         * @param createdAt the enqueue instant
         * @param actor the identity attributed to the operation
         * @param correlationId the batch identity, or {@code null} for an independent execution
         * @param idempotencyKey the optional key used to deduplicate scheduling requests
         * @param payload the input passed to the job handler
         */
        public NewExecution {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(scheduledAt, "scheduledAt");
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(payload, "payload");
        }
    }

    /**
     * The birth record: an {@code INSERT} into history with {@code state = 'PENDING'} and, when there
     * is an {@code idempotencyKey}, an {@code INSERT} into {@code mohs_idempotency} — whose primary
     * key conflict IS the deduplication check (Idempotent Receiver, EIP): it propagates as a
     * {@code DuplicateKeyException} for the caller to resolve the winning execution through
     * {@link #findByIdempotencyKey}.
     *
     * <p>It does NOT open a transaction of its own: the caller MUST compose {@code record} plus
     * {@code WorkQueue.offer} into a single transaction — see {@code WorkQueue#offer}'s contract;
     * outside a transaction, a partial failure leaves either an orphan key or an unreachable
     * execution. With N executions and ONE duplicated key, the WHOLE unit aborts — per-item
     * resolution is the caller's job (retrying without the duplicate), not this port's.
     *
     * @param executions the executions to process
     */
    void record(List<NewExecution> executions);

    /**
     * Who won the idempotency race — the id recorded in {@code mohs_idempotency} for (job, key); empty when the key was never used (or has been pruned).
     *
     * @param jobKey the stable identity of the job
     * @param idempotencyKey the optional key used to deduplicate scheduling requests
     * @return the winning execution identity, or empty when no retained key matches
     */
    Optional<ExecutionId> findByIdempotencyKey(JobKey jobKey, String idempotencyKey);

    /**
     * The batched read that follows each claim round: the payload plus the header of the claimed ids
     * (dispatch needs {@code scheduledAt}/{@code actor}/{@code priority}/{@code batchId} for events,
     * rearming and completion — one read, never N).
     *
     * <p>Infrastructure and row failures are separated: a deserialisation failure of ONE row does not
     * take down the batch — that row enters {@code unreadable} and the rest dispatches.
     *
     * @param ids the execution identities to look up
     * @return the decoded rows and per-execution decoding failures
     */
    PayloadBatch findPayloads(List<ExecutionId> ids);

    /**
     * The hydrated payload plus the execution's header — what dispatch, the events and the completion need about the row.
     *
     * @param head the execution header without its payload or attempts
     * @param payload the input passed to the job handler
     */
    record PayloadRow(ExecutionHead head, Object payload) {
        /**
         * Creates a {@code PayloadRow} with the supplied values.
         *
         * @param head the execution header without its payload or attempts
         * @param payload the input passed to the job handler
         */
        public PayloadRow {
            Objects.requireNonNull(head, "head");
            Objects.requireNonNull(payload, "payload");
        }
    }

    /**
     * One history row's header — everything but the payload and the attempts; {@code correlationId} carries the batch.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param scheduledAt the intended firing instant
     * @param createdAt the enqueue instant
     * @param actor the identity attributed to the operation
     * @param priority the ordering priority used when claiming work
     * @param correlationId the batch identity, or {@code null} for an independent execution
     */
    record ExecutionHead(ExecutionId executionId, JobKey jobKey, Instant scheduledAt, Instant createdAt, String actor,
            int priority, @Nullable String correlationId) {
        /**
         * Creates a {@code ExecutionHead} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param jobKey the stable identity of the job
         * @param scheduledAt the intended firing instant
         * @param createdAt the enqueue instant
         * @param actor the identity attributed to the operation
         * @param priority the ordering priority used when claiming work
         * @param correlationId the batch identity, or {@code null} for an independent execution
         */
        public ExecutionHead {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(scheduledAt, "scheduledAt");
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(actor, "actor");
        }
    }

    /**
     * The separation: {@code unreadable} means unreadable rows (a PERMANENT failure, with the cause for the terminal attempt), never infrastructure — infrastructure propagates as an exception from the call itself.
     *
     * @param rows the successfully decoded execution payloads by identity
     * @param unreadable the decoding failures indexed by execution identity
     */
    record PayloadBatch(Map<ExecutionId, PayloadRow> rows, Map<ExecutionId, RuntimeException> unreadable) {
        /**
         * Creates a {@code PayloadBatch} with the supplied values.
         *
         * @param rows the successfully decoded execution payloads by identity
         * @param unreadable the decoding failures indexed by execution identity
         */
        public PayloadBatch {
            rows = Map.copyOf(rows);
            unreadable = Map.copyOf(unreadable);
        }
    }

    /**
     * Headers only — the reaper's cold path (rearming, batching and pruning terminal candidates) without paying for payload deserialisation.
     *
     * @param ids the execution identities to look up
     * @return the matching headers without payload decoding
     */
    List<ExecutionHead> findHeads(List<ExecutionId> ids);

    /**
     * One execution's attempts, in number order — the detail view. The primary key {@code (execution_id, number)} serves both the predicate and the ordering, on every database.
     *
     * @param executionId the identity of the execution
     * @return the attempts ordered by attempt number
     */
    List<Attempt> findAttempts(ExecutionId executionId);

    /**
     * The execution as the public API sees it — with attempts, and with a DERIVED state: history's
     * {@code state} column is advisory (PENDING until terminal), while in flight the truth is the
     * queue and the ownership.
     *
     * <p>The derivation: a lease present means {@code RUNNING} (with {@code owner} = the owning node
     * and {@code firedAt} = the claim); a queue entry with {@code attempt > 1} still invisible means
     * {@code RETRY_WAITING}; a queue entry otherwise means {@code ENQUEUED}; and a terminal column
     * means itself. A PENDING row with neither queue nor ownership is the window of a completion
     * flush in progress — it reads {@code ENQUEUED} (the model's bounded, documented staleness).
     *
     * <p>{@code now} decides {@code ENQUEUED} versus {@code RETRY_WAITING} (the visibility rule) — from
     * the caller's injected {@code Clock}, like every "when" in the project.
     *
     * @param id the identity of the execution
     * @param now the current instant from the configured time source
     * @return the execution with its derived state and attempts, or empty when absent
     */
    Optional<Execution> find(ExecutionId id, Instant now);

    /**
     * The dashboard's page — the same derivation as {@link #find}, without attempts (a summary by
     * contract); ordered by descending id (UUIDv7, so most recent first), with a keyset on
     * {@code id < cursor}.
     *
     * <p>The {@code status} filter applies to the DERIVED state: a terminal one filters on the column;
     * {@code RUNNING} filters by ownership; {@code ENQUEUED}/{@code RETRY_WAITING} filter by the queue.
     *
     * @param jobKey the job filter, or {@code null} for all jobs
     * @param status the execution state filter, or {@code null} for all states
     * @param from the inclusive lower bound on scheduled time, or {@code null}
     * @param to the exclusive upper bound on scheduled time, or {@code null}
     * @param cursor the last execution identity of the previous page, or {@code null}
     * @param limit the maximum number of results in one batch
     * @param now the current instant from the configured time source
     * @return the matching execution summaries in descending identity order
     */
    List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit, Instant now);

    /**
     * The live work's count by state for {@code GET /overview} — a cost proportional to the live work
     * by construction on the new layout: {@code RUNNING} is the size of {@code mohs_lease};
     * {@code ENQUEUED}/{@code RETRY_WAITING} are {@code mohs_ready} split by the visibility rule. No
     * read touches history.
     *
     * @param now the current instant from the configured time source
     * @return the counts of live work by derived execution state
     */
    Map<ExecutionState, Long> countActiveByState(Instant now);

    /**
     * The recent window's throughput ({@code GET /overview}): terminal attempts with {@code finished_at >= since}, by outcome — the throughput index serves it by construction.
     *
     * @param since the inclusive lower bound on completion time
     * @return the terminal attempt counts by outcome in the window
     */
    Map<ExecutionState, Long> countTerminalOutcomesSince(Instant since);

    /**
     * Pruning of {@code mohs_idempotency} by the idempotency window — called by housekeeping, never on the hot path.
     *
     * @param cutoff the exclusive retention cutoff
     * @return the number of deleted idempotency keys
     */
    int pruneIdempotencyBefore(Instant cutoff);

    /**
     * One bounded sweep of the history retention window — called by housekeeping, never on the hot
     * path. Deletes, in this order and at most {@code limit} rows per table: TERMINAL executions
     * finished before {@code cutoff}, attempts whose execution no longer exists, and batches with no
     * remaining member. The order is what makes each statement safe on its own: an execution is only
     * deleted in a state a concurrent manual retry can no longer rearm, an attempt only once its
     * execution is gone (so a live execution's attempt count is never corrupted), and a batch only
     * once no member can still count into it. A crash between statements leaves rows the NEXT sweep's
     * own predicates collect — no cross-statement transaction to lose.
     *
     * <p>{@code mohs_idempotency} is deliberately untouched: its window is the deduplication
     * contract, not history — see {@link #pruneIdempotencyBefore}.
     *
     * @param cutoff the exclusive retention cutoff
     * @param limit the maximum number of results in one batch
     * @return the deleted-row count for each pruned table
     */
    PrunedHistory pruneHistoryBefore(Instant cutoff, int limit);

    /**
     * What one sweep removed, per table — {@link #drained(int)} says whether every table came back under the batch, i.e. the window is clean.
     *
     * @param executions the number of execution rows removed
     * @param attempts the number of orphaned attempt rows removed
     * @param batches the number of batch rows removed
     */
    record PrunedHistory(int executions, int attempts, int batches) {

        /**
         * Reports whether every pruned table returned fewer rows than the batch limit.
         *
         * @param limit the maximum number of results in one batch
         * @return whether all three counts are strictly below the batch limit
         */
        public boolean drained(int limit) {
            return executions < limit && attempts < limit && batches < limit;
        }
    }
}
