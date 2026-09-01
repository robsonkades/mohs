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
     */
    record NewExecution(ExecutionId executionId, JobKey jobKey, int shard, int priority, Instant scheduledAt,
            Instant createdAt, String actor, @Nullable String correlationId, @Nullable String idempotencyKey,
            Object payload) {
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
     */
    void record(List<NewExecution> executions);

    /** Who won the idempotency race — the id recorded in {@code mohs_idempotency} for (job, key); empty when the key was never used (or has been pruned). */
    Optional<ExecutionId> findByIdempotencyKey(JobKey jobKey, String idempotencyKey);

    /**
     * The batched read that follows each claim round: the payload plus the header of the claimed ids
     * (dispatch needs {@code scheduledAt}/{@code actor}/{@code priority}/{@code batchId} for events,
     * rearming and completion — one read, never N).
     *
     * <p>Infrastructure and row failures are separated: a deserialisation failure of ONE row does not
     * take down the batch — that row enters {@code unreadable} and the rest dispatches.
     */
    PayloadBatch findPayloads(List<ExecutionId> ids);

    /** The hydrated payload plus the execution's header — what dispatch, the events and the completion need about the row. */
    record PayloadRow(ExecutionHead head, Object payload) {
        public PayloadRow {
            Objects.requireNonNull(head, "head");
            Objects.requireNonNull(payload, "payload");
        }
    }

    /** One history row's header — everything but the payload and the attempts; {@code correlationId} carries the batch. */
    record ExecutionHead(ExecutionId executionId, JobKey jobKey, Instant scheduledAt, Instant createdAt, String actor,
            int priority, @Nullable String correlationId) {
        public ExecutionHead {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(scheduledAt, "scheduledAt");
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(actor, "actor");
        }
    }

    /** The separation: {@code unreadable} means unreadable rows (a PERMANENT failure, with the cause for the terminal attempt), never infrastructure — infrastructure propagates as an exception from the call itself. */
    record PayloadBatch(Map<ExecutionId, PayloadRow> rows, Map<ExecutionId, RuntimeException> unreadable) {
        public PayloadBatch {
            rows = Map.copyOf(rows);
            unreadable = Map.copyOf(unreadable);
        }
    }

    /** Headers only — the reaper's cold path (rearming, batching and pruning terminal candidates) without paying for payload deserialisation. */
    List<ExecutionHead> findHeads(List<ExecutionId> ids);

    /** One execution's attempts, in number order — the detail view. The primary key {@code (execution_id, number)} serves both the predicate and the ordering, on every database. */
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
     */
    Optional<Execution> find(ExecutionId id, Instant now);

    /**
     * The dashboard's page — the same derivation as {@link #find}, without attempts (a summary by
     * contract); ordered by descending id (UUIDv7, so most recent first), with a keyset on
     * {@code id < cursor}.
     *
     * <p>The {@code status} filter applies to the DERIVED state: a terminal one filters on the column;
     * {@code RUNNING} filters by ownership; {@code ENQUEUED}/{@code RETRY_WAITING} filter by the queue.
     */
    List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit, Instant now);

    /**
     * The live work's count by state for {@code GET /overview} — a cost proportional to the live work
     * by construction on the new layout: {@code RUNNING} is the size of {@code mohs_lease};
     * {@code ENQUEUED}/{@code RETRY_WAITING} are {@code mohs_ready} split by the visibility rule. No
     * read touches history.
     */
    Map<ExecutionState, Long> countActiveByState(Instant now);

    /** The recent window's throughput ({@code GET /overview}): terminal attempts with {@code finished_at >= since}, by outcome — the throughput index serves it by construction. */
    Map<ExecutionState, Long> countTerminalOutcomesSince(Instant since);

    /** Pruning of {@code mohs_idempotency} by the idempotency window — called by housekeeping, never on the hot path. */
    int pruneIdempotencyBefore(Instant cutoff);
}
