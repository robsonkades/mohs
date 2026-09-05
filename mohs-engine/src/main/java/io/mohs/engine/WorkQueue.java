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
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;

/**
 * The QUEUE ({@code mohs_ready}) and the queue-to-ownership transition — one of the four ports that
 * replaced the eight of the single-table era.
 *
 * <p>The queue's size is the BACKLOG, never the history: an entry is born at enqueue, retry or
 * requeue ({@link #offer}) and dies at the claim ({@link #claim}) — which, in the same transaction,
 * inserts the ownership into {@code mohs_lease} (there is no instant at which an execution is
 * neither queued nor owned; what guarantees that is the storage, not the application's call order).
 *
 * <p>Retry, delayed, a reaper's requeue and an immediate enqueue are the SAME operation with a
 * different {@code visibleAt} — one visibility rule ({@code visible_at <= now}), zero states: that is
 * what took retry out of the claim predicate ({@code RETRY_WAITING} is not claimable) and killed the
 * 3x regression of the two-value {@code IN} on MySQL.
 */
public interface WorkQueue {

    /**
     * One queue entry. {@code attempt} is the attempt this entry WILL become once claimed (1 on the
     * first enqueue; the previous plus 1 on a retry or requeue) — the claim copies it into
     * {@code mohs_lease.attempt_number} and nothing else counts attempts on the hot path.
     *
     * <p>{@code shard} comes from {@link Shards#of}; the range is validated HERE, where the data
     * enters the type (Effective Java 49): a row written outside [0, 64) would never be claimed — the
     * lap only probes shards the derived partition distributes, and the entry would rot in the queue
     * in silence.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param shard the deterministic queue partition
     * @param priority the ordering priority used when claiming work
     * @param attempt the one-based attempt number
     * @param visibleAt the instant when the queue entry becomes claimable
     */
    record ReadyEntry(ExecutionId executionId, JobKey jobKey, int shard, int priority, int attempt, Instant visibleAt) {
        /**
         * Creates a {@code ReadyEntry} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param jobKey the stable identity of the job
         * @param shard the deterministic queue partition
         * @param priority the ordering priority used when claiming work
         * @param attempt the one-based attempt number
         * @param visibleAt the instant when the queue entry becomes claimable
         */
        public ReadyEntry {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(visibleAt, "visibleAt");
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt must be >= 1 — it is the attempt this entry becomes, not a counter");
            }
            if (shard < 0 || shard >= Shards.SHARD_COUNT) {
                throw new IllegalArgumentException("shard must be in [0, " + Shards.SHARD_COUNT
                        + ") — no node's derived partition would ever probe it and the entry would rot unclaimed");
            }
        }
    }

    /**
     * What the claim returns: identity, never the payload — the dispatcher follows with ONE batched read of history. {@code priority} travels back so an admission-loss requeue can rebuild the entry without an extra read.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param attemptNumber the one-based attempt number
     * @param priority the ordering priority used when claiming work
     */
    record ClaimedWork(ExecutionId executionId, JobKey jobKey, int attemptNumber, int priority) {
        /**
         * Creates a {@code ClaimedWork} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param jobKey the stable identity of the job
         * @param attemptNumber the one-based attempt number
         * @param priority the ordering priority used when claiming work
         */
        public ClaimedWork {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
        }
    }

    /**
     * The claim: in one transaction, it removes up to {@code limit} visible entries from the shard —
     * ordered by {@code (priority, visible_at)}, skipping rows locked by concurrent claims — and
     * inserts the {@code (nodeId, epoch, attempt)} ownership into {@code mohs_lease}.
     *
     * <p>ONE shard per call, never a list (a measured lesson: a multi-shard predicate kills the index's
     * ordering — a measured 25.5 ms/round against 0.43 ms); the caller round-robins the shards it owns.
     *
     * <p>{@code inadmissible} is the node-local list of job keys excluded IN THIS ROUND (a closed
     * window, a cap with no headroom, an empty rate-limit semaphore, a missing handler) — computed in
     * memory BEFORE the round, per job rather than per candidate. A deliberate snapshot: a guard that
     * flips mid-round is resolved at dispatch (admission lost, so a requeue), not here.
     *
     * <p>The returned list comes IN {@code (priority, visible_at)} ORDER in all four delegates
     * (contract, not accident). A sizing invariant: {@code limit} is at most the dispatch headroom
     * (~1k), which is below 2000 — SQL Server's parameter ceiling; the portable form deliberately does
     * not chunk the DELETE's {@code IN}.
     *
     * @param shard the deterministic queue partition
     * @param nodeId the identity of the engine node
     * @param epoch the ownership generation used by the completion fence
     * @param limit the maximum number of results in one batch
     * @param inadmissible the claimed entries that cannot currently be dispatched
     * @param now the current instant from the configured time source
     * @return the work entries whose ownership was acquired
     */
    List<ClaimedWork> claim(int shard, String nodeId, long epoch, int limit, Collection<JobKey> inadmissible, Instant now);

    /**
     * Is there any visible entry in any of these shards? A read that claims nothing and opens no
     * transaction — which is why it costs ONE round trip while the claim costs three: the
     * transaction's {@code BEGIN} and the {@code SHOW TRANSACTION ISOLATION LEVEL} that explicit
     * isolation drags along.
     *
     * <p>It is the idle gate: while the previous round came back empty, the tick pays this instead of
     * a lap of {@link Shards#SHARD_COUNT} statements, which measured 96% of an idle node's query cost
     * (measured). That lesson applies to the CLAIM, not to this question: a multi-shard predicate
     * kills the index's ordering, and nothing is ordered here — the answer is "there is" or "there is
     * not".
     *
     * <p>It shares the loop thread with {@link #countVisible} and carries no time bound, deliberately:
     * the shard predicate rides the claim's own index and the existence check short-circuits, so this
     * is bounded by one node's shards where the count is bounded by the whole queue.
     *
     * <p>Best-effort in one direction only: {@code true} is always safe (it costs a lap), while
     * {@code false} is an ASSERTION — it may only come from a fresh read of the real state, never from
     * a cache or an in-memory flag. When in doubt, {@code true}: a persistently wrong {@code false}
     * does not cost a poll, it stops this node's queue.
     *
     * @param shards the queue partitions eligible for this operation
     * @param now the current instant from the configured time source
     * @return whether any eligible queue entry is visible
     */
    boolean hasVisibleWork(Collection<Integer> shards, Instant now);

    /**
     * The backlog: entries visible to a claim right now, across EVERY shard — not only this node's.
     *
     * <p>A cluster-wide number answered by a node-local caller, deliberately: the backlog is a
     * property of the queue, not of whoever asks, and an operator alerting on "work is piling up"
     * wants the queue's size rather than one node's slice of it. Every node therefore reports the
     * same value, and the aggregation across the fleet is {@code max}, never {@code sum}.
     *
     * <p>The cost is proportional to the BACKLOG, never to history: {@code mohs_ready} holds only
     * work that has not finished. It is still a count rather than an existence check, so the caller
     * samples it on a cadence of its own — never once per tick.
     *
     * @param now the current instant from the configured time source
     * @return the number of visible queue entries
     */
    long countVisible(Instant now);

    /**
     * Inserts entries into the queue. It does NOT open a transaction of its own: the caller MUST
     * compose {@code HistoryStore.record} plus {@code offer} into a single transaction (joining the
     * host's when there is one, and the facade opening its own, {@code REQUIRED}, when there is not).
     *
     * <p>Calling it outside a transaction breaks the enqueue's unit: a partial failure leaves either an
     * orphan idempotency key (deduplicating against nothing for the whole window) or an unreachable
     * {@code PENDING} execution — not a supported mode.
     *
     * @param entries the queue entries to persist
     */
    void offer(List<ReadyEntry> entries);

    /**
     * The recovery and admission-loss path: in one transaction, it deletes the given lease — fenced by
     * the OBSERVED incarnation's {@code (node_id, epoch, attempt_number)}, the fencing token — and reinserts the entry
     * into the queue.
     *
     * <p>Losing the fence (the lease no longer exists, or changed owner) skips that entry's
     * reinsertion: the new incarnation is the authority. It returns how many entries actually made it
     * back into the queue.
     *
     * @param orders the observed lease fences and replacement ready entries
     * @return the number of entries successfully fenced and requeued
     */
    int requeue(List<Requeue> orders);

    /**
     * A requeue order: the lease to drop, with the whole observed fencing token
     * ({@code nodeId}, {@code epoch}, {@code attemptNumber}), and the entry that is reborn in the queue.
     *
     * @param executionId the identity of the execution
     * @param nodeId the identity of the engine node
     * @param epoch the ownership generation used by the completion fence
     * @param attemptNumber the one-based attempt number
     * @param entry the replacement queue entry
     */
    record Requeue(ExecutionId executionId, String nodeId, long epoch, int attemptNumber, ReadyEntry entry) {
        /**
         * Creates a {@code Requeue} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param nodeId the identity of the engine node
         * @param epoch the ownership generation used by the completion fence
         * @param attemptNumber the one-based attempt number
         * @param entry the replacement queue entry
         */
        public Requeue {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(entry, "entry");
            // The same guard as Dispatcher.Grant's: a token with an impossible attempt would lose every
            // fence, silently — a requeue that never requeues, with nothing but a DEBUG line to show
            if (attemptNumber < 1) {
                throw new IllegalArgumentException("attemptNumber must be >= 1, got " + attemptNumber);
            }
        }
    }

    /**
     * Cancels an execution still in the QUEUE (the "pending" half of cancellation): in one
     * transaction, it removes the entry and writes the advisory terminal {@code CANCELLED} to history.
     *
     * <p>It loses to any concurrent claim (the entry has already left the queue) — the caller then
     * falls back to ownership's cooperative flag ({@code LeaseStore#requestCancellation}).
     *
     * @param id the identity of the execution
     * @param now the current instant from the configured time source
     * @return {@code true} if THIS call removed the entry
     */
    boolean cancelQueued(ExecutionId id, Instant now);

    /**
     * The operator's manual retry: in one transaction, a CAS of the advisory {@code FAILED} to
     * {@code PENDING} — guarded by the job not being retired, the same guard as the old CAS — and a
     * rebirth in the queue with {@code attempt = recorded attempts + 1} and the original priority. It
     * bypasses the budget on purpose: the operator decides.
     *
     * @param id the identity of the execution
     * @param now the current instant from the configured time source
     * @return {@code true} if THIS call armed it; {@code false} means a nonexistent id, a state other
     *         than {@code FAILED}, or a retired job — the caller tells them apart with a read
     */
    boolean rearmForManualRetry(ExecutionId id, Instant now);
}
