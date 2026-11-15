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
 * Complete active-work operations: acquisition, fenced admission deferral, cancellation and manual retry.
 * The JDBC representation is one READY/LEASED row, with retained history and receipts separate.
 * Claim assigns ownership atomically; requeue preserves the attempt, while retry advances it.
 */
public interface WorkQueue {

    /**
     * One ready entry. {@code attempt} is the assigned attempt number. Claim preserves it;
     * retry increments it and admission deferral does not.
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
            visibleAt = io.mohs.core.TimePrecision.eligible(visibleAt);
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
    record ClaimedWork(ExecutionId executionId, JobKey jobKey, int attemptNumber, int priority, String claimToken) {
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
            io.mohs.core.GeneratedId.require(claimToken, "claimToken");
        }
    }

    /**
     * A bounded keyset page of job identities with visible queued work in one shard. A null cursor
     * starts at the first key; callers advance even when policy rejects an entire page. No payloads.
     * The page is advisory; the claim transaction rechecks eligibility, session and global capacity.
     */
    List<JobKey> findCandidateJobs(int shard, @org.jspecify.annotations.Nullable JobKey after, int limit, Instant horizon);

    /**
     * Atomically acquires visible work under a live session and global job capacity. A nonempty
     * candidateJobs restricts this claim to those identities; empty requests an unrestricted store-level
     * claim. The engine always supplies a nonempty bounded page after local admission.
     * Priority orders work inside the supplied page and shard; it does not promise global FIFO.
     * A caller horizon can narrow eligibility but cannot advance the database's clock.
     */
    List<ClaimedWork> claim(int shard, String nodeId, int limit, Collection<JobKey> candidateJobs, Instant now);

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
     * <p>The cost is proportional to the BACKLOG, never to history: {@code mohs_work} in READY state holds only
     * work that has not finished. It is still a count rather than an existence check, so the caller
     * samples it on a cadence of its own — never once per tick.
     *
     * @param now the current instant from the configured time source
     * @return the number of visible queue entries
     */
    long countVisible(Instant now);



    /**
     * The recovery and admission-loss path: in one transaction, it deletes the given lease — fenced by
     * the OBSERVED incarnation's {@code (node_id, claim_token, attempt_number)}, the fencing token — and reinserts the entry
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
     * ({@code nodeId}, {@code claimToken}, {@code attemptNumber}), and the entry that is reborn in the queue.
     *
     * @param executionId the identity of the execution
     * @param nodeId the identity of the engine node
     * @param claimToken the ownership generation used by the completion fence
     * @param attemptNumber the one-based attempt number
     * @param visibleAt when the same attempt becomes eligible again
     */
    record Requeue(ExecutionId executionId, String nodeId, String claimToken, int attemptNumber, Instant visibleAt) {
        /**
         * Creates a {@code Requeue} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param nodeId the identity of the engine node
         * @param claimToken the ownership generation used by the completion fence
         * @param attemptNumber the one-based attempt number
         * @param visibleAt when the same attempt becomes eligible again
         */
        public Requeue {
            Objects.requireNonNull(executionId, "executionId");
            io.mohs.core.GeneratedId.require(nodeId, "nodeId");
            io.mohs.core.GeneratedId.require(claimToken, "claimToken");
            visibleAt = io.mohs.core.TimePrecision.eligible(visibleAt);
            // The same guard as Dispatcher.Grant's: a token with an impossible attempt would lose every
            // fence, silently — a requeue that never requeues, with nothing but a DEBUG line to show
            if (attemptNumber < 1) {
                throw new IllegalArgumentException("attemptNumber must be >= 1, got " + attemptNumber);
            }
        }
    }

    /**
     * Atomically cancels queued work or durably requests cancellation of its current ownership.
     * An accepted request prevents a later retry or requeue; successful external work can still
     * complete as SUCCEEDED. Terminal executions are unchanged.
     * @param id the execution identity
     * @param now the cancellation instant
     * @return whether an active execution accepted the request
     */
    boolean cancel(ExecutionId id, Instant now);

    /**
     * The operator's manual retry: in one transaction, a CAS of the advisory {@code FAILED} to
     * {@code PENDING} — guarded by the job not being retired, the same guard as the old CAS — and a
     * rebirth in the queue with {@code attempt = last_attempt_number + 1} and the original priority. It
     * bypasses the budget on purpose: the operator decides.
     *
     * @param id the identity of the execution
     * @param now the current instant from the configured time source
     * @return {@code true} if THIS call armed it; {@code false} means a nonexistent id, a state other
     *         than {@code FAILED}, a scheduled occurrence, batch member, or a retired job — the caller tells them apart with a read
     */
    boolean rearmForManualRetry(ExecutionId id, Instant now);
}
