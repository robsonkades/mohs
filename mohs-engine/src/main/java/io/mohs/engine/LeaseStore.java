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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * Persisted execution ownership and atomic completion.
 *
 * <p>A normal completion requires an active database session and the exact acquisition token.
 * Recovery requires a revoked session. Tokens change on every acquisition, including requeue of
 * the same attempt. They reject stale scheduler writes; they are not monotonic fences for external
 * systems and do not make a job's external effects exactly once.
 */
public interface LeaseStore {

    /**
     * A live lease. {@code attemptNumber} and {@code priority} came from the queue entry the claim consumed — which is what lets the reaper rebuild the requeue entry without reading history.
     *
     * @param executionId the identity of the execution
     * @param jobKey the stable identity of the job
     * @param nodeId the identity of the engine node
     * @param claimToken the ownership generation used by the completion fence
     * @param attemptNumber the one-based attempt number
     * @param priority the ordering priority used when claiming work
     * @param claimedAt the instant ownership was granted
     * @param cancelRequested whether cooperative cancellation has been requested
     */
    record Lease(ExecutionId executionId, JobKey jobKey, String nodeId, String claimToken, int attemptNumber, int priority,
            Instant claimedAt, boolean cancelRequested) {
        /**
         * Creates a {@code Lease} with the supplied values.
         *
         * @param executionId the identity of the execution
         * @param jobKey the stable identity of the job
         * @param nodeId the identity of the engine node
         * @param claimToken the ownership generation used by the completion fence
         * @param attemptNumber the one-based attempt number
         * @param priority the ordering priority used when claiming work
         * @param claimedAt the instant ownership was granted
         * @param cancelRequested whether cooperative cancellation has been requested
         */
        public Lease {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            io.mohs.core.GeneratedId.require(nodeId, "nodeId");
            io.mohs.core.GeneratedId.require(claimToken, "claimToken");
            Objects.requireNonNull(claimedAt, "claimedAt");
        }
    }

    /**
     * A fenced attempt outcome and optional retry time. Routing, batch membership and schedule
     * rearm are derived from accepted metadata inside the completion transaction.
     */
    record CompletionResult(ExecutionId executionId, JobKey jobKey, String nodeId, String claimToken, int attemptNumber,
            Instant startedAt, Instant finishedAt, ExecutionState outcome, @Nullable String errorType, @Nullable String error,
            @Nullable Instant retryAt) {
        public CompletionResult {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            io.mohs.core.GeneratedId.require(nodeId, "nodeId");
            io.mohs.core.GeneratedId.require(claimToken, "claimToken");
            Objects.requireNonNull(outcome, "outcome");
            startedAt = io.mohs.core.TimePrecision.observed(startedAt);
            finishedAt = io.mohs.core.TimePrecision.observed(finishedAt);
            if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
            if (outcome != ExecutionState.SUCCEEDED && outcome != ExecutionState.FAILED && outcome != ExecutionState.CANCELLED) {
                throw new IllegalArgumentException("an attempt outcome must be SUCCEEDED, FAILED or CANCELLED");
            }
            if (retryAt != null) {
                if (outcome != ExecutionState.FAILED) throw new IllegalArgumentException("only a failed attempt can retry");
                if (attemptNumber == Integer.MAX_VALUE) throw new IllegalArgumentException("attempt number exhausted");
                retryAt = io.mohs.core.TimePrecision.eligible(retryAt);
            }
        }

        /** Terminal state is derived from the attempt outcome and the absence of a retry. */
        public @Nullable ExecutionState terminalState() {
            return retryAt == null ? outcome : null;
        }

        /** Successful work remains successful; accepted cancellation prevents another attempt. */
        public CompletionResult honourCancellation() {
            if (outcome == ExecutionState.SUCCEEDED || outcome == ExecutionState.CANCELLED) return this;
            return new CompletionResult(executionId, jobKey, nodeId, claimToken, attemptNumber, startedAt, finishedAt,
                    ExecutionState.CANCELLED, null, null, null);
        }

        /** A retired job records the current outcome without creating another attempt. */
        public CompletionResult honourRetirement() {
            if (retryAt == null) return this;
            return new CompletionResult(executionId, jobKey, nodeId, claimToken, attemptNumber, startedAt, finishedAt,
                    outcome, errorType, error, null);
        }
    }

    /**
     * What the completion produced: {@code owned} is the fence's verdict — false when the incarnation
     * already belonged to somebody else (the reaper or a requeue got there first) and NOTHING was
     * written; {@code closedBatch} is filled only for the SINGLE completion that zeroed the batch's
     * pending count — the balance comes up from the transaction itself, never from a re-read (two
     * concurrent re-reads would both believe they closed it).
     *
     * @param result the result actually persisted, or null when no write was accepted
     * @param alreadyApplied whether this exact acquisition already completed
     * @param closedBatch the counters when this completion closes the batch, or {@code null}
     */
    record Completion(@Nullable CompletionResult result, boolean alreadyApplied, @Nullable BatchCounters closedBatch) {

        public static final Completion FENCED_OUT = new Completion(null, false, null);
        public static final Completion ALREADY_APPLIED = new Completion(null, true, null);

        public boolean owned() {
            return result != null;
        }

        /**
         * Creates an accepted completion, optionally carrying the batch it closed.
         *
         * @param closedBatch the counters when this completion closes the batch, or {@code null}
         * @return an accepted fence outcome with the supplied batch counters
         */
        public static Completion owned(CompletionResult result, @Nullable BatchCounters closedBatch) {
            return new Completion(Objects.requireNonNull(result, "result"), false, closedBatch);
        }
    }

    /**
     * The completion transaction: a {@code DELETE} of the leases fenced by {@code (node_id, claim_token, attempt_number)} —
     * the count says exactly which ones this caller still owned — an {@code UPDATE} of the latest
     * completed attempt and optional terminal state — matched by {@code execution_id},
     * which is the primary key on every database, so it touches at most one row — the batch count and
     * the retry queue entry and fixed-delay rearm: all or nothing.
     *
     * <p>A result with {@code owned = false} belonged to a lost incarnation and is discarded —
     * detected, never silently lost. Rearm and batch counting belong to the store transaction.
     *
     * @param results at most one completion result per execution
     * @return the fence outcome for each submitted execution
     */
    Map<ExecutionId, Completion> complete(List<CompletionResult> results);

    /**
     * Completes grants of revoked sessions under the same transaction deadline as normal completion.
     * @param results at most one result per execution
     * @return the verdict for each execution
     */
    Map<ExecutionId, Completion> reclaim(List<CompletionResult> results);

    /**
     * These nodes' leases — the raw material of the drain visible in {@code GET /nodes}.
     *
     * @param nodeIds the node identities to query
     * @return the leases owned by the requested nodes
     */
    List<Lease> findByNodes(Collection<String> nodeIds);

    /** Stable continuation through a session's grants in claimed-time/identity order. */
    record LeaseCursor(Instant claimedAt, ExecutionId executionId) {
        public LeaseCursor {
            Objects.requireNonNull(claimedAt, "claimedAt");
            Objects.requireNonNull(executionId, "executionId");
        }
    }

    /** Bounded maintenance read; a null cursor starts a new pass. */
    List<Lease> findByNodePage(String nodeId, @Nullable LeaseCursor after, int limit);

    /**
     * Reads grants whose session has been durably revoked, oldest first. A stale list of live nodes
     * is never recovery authority; revokeExpired arbitrates revocation against heartbeat first.
     * @param limit the maximum number of candidates
     * @return grants of revoked sessions
     */
    List<Lease> findOrphaned(int limit);


    /**
     * The tick's poll: of these in-flight ids, which have a pending cancel order.
     *
     * @param ids the execution identities to look up
     * @return the leased execution identities with cancellation requested
     */
    Set<ExecutionId> findCancelRequested(Collection<ExecutionId> ids);
}
