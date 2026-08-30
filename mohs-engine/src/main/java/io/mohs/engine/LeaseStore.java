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
 * OWNERSHIP ({@code mohs_lease}).
 *
 * <p>The table contains exactly the work executing across the cluster, bounded by
 * {@code nodes x dispatch-concurrency} — thousands of rows, never millions. That is what makes the
 * DERIVED concurrency cap ({@link #countByJob}) an index-only scan that is always cached, and what
 * killed the hot {@code running_execution_count} counter.
 *
 * <p>The fence is {@code (node_id, epoch)} — DDIA ch. 8's fencing token: every write over owned work
 * carries the pair, and a zombie (a reaped node that came back) loses ALL of them because it carries
 * an old epoch. Deleting the lease IS releasing the slot — there is no longer a slot to give back.
 */
public interface LeaseStore {

    /** A live lease. {@code attemptNumber} and {@code priority} came from the queue entry the claim consumed — which is what lets the reaper rebuild the requeue entry without reading history. */
    record Lease(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber, int priority,
            Instant claimedAt, boolean cancelRequested) {
        public Lease {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(claimedAt, "claimedAt");
        }
    }

    /**
     * An attempt's result, ready for durability.
     *
     * <p>{@code outcome} is THE ATTEMPT's outcome ({@code SUCCEEDED}/{@code FAILED}/{@code CANCELLED} —
     * the vocabulary {@code Attempt} accepts), typed on purpose: a typo in a string would write
     * cleanly and blow up months later in the detail view's {@code valueOf}.
     *
     * <p>{@code terminalState} is the advisory state written to history, and {@code null} when the
     * outcome is NOT terminal — in that case {@code retry} is mandatory and {@link #complete} ITSELF
     * reinserts the queue entry, IN THE SAME transaction (a retry is an insert into the queue; outside
     * the transaction, a crash between the completion's commit and the insert would leave the
     * execution with no lease, no queue entry and non-terminal — an orphan invisible forever). The
     * caller NEVER calls {@link WorkQueue#offer} for a retry: it would duplicate {@code mohs_ready}'s
     * primary key.
     *
     * <p>{@code batchId} makes a terminal completion count towards the batch in the SAME transaction,
     * and {@code rearmNextFireAt} rearms the fixed-delay chain likewise — a separate write between the
     * commit and a crash would be lost.
     */
    record CompletionResult(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber,
            Instant startedAt, Instant finishedAt, ExecutionState outcome, @Nullable String errorType, @Nullable String error,
            @Nullable ExecutionState terminalState,
            WorkQueue.@Nullable ReadyEntry retry, @Nullable String batchId, @Nullable Instant rearmNextFireAt) {
        public CompletionResult {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(startedAt, "startedAt");
            Objects.requireNonNull(finishedAt, "finishedAt");
            if (terminalState != null && retry != null) {
                throw new IllegalArgumentException("a result is terminal OR schedules a retry — never both");
            }
            if (terminalState == null && retry == null) {
                throw new IllegalArgumentException("a non-terminal result must carry the retry entry — "
                        + "without it the execution would end up owned by nobody and queued nowhere");
            }
            if (terminalState == null && rearmNextFireAt != null) {
                throw new IllegalArgumentException("rearmNextFireAt only applies to terminal results — the retry chain is still alive");
            }
        }

        /** The form with neither batch nor rearm — the majority of completions. */
        public CompletionResult(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber,
                Instant startedAt, Instant finishedAt, ExecutionState outcome, @Nullable String errorType,
                @Nullable String error, @Nullable ExecutionState terminalState,
                WorkQueue.@Nullable ReadyEntry retry) {
            this(executionId, jobKey, nodeId, epoch, attemptNumber, startedAt, finishedAt, outcome, errorType, error,
                    terminalState, retry, null, null);
        }
    }

    /**
     * What the completion produced: {@code owned} is the fence's verdict — false when the incarnation
     * already belonged to somebody else (the reaper or a requeue got there first) and NOTHING was
     * written; {@code closedBatch} is filled only for the SINGLE completion that zeroed the batch's
     * pending count — the balance comes up from the transaction itself, never from a re-read (two
     * concurrent re-reads would both believe they closed it).
     */
    record Completion(boolean owned, @Nullable BatchCounters closedBatch) {

        public static final Completion FENCED_OUT = new Completion(false, null);

        public static Completion owned(@Nullable BatchCounters closedBatch) {
            return new Completion(true, closedBatch);
        }
    }

    /**
     * The completion transaction: a {@code DELETE} of the leases fenced by {@code (node_id, epoch)} —
     * the count says exactly which ones this caller still owned — an {@code INSERT} of the confirmed
     * attempts, the advisory terminal {@code UPDATE} of history — matched by {@code execution_id},
     * which is the primary key on every dialect, so it touches at most one row — the batch count and
     * the fixed-delay rearm: all or nothing.
     *
     * <p>A result with {@code owned = false} belonged to a lost incarnation and is discarded —
     * detected, never silently lost. {@code jobStore} comes in for the same reason as before: the
     * rearm takes part in the transaction by sharing the {@code DataSource}.
     */
    Map<ExecutionId, Completion> complete(List<CompletionResult> results, JobStore jobStore);

    /** These nodes' leases — the raw material of the drain visible in {@code GET /nodes}. */
    List<Lease> findByNodes(Collection<String> nodeIds);

    /**
     * The leases whose owner is NOT in {@code aliveNodeIds} — the reaper's candidate selection: a node
     * absent from the list is dead by definition (its row purged, its promise expired — the caller
     * decides alive versus dead by reading {@code mohs_nodes}).
     *
     * <p>Bounded to {@code limit} per call, oldest first ({@code claimed_at}) — a mass death drains
     * over several passes, never in one unbounded transaction.
     */
    List<Lease> findOrphaned(Collection<String> aliveNodeIds, int limit);

    /** The derived cap: a per-job count of the live leases, read ONCE per round — never per candidate. */
    Map<JobKey, Integer> countByJob(Collection<JobKey> jobKeys);

    /** Cooperative cancellation over the ownership: it raises the flag if the lease exists; {@code false} means it is not executing. */
    boolean requestCancellation(ExecutionId id);

    /** The tick's poll: of these in-flight ids, which have a pending cancel order. */
    Set<ExecutionId> findCancelRequested(Collection<ExecutionId> ids);
}
