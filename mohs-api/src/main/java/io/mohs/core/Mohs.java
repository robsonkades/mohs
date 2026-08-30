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
package io.mohs.core;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import org.springframework.lang.CheckReturnValue;

import io.mohs.core.definition.JobDefinition;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.schedule.Schedule;

/**
 * Mohs's public facade — one verb per operation, always over an existing definition.
 *
 * <p>The read methods ({@link #findJob}/{@link #jobs}/{@link #findExecution}/{@link #executions}/
 * {@link #payloadType}) exist for {@code io.mohs.rest}: the architectural boundary
 * ({@code ArchitectureTest#rest_only_sees_public_api}) forbids the REST layer from seeing
 * {@code io.mohs.engine} directly, so this facade is the only read path available to it.
 */
public interface Mohs {

    @CheckReturnValue
    <T> ScheduleCommand schedule(JobRef<T> ref, T payload);

    /** The string overload; the payload type is checked at runtime against the definition (a clear error, not a ClassCastException). */
    @CheckReturnValue
    ScheduleCommand schedule(String jobId, Object payload);

    /**
     * Schedules several jobs as one batch: the {@code configurer} collects the members, the total is
     * fixed at creation, and each execution is born already carrying the {@code batchId} — which is
     * what makes each member's completion count towards the batch. An empty batch is refused: with
     * no members it would never complete, and a forever-open batch is worse than an error.
     *
     * <p>{@code name} is the batch's label and is PERSISTED: it appears in
     * {@link BatchSnapshot#name()}, in the {@link io.mohs.core.event.BatchCompleted} handed to
     * {@link Batch#onCompletion}, and in {@code GET /batches/{id}}. It is what ties the batch back
     * to the intent of whoever created it, so the operator is not left with only a UUID at 3 a.m.
     *
     * <p><b>All-or-nothing:</b> ALL members are validated before any write, and the batch row plus
     * its members enter in a single transaction. An exception from here guarantees nothing was
     * persisted — the call can be repeated with no risk of a partial batch.
     *
     * @return the batch's receipt, with {@code batchId} already durable
     * @throws IllegalArgumentException if the batch is empty (it would never complete) or if any
     *         member references an undefined job — no write occurred in either case
     */
    @CheckReturnValue
    Batch batch(String name, Consumer<BatchBuilder> configurer);

    /**
     * Registers (or updates) a job definition. An upsert by {@link JobDefinition#key()}: redefining
     * the same job replaces the definitional part and NEVER touches the operational one
     * ({@code paused}/{@code orphaned}/{@code next_fire_at}).
     */
    void define(JobDefinition definition);

    /**
     * Retirement: cancels future firings ({@code ENQUEUED} executions become {@code CANCELLED}) and
     * preserves history.
     *
     * <p>Only for programmatic definitions — for a {@code @MohsJob} job, remove the annotation and
     * the scanner marks it {@code ORPHANED} on the next boot; calling this on one throws
     * {@link IllegalArgumentException}. An unknown job is a no-op.
     */
    void remove(JobKey jobKey);

    /**
     * Cancels an execution. A pending one ({@code ENQUEUED}/{@code RETRY_WAITING}) becomes
     * {@code CANCELLED} immediately; a {@code RUNNING} one receives the cooperative request — the
     * owning node observes it within at most one interval of its loop (between
     * {@code mohs.engine.poll-interval} and {@code mohs.engine.max-poll-interval}, depending on the
     * backoff) and the handler decides when to stop (through
     * {@link io.mohs.core.execution.JobContext#cancellationRequested()}); a terminal state does not
     * change.
     *
     * <p>It is never immediate nor guaranteed: a completion may win the race, and in that case it
     * stands.
     *
     * @return the execution in its current state right after the request — not necessarily
     *         terminal; empty if the id does not exist
     */
    Optional<Execution> cancel(ExecutionId executionId);

    /**
     * A manual retry of a {@code FAILED} execution: it rearms the SAME row as {@code RETRY_WAITING}
     * due now, and the new attempt travels the normal claim path, competing like any other
     * candidate.
     *
     * <p>It bypasses the {@code retries} budget on purpose: the policy protects the system from
     * automatic loops, whereas here the decision is the operator's. It does not go through
     * Idempotency-Key deduplication (nothing new is inserted); the natural idempotence is the CAS
     * itself — repeating the call finds the execution already rearmed and fails with the state
     * exception.
     *
     * @return the execution already rearmed ({@code RETRY_WAITING}); empty if the id does not exist
     * @throws IllegalStateException if the execution exists but is not {@code FAILED} (a cancelled
     *         one was an explicit decision; the other states have an owner — the engine), belongs to
     *         a retired job (the rearmed row would never be claimed), or is a BATCH MEMBER — the
     *         batch already counted this failure, and counting it again would close the batch early;
     *         schedule the job standalone to redo the work
     */
    Optional<Execution> retry(ExecutionId executionId);

    Optional<JobSnapshot> findJob(JobKey jobKey);

    /** Every registered job — bounded cardinality (a definition, not an execution), so no pagination. */
    List<JobSnapshot> jobs();

    /**
     * The cluster's nodes with a registered heartbeat, most recent first — bounded cardinality (the
     * cluster's size plus whatever residue the purge has not collected yet), with no pagination,
     * like {@link #jobs()}.
     *
     * <p>Death is not a field: it is derived from the age of {@link NodeSnapshot#lastHeartbeatAt()}
     * at read time. {@code STOPPED} is the only self-reported outcome (a clean shutdown).
     */
    List<NodeSnapshot> nodes();

    /**
     * THIS node's runners — their declared configuration and current occupancy. Bounded cardinality
     * (whatever the application declared at boot), with no pagination, like {@link #jobs()}.
     *
     * <p>Unlike {@link #nodes()} and {@link #jobs()}, this read touches no database and does not see
     * the cluster: a thread pool belongs to the process. Consumers must say which node they are
     * talking about — see {@link RunnerSnapshot}.
     */
    List<RunnerSnapshot> runners();

    /**
     * The declared rate limits and each one's current bucket balance, by name — bounded
     * cardinality, no pagination, like {@link #jobs()}. A pure read: checking the balance consumes
     * no token.
     */
    List<RateLimitSnapshot> rateLimits();

    /**
     * Adjusts {@code max}/{@code window} of an ALREADY declared limit, at runtime and cluster-wide —
     * an emergency change under the same PATCH contract as {@link #reschedule}: the boot reapplies
     * the code's value on the next start under the default {@code on-conflict: override}.
     *
     * <p>The bucket survives the adjustment (its balance clamped to the new ceiling): lowering the
     * limit cuts future throughput, it does not give back what was already consumed.
     *
     * @return the adjusted limit, or empty if {@code name} does not exist — declaring a new limit is
     *         an act of boot, not of emergency
     */
    Optional<RateLimitSnapshot> adjustRateLimit(String name, int max, Duration window);

    /** Suspends automatic firings; manual scheduling is still allowed (mirroring the engine). No effect if {@code jobKey} does not exist. */
    void pause(JobKey jobKey);

    void resume(JobKey jobKey);

    /**
     * Changes the job's stored schedule at runtime — an emergency change under the runtime PATCH
     * contract: on an {@code ANNOTATION} job it holds until the next boot under
     * {@code on-conflict=override} (the scanner restores the code's version with a logged diff),
     * while {@code preserve} keeps it; on a {@code PROGRAMMATIC} job it lasts until the application
     * redefines it.
     *
     * <p>The trigger is recomputed from the clock in the same write — {@code ON_DEMAND} disarms the
     * recurrence.
     *
     * @return the snapshot already carrying the new schedule; empty if the job does not exist (or is
     *         retired)
     * @throws IllegalArgumentException if the schedule is unrealisable (a syntactically valid cron
     *         that never fires, for instance)
     */
    Optional<JobSnapshot> reschedule(JobKey jobKey, Schedule schedule);

    /**
     * The real type of the payload parameter of {@code jobKey}'s handler, or empty if the job does
     * not exist or the annotated method declares no payload (only a {@code JobContext}, or no
     * parameters).
     *
     * <p>Used by the REST layer to convert the JSON body of {@code POST .../schedule} before
     * scheduling, rather than persisting a raw {@code Map} the handler cannot consume. Only the
     * {@code @MohsJob} scanner knows the type: a handler registered manually (the
     * {@code HandlerRegistry}'s internal or test path) without declaring a {@code payloadType} is
     * treated by REST as a job that accepts no payload.
     */
    Optional<Class<?>> payloadType(JobKey jobKey);

    Optional<Execution> findExecution(ExecutionId executionId);

    /**
     * The batch by the id returned from {@link #batch}. A cheap read, flat in the batch's size: the
     * counter is maintained rather than aggregated from the members.
     */
    Optional<BatchSnapshot> findBatch(String batchId);

    /**
     * The dashboard's aggregate view ({@code GET /overview}): live-work counts and the terminal
     * throughput over the last {@code throughputWindow} — see {@link OverviewSnapshot} for the
     * contract, and for why there is no all-time count of terminal states. The caller chooses the
     * window: it is part of the response, not engine policy.
     *
     * <p>The counts come from independent reads rather than one transactional cut, so executions
     * transitioning during the query may disagree between the numbers (read skew, DDIA ch. 7). That
     * is acceptable for polling, and a serialisable cut here would be cost without benefit.
     */
    OverviewSnapshot overview(Duration throughputWindow);

    /**
     * Up to {@code query.limit()} executions, ordered by descending id (UUIDv7) — see
     * {@link ExecutionQuery}.
     *
     * <p>A SUMMARY: {@code attempts()} comes back empty in a listing (a dashboard read — one query,
     * without the arbitrarily large {@code error} column); the detail with attempts is
     * {@link #findExecution}.
     */
    List<Execution> executions(ExecutionQuery query);

    MohsLifecycle lifecycle();
}
