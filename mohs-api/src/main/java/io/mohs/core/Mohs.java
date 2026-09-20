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
import java.time.Instant;
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
import io.mohs.core.schedule.RecurringOptions;
import io.mohs.core.schedule.RecurringSchedule;
import io.mohs.core.event.Enqueued;

/**
 * Mohs's public facade for typed submissions, named recurring schedules and execution controls.
 * Declare reusable code with {@link io.mohs.core.definition.JobRegistration} before submitting.
 *
 * <p>The read methods ({@link #findJob}/{@link #jobs}/{@link #findExecution}/{@link #executions}/
 * {@link #jobRef}) exist for {@code io.mohs.rest}: the architectural boundary
 * ({@code ArchitectureTest#rest_only_sees_public_api}) forbids the REST layer from seeing
 * {@code io.mohs.engine} directly, so this facade is the only read path available to it.
 */
public interface Mohs {

    /**
     * Enqueues one typed invocation for immediate background eligibility after commit.
     *
     * @param <T>   the input contract
     * @param job   the registered executable and its input contract
     * @param input the invocation data, persisted as a JSON snapshot
     * @return the acceptance receipt, subject to any enclosing transaction's commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    default <T> Enqueued enqueue(JobRef<T> job, T input) {
        return enqueue(job, input, SubmissionOptions.defaults());
    }

    /**
     * Enqueues one invocation with explicit priority, actor or deduplication. Never executes inline.
     *
     * @param <T>     the input contract
     * @param job     the registered executable and its input contract
     * @param input   the invocation data, persisted as a JSON snapshot
     * @param options the immutable options for this operation
     * @return the acceptance receipt, subject to any enclosing transaction's commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    <T> Enqueued enqueue(JobRef<T> job, T input, SubmissionOptions options);

    /**
     * Persists one invocation now, eligible at the supplied instant after commit. Past instants are due.
     *
     * @param <T>   the input contract
     * @param job   the registered executable and its input contract
     * @param input the invocation data, persisted as a JSON snapshot
     * @param when  the earliest eligibility instant; past instants are due
     * @return the acceptance receipt, subject to any enclosing transaction's commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    default <T> Enqueued scheduleAt(JobRef<T> job, T input, Instant when) {
        return scheduleAt(job, input, when, SubmissionOptions.defaults());
    }

    /**
     * Persists one invocation with an absolute due instant and explicit submission options.
     *
     * @param <T>     the input contract
     * @param job     the registered executable and its input contract
     * @param input   the invocation data, persisted as a JSON snapshot
     * @param when    the earliest eligibility instant; past instants are due
     * @param options the immutable options for this operation
     * @return the acceptance receipt, subject to any enclosing transaction's commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    <T> Enqueued scheduleAt(JobRef<T> job, T input, Instant when, SubmissionOptions options);

    /**
     * Persists one invocation after a nonnegative delay measured with Mohs's clock.
     *
     * @param <T>   the input contract
     * @param job   the registered executable and its input contract
     * @param input the invocation data, persisted as a JSON snapshot
     * @param delay the nonnegative delay measured from Mohs's clock
     * @return the acceptance receipt, subject to any enclosing transaction's commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    default <T> Enqueued scheduleAfter(JobRef<T> job, T input, Duration delay) {
        return scheduleAfter(job, input, delay, SubmissionOptions.defaults());
    }

    /**
     * Persists one invocation after a nonnegative delay with explicit submission options.
     *
     * @param <T>     the input contract
     * @param job     the registered executable and its input contract
     * @param input   the invocation data, persisted as a JSON snapshot
     * @param delay   the nonnegative delay measured from Mohs's clock
     * @param options the immutable options for this operation
     * @return the acceptance receipt, subject to any enclosing transaction's commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    <T> Enqueued scheduleAfter(JobRef<T> job, T input, Duration delay, SubmissionOptions options);

    /**
     * Creates or updates a named recurrence with a durable input snapshot, reusing a registered executable.
     * The ID must be distinct from the executable's ID. Repeating it updates the same schedule;
     * existing executions retain their inputs and operator pause state is preserved.
     * CronSpec and both IntervalSpec forms are accepted; OnDemandSpec is not a recurrence.
     *
     * @param <T>        the input contract
     * @param scheduleId the stable recurring ID, distinct from the executable ID
     * @param job        the registered executable and its input contract
     * @param input      the invocation data, persisted as a JSON snapshot
     * @param recurrence the cron or interval recurrence
     * @return the updated recurring schedule snapshot, subject to commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    default <T> RecurringSchedule scheduleRecurring(String scheduleId, JobRef<T> job, T input, Schedule recurrence) {
        return scheduleRecurring(scheduleId, job, input, recurrence, RecurringOptions.defaults());
    }

    /**
     * Upserts a named recurrence with explicit execution/misfire/initial-pause options.
     * A recurrence cannot change its executable identity. Returns trigger state, not an execution ID.
     *
     * @param <T>        the input contract
     * @param scheduleId the stable recurring ID, distinct from the executable ID
     * @param job        the registered executable and its input contract
     * @param input      the invocation data, persisted as a JSON snapshot
     * @param recurrence the cron or interval recurrence
     * @param options    the immutable options for this operation
     * @return the updated recurring schedule snapshot, subject to commit
     * @throws IllegalArgumentException if the reference, input or options violate the job contract
     */
    <T> RecurringSchedule scheduleRecurring(String scheduleId, JobRef<T> job, T input,
                                            Schedule recurrence, RecurringOptions options);

    /**
     * Accepts a nonempty batch and all of its input snapshots atomically. The name and counters
     * remain queryable through {@link #findBatch}; listeners are best-effort notifications.
     * A receipt inside a host transaction becomes durable only with that transaction's commit.
     * Connection loss after commit can leave the result unknown; batches have no submission
     * idempotency key and must not be retried automatically in that case.
     *
     * @param name       the persisted human-readable name
     * @param configurer the callback collecting typed members before acceptance
     * @return a batch identity, subject to commit of any enclosing transaction
     * @throws IllegalArgumentException for an empty batch or invalid member
     */
    @CheckReturnValue
    Batch batch(String name, Consumer<BatchBuilder> configurer);


    /**
     * Retirement: cancels future firings ({@code ENQUEUED} executions become {@code CANCELLED}) and
     * preserves history.
     *
     * <p>Applies to annotated and programmatic definitions. An unknown job is a no-op.
     * Startup registration preserves retirement; only {@link #reactivate} reverses it.
     * Remove active named recurrences before retiring their shared executable.
     * A joined JDBC transaction must use READ_COMMITTED; retirement rejects a retained snapshot
     * that could omit work accepted before its job lock. Standalone retirement selects this isolation.
     *
     * @param jobKey the stable identity of the job
     */
    void remove(JobKey jobKey);

    /**
     * Reactivates a retained definition with its existing input contract and policy. Starts a new
     * schedule revision and preserves the operator's pause state. Cancelled executions stay cancelled.
     * The local catalog must match the installed cluster catalog and contain its executable.
     *
     * @param jobKey the retained definition identity
     * @return true when a retired definition was reactivated; false when absent or already active
     */
    boolean reactivate(JobKey jobKey);

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
     * @param executionId the identity of the execution
     * @return the execution in its current state right after the request — not necessarily
     * terminal; empty if the id does not exist
     */
    Optional<Execution> cancel(ExecutionId executionId);

    /**
     * A manual retry of a standalone, manually submitted {@code FAILED} execution: it rearms the SAME row as {@code RETRY_WAITING}
     * due now, and the new attempt travels the normal claim path, competing like any other
     * candidate.
     *
     * <p>It bypasses the {@code retries} budget on purpose: the policy protects the system from
     * automatic loops, whereas here the decision is the operator's. It does not go through
     * Idempotency-Key deduplication (nothing new is inserted); the natural idempotence is the CAS
     * itself — repeating the call finds the execution already rearmed and fails with the state
     * exception.
     *
     * @param executionId the identity of the execution
     * @return the execution already rearmed ({@code RETRY_WAITING}); empty if the id does not exist
     * @throws IllegalStateException if the execution exists but is not {@code FAILED} (a cancelled
     *                               one was an explicit decision; the other states have an owner — the engine), belongs to
     *                               a retired job (the rearmed row would never be claimed), was created by an automatic trigger, or is a BATCH MEMBER — the
     *                               batch already counted this failure, and counting it again would close the batch early;
     *                               submit the same input as a new standalone execution to redo the work
     */
    Optional<Execution> retry(ExecutionId executionId);

    /**
     * Looks up a definition and its current operational state.
     *
     * @param jobKey the stable identity of the job
     * @return the job snapshot, or empty when the job does not exist
     */
    Optional<JobSnapshot> findJob(JobKey jobKey);

    /**
     * Every registered job — bounded cardinality (a definition, not an execution), so no pagination.
     *
     * @return the registered job snapshots
     */
    List<JobSnapshot> jobs();

    /**
     * The cluster's nodes with a registered heartbeat, most recent first — bounded cardinality (the
     * cluster's size plus whatever residue the purge has not collected yet), with no pagination,
     * like {@link #jobs()}.
     *
     * <p>Death is not a field: it is derived from the age of {@link NodeSnapshot#lastHeartbeatAt()}
     * at read time. {@code STOPPED} is the only self-reported outcome (a clean shutdown).
     *
     * @return the heartbeat snapshots, most recent first
     */
    List<NodeSnapshot> nodes();

    /**
     * THIS node's runners — their declared configuration and current occupancy. Bounded cardinality
     * (whatever the application declared at boot), with no pagination, like {@link #jobs()}.
     *
     * <p>Unlike {@link #nodes()} and {@link #jobs()}, this read touches no database and does not see
     * the cluster: a thread pool belongs to the process. Consumers must say which node they are
     * talking about — see {@link RunnerSnapshot}.
     *
     * @return the configurations and occupancy of this node's runners
     */
    List<RunnerSnapshot> runners();

    /**
     * The declared rate limits and each one's current bucket balance, by name — bounded
     * cardinality, no pagination, like {@link #jobs()}. A pure read: checking the balance consumes
     * no token.
     *
     * @return the declared limits and current bucket balances, ordered by name
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
     * @param name   the human-readable name
     * @param max    the maximum permitted count
     * @param window the positive window over which the rate is limited
     * @return the adjusted limit, or empty if {@code name} does not exist — declaring a new limit is
     * an act of boot, not of emergency
     */
    Optional<RateLimitSnapshot> adjustRateLimit(String name, int max, Duration window);

    /**
     * Suspends automatic firings; manual scheduling is still allowed (mirroring the engine). No effect if {@code jobKey} does not exist.
     *
     * @param jobKey the stable identity of the job
     */
    void pause(JobKey jobKey);

    /**
     * Resumes automatic firings; an unknown job has no effect.
     *
     * @param jobKey the stable identity of the job
     */
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
     * @param jobKey   the stable identity of the job
     * @param schedule the replacement firing schedule
     * @return the snapshot already carrying the new schedule; empty if the job does not exist (or is
     * retired)
     * @throws IllegalArgumentException if the schedule is unrealisable (a syntactically valid cron
     *                                  that never fires, for instance)
     */
    Optional<JobSnapshot> reschedule(JobKey jobKey, Schedule schedule);

    /**
     * Resolves a registered local input type and stable contract for a live job identity.
     * Named recurrences resolve to their executable's type and contract while retaining their
     * own job key, so an explicit submission keeps its chosen scheduling identity.
     *
     * @param jobKey the job or recurrence identity
     * @return its typed reference, or empty if the definition or local executable is unavailable
     */
    Optional<JobRef<?>> jobRef(JobKey jobKey);

    /**
     * Looks up an execution and its latest completed attempt.
     *
     * @param executionId the identity of the execution
     * @return the execution with its latest completed attempt, or empty when it does not exist
     */
    Optional<Execution> findExecution(ExecutionId executionId);

    /**
     * The batch by the id returned from {@link #batch}. A cheap read, flat in the batch's size: the
     * counter is maintained rather than aggregated from the members.
     *
     * @param batchId the identity of the batch
     * @return the batch counters, or empty if the batch does not exist
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
     *
     * @param throughputWindow the duration of the terminal-count window
     * @return live-work counts and the two independent throughput readings
     */
    OverviewSnapshot overview(Duration throughputWindow);

    /**
     * Up to {@code query.limit()} executions, ordered by descending id (UUIDv7) — see
     * {@link ExecutionQuery}.
     *
     * <p>A SUMMARY: {@code lastAttempt()} comes back empty in a listing (a dashboard read — one query,
     * without the arbitrarily large {@code error} column); the detail with the latest completed attempt is
     * {@link #findExecution}.
     *
     * @param query the execution filters and page limit
     * @return up to the requested limit of executions, ordered by descending ID
     */
    List<Execution> executions(ExecutionQuery query);

    /**
     * Returns the lifecycle controls for this local engine.
     *
     * @return the lifecycle controls for this node
     */
    MohsLifecycle lifecycle();
}
