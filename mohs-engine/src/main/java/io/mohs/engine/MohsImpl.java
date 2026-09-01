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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.robsonkades.uuidv7.UUIDv7;

import io.mohs.core.Batch;
import io.mohs.core.BatchBuilder;
import io.mohs.core.BatchSnapshot;
import io.mohs.core.ExecutionQuery;
import io.mohs.core.JobSnapshot;
import io.mohs.core.Mohs;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.NodeSnapshot;
import io.mohs.core.OverviewSnapshot;
import io.mohs.core.RateLimitSnapshot;
import io.mohs.core.RunnerSnapshot;
import io.mohs.core.ScheduleCommand;
import io.mohs.core.ThroughputReading;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.core.resource.RateLimit;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Schedule;

/**
 * {@link Mohs} over the persistence ports ({@link WorkQueue}/{@link HistoryStore}/
 * {@link LeaseStore}) — {@code define} and {@code remove} delegate directly, while
 * {@code schedule} assembles a {@link ScheduleCommandImpl}. {@link #lifecycle()} returns the
 * injected {@link Engine}, which implements {@link MohsLifecycle} directly.
 *
 * <p>{@link #batch} writes the batch row and its members — history plus queue — in a SINGLE
 * transaction ({@link StoreTransactions}): the partial failure of "a batch written with only some of
 * its members" becomes structurally impossible. The completion count stays in the completion
 * transaction — there is no counter here.
 */
public final class MohsImpl implements Mohs {

    /**
     * The overview's short reading window — fixed, and not a knob: it does not exist for the user to
     * choose a slice, it exists to be DIVIDED. 10s is short enough to mean "now" and long enough not
     * to become sampling noise at one execution per second.
     *
     * <p><b>And it is a floor, not merely a preference:</b> it must be at least the sampler's cadence
     * ({@code OverviewStreamBroadcaster.STREAM_INTERVAL}, today 2s, whose Javadoc invites it to become
     * a property). With a window SHORTER than the tick, each frame describes only a fraction of the
     * elapsed interval and the panel starts IGNORING the rest of the work — with nothing to flag it,
     * because each isolated reading remains correct. The two constants live in different modules and
     * the compiler will never connect them: whoever raises the tick above 10s moves this along with it.
     */
    private static final Duration RECENT_WINDOW = Duration.ofSeconds(10);

    /** The ceiling of mohs_batches' `name` column in all four databases — validated here so it becomes an error that teaches, not an INSERT failure. */
    private static final int MAX_BATCH_NAME_LENGTH = 255;

    private static final Logger log = LoggerFactory.getLogger(MohsImpl.class);

    /** The actor for callers of {@link Mohs#schedule}/{@link Mohs#batch} without an explicit {@link ScheduleCommand#as(String)} — the process itself, not an identifiable user. */
    static final String DEFAULT_ACTOR = "application";

    private final JobStore jobStore;
    private final WorkQueue workQueue;
    private final HistoryStore historyStore;
    private final LeaseStore leaseStore;
    private final StoreTransactions storeTransactions;
    private final NodeStore nodeStore;
    private final RateLimitStore rateLimitStore;
    private final HandlerRegistry handlerRegistry;
    private final Clock clock;
    private final MohsLifecycle lifecycle;
    private final BatchStore batchStore;
    private final BatchCompletionCallbacks callbacks;
    private final RunnerRegistry runnerRegistry;
    /** The local wake-up tier: wakes the local engine's loop after the commit — {@code Engine#signalWorkScheduled}; best-effort by contract. */
    private final Runnable localWakeSignal;

    public MohsImpl(JobStore jobStore, WorkQueue workQueue, HistoryStore historyStore, LeaseStore leaseStore,
            StoreTransactions storeTransactions, NodeStore nodeStore, RateLimitStore rateLimitStore,
            HandlerRegistry handlerRegistry, Clock clock, MohsLifecycle lifecycle, BatchStore batchStore,
            BatchCompletionCallbacks callbacks, RunnerRegistry runnerRegistry, Runnable localWakeSignal) {
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.workQueue = Objects.requireNonNull(workQueue, "workQueue");
        this.historyStore = Objects.requireNonNull(historyStore, "historyStore");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.storeTransactions = Objects.requireNonNull(storeTransactions, "storeTransactions");
        this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
        this.rateLimitStore = Objects.requireNonNull(rateLimitStore, "rateLimitStore");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
        this.callbacks = Objects.requireNonNull(callbacks, "callbacks");
        this.runnerRegistry = Objects.requireNonNull(runnerRegistry, "runnerRegistry");
        this.localWakeSignal = Objects.requireNonNull(localWakeSignal, "localWakeSignal");
    }

    @Override
    public <T> ScheduleCommand schedule(JobRef<T> ref, T payload) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(payload, "payload");
        return new ScheduleCommandImpl(jobStore, historyStore, workQueue, storeTransactions, clock, ref.key(), payload,
                localWakeSignal);
    }

    @Override
    public ScheduleCommand schedule(String jobId, Object payload) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(payload, "payload");
        return new ScheduleCommandImpl(jobStore, historyStore, workQueue, storeTransactions, clock, JobKey.of(jobId), payload,
                localWakeSignal);
    }

    /**
     * The batch's total is fixed at creation, so members are collected BEFORE the row exists: there is
     * no "still accepting members" state to track, and the batch is born already knowing how many
     * completions will close it. An empty batch is refused at the door — it would never complete, and
     * a forever-open batch is worse than an error.
     */
    @Override
    public Batch batch(String name, Consumer<BatchBuilder> configurer) {
        // Validated HERE, and not only in BatchSnapshot/BatchCompleted's compact constructor: a blank
        // name crossed the write, became durable, and only blew up on READ — BatchCompleted's
        // constructor threw inside the event channel, where the exception is swallowed by design, and
        // the user's onCompletion never ran. A value the API accepts must not be a value it cannot
        // read back
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(configurer, "configurer");
        if (name.isBlank()) {
            throw new IllegalArgumentException("a batch name must not be blank — it is the label the operator reads"
                    + " instead of the batchId");
        }
        if (name.length() > MAX_BATCH_NAME_LENGTH) {
            throw new IllegalArgumentException("a batch name must be at most " + MAX_BATCH_NAME_LENGTH
                    + " characters (the column limit on every supported database), got " + name.length());
        }
        List<Member> members = collectMembers(configurer);
        if (members.isEmpty()) {
            throw new IllegalArgumentException("a batch needs at least one member — an empty batch would never complete");
        }

        requireAllDefined(members);

        String batchId = UUIDv7.randomUUIDString();
        // The batch row plus its members (history AND queue) in a single transaction: the partial
        // failure that left the batch written with M < N members — and a BatchCompleted that never
        // comes — dies by construction
        storeTransactions.inTransaction(() -> {
            batchStore.insert(batchId, name, members.size());
            enqueueMembers(members, batchId);
        });
        // membros nascem devidos (scheduledAt = now) — tier 1 acorda o loop
        localWakeSignal.run();
        return new BatchImpl(batchId, callbacks);
    }

    private static List<Member> collectMembers(Consumer<BatchBuilder> configurer) {
        CollectingBatchBuilder collected = new CollectingBatchBuilder();
        configurer.accept(collected);
        return collected.members;
    }

    /**
     * Every member is born {@code ENQUEUED} carrying the {@code batchId} — that is what makes its
     * completion count towards the batch — and they all share the same {@code scheduledAt}, because
     * the batch was requested in one go.
     */
    private void enqueueMembers(List<Member> members, String batchId) {
        Instant scheduledAt = clock.instant();
        for (Member member : members) {
            ExecutionId id = ExecutionId.of(UUIDv7.randomUUIDString());
            int shard = Shards.of(id);
            historyStore.record(List.of(new HistoryStore.NewExecution(id, member.key(), shard, Priority.NORMAL.value(),
                    scheduledAt, scheduledAt, DEFAULT_ACTOR, batchId, null, member.payload())));
            workQueue.offer(List.of(new WorkQueue.ReadyEntry(id, member.key(), shard, Priority.NORMAL.value(), 1, scheduledAt)));
        }
    }

    /**
     * ALL members are validated before any write, rather than one at a time during enqueueing: a
     * nonexistent job halfway through would leave the batch row written with a full {@code total} and
     * only part of the members queued — the rest would never exist, the batch would never close, and
     * {@code BatchCompleted} would never fire, silently. That is exactly the failure mode the batch
     * design exists to avoid.
     *
     * <p>Distinct keys, not one query per member: a large batch usually points at few jobs, and
     * validating 1,000 members of a single job is one question, not a thousand.
     */
    private void requireAllDefined(List<Member> members) {
        members.stream().map(Member::key).distinct().forEach(this::requireDefined);
    }

    /** The same reason as {@code ScheduleCommandImpl.at}: without this the caller would see a raw foreign-key violation rather than a message that teaches. */
    private void requireDefined(JobKey key) {
        if (jobStore.find(key).isEmpty()) {
            throw new IllegalArgumentException(
                    "no job registered for id '" + key.value() + "' — call Mohs.define first");
        }
    }

    @Override
    public Optional<BatchSnapshot> findBatch(String batchId) {
        Objects.requireNonNull(batchId, "batchId");
        return batchStore.find(batchId)
                .map(counters -> new BatchSnapshot(counters.batchId(), counters.name(), counters.total(), counters.succeeded(),
                        counters.failed()));
    }

    /**
     * The batch has already counted this failure, and the counter cannot give it back without
     * reopening the batch — which would stop {@code BatchCompleted} being terminal and, worse, the
     * second event would no longer find {@code onCompletion}'s one-shot callback: whoever rescued the
     * member is precisely who would be left without the notification of the real end. An explicit
     * refusal, then, rather than silent double counting.
     */
    private static IllegalStateException batchMemberNotRetryable(ExecutionId executionId, String batchId) {
        return new IllegalStateException("execution " + executionId + " is a member of batch " + batchId
                + " — a batch member is not retried individually, because the batch already counted this"
                + " failure and counting it again would close the batch early. Schedule the job standalone"
                + " to redo the work");
    }

    private record Member(JobKey key, Object payload) {
    }

    /** Accumulates the members; nothing is persisted while the total is not closed. */
    private static final class CollectingBatchBuilder implements BatchBuilder {

        private final List<Member> members = new ArrayList<>();

        @Override
        public <T> void add(JobRef<T> ref, T payload) {
            Objects.requireNonNull(ref, "ref");
            Objects.requireNonNull(payload, "payload");
            members.add(new Member(ref.key(), payload));
        }
    }

    /** The receipt: {@code batchId} is already durable when this returns. */
    private record BatchImpl(String batchId, BatchCompletionCallbacks callbacks) implements Batch {

        @Override
        public Batch onCompletion(Consumer<BatchCompleted> callback) {
            callbacks.register(batchId, callback);
            return this;
        }
    }

    @Override
    public void define(JobDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        jobStore.upsert(definition);
    }

    /** An unknown (or already retired) job is a no-op — the same stance as {@link Mohs#pause}. */
    @Override
    public void remove(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.find(jobKey).ifPresent(stored -> {
            if (stored.definition().source() == DefinitionSource.ANNOTATION) {
                throw new IllegalArgumentException("job '" + jobKey.value()
                        + "' is @MohsJob-annotated — Mohs.remove only retires PROGRAMMATIC definitions; "
                        + "remove the annotation instead and the scanner marks it ORPHANED on the next boot");
            }
            jobStore.remove(jobKey);
        });
    }

    @Override
    public Optional<JobSnapshot> findJob(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        return jobStore.find(jobKey).map(this::toSnapshot);
    }

    @Override
    public List<JobSnapshot> jobs() {
        try (var stored = jobStore.findAll()) {
            return stored.map(this::toSnapshot).toList();
        }
    }

    @Override
    public void pause(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.pause(jobKey);
    }

    @Override
    public void resume(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.resume(jobKey);
    }

    @Override
    public Optional<JobSnapshot> reschedule(JobKey jobKey, Schedule schedule) {
        Objects.requireNonNull(jobKey, "jobKey");
        Objects.requireNonNull(schedule, "schedule");

        if (!jobStore.reschedule(jobKey, schedule)) {
            return Optional.empty();
        }

        return findJob(jobKey);
    }

    @Override
    public Optional<Class<?>> payloadType(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        return handlerRegistry.payloadType(jobKey);
    }

    /**
     * The orchestration of cancellation's two halves: first the pending CAS; if that loses (the row is
     * already running, or already finished), it tries {@code RUNNING}'s cooperative flag.
     *
     * <p>The two predicates partition the state space, but the state can migrate BETWEEN the checks
     * (time-of-check/time-of-use — DDIA ch. 7: a CAS covers a predicate, not a sequence): an attempt
     * completion taking {@code RUNNING} to {@code RETRY_WAITING} in the middle of the pair would let
     * the operator's order fall into the gap. The second pass closes the window — another migration
     * would require a whole attempt cycle within microseconds. On a terminal state both remain no-ops:
     * cancelling what has already decided changes nothing, and the return shows the state that stood.
     *
     * <p>A declared window: the cooperative flag lives on the LEASE and dies with it — a cancel landing
     * between the end of the handler and the flush's commit (at most the flush interval) is lost, and
     * an eventual retry runs. Acceptable for cooperative cancellation (the operator re-cancels the
     * retry); the era with the flag on the execution row had no such window.
     */
    @Override
    public Optional<Execution> cancel(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");

        boolean cancelledPending = workQueue.cancelQueued(executionId, clock.instant());

        if (!cancelledPending && !leaseStore.requestCancellation(executionId)) {
            cancelledPending = workQueue.cancelQueued(executionId, clock.instant());
            if (!cancelledPending) {
                leaseStore.requestCancellation(executionId);
            }
        }

        Optional<Execution> result = historyStore.find(executionId, clock.instant());
        if (cancelledPending) {
            result.ifPresent(this::rearmAfterFinishChain);
        }

        return result;
    }

    /**
     * The fixed-delay chain's cure: a cancelled scheduler occurrence that is still pending never goes
     * through the completion path that rearms the trigger, and without this the chain would die in
     * silence ({@code next_fire_at} left {@code NULL}, awaiting an end that never comes).
     *
     * <p>Only a scheduler occurrence — a cancelled manual execution is not the chain.
     * {@link JobStore#armNextFire}'s {@code IS NULL} guard protects against rearming a series that is
     * already live.
     *
     * <p>An accepted residual window: a crash between {@code cancelIfPending} and this rearm leaves the
     * chain disarmed — the upsert's {@code NULL} cure (at boot, or on define) rearms it. Making it
     * transactional would require leaking the storage boundary up to here.
     */
    private void rearmAfterFinishChain(Execution execution) {
        if (!Execution.SCHEDULER_ACTOR.equals(execution.actor())) {
            return;
        }
        jobStore.find(execution.jobKey()).ifPresent(stored -> {
            if (stored.definition().schedule() instanceof IntervalSpec(Duration interval, boolean afterFinish) && afterFinish) {
                jobStore.armNextFire(execution.jobKey(), clock.instant().plus(interval));
            }
        });
    }

    @Override
    public Optional<Execution> findExecution(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        return historyStore.find(executionId, clock.instant());
    }

    /**
     * The port's CAS is the authority; the read only tells the reasons for defeat apart (nonexistent
     * versus retired job versus wrong state) — it never decides.
     *
     * <p>A defeat with the row still {@code FAILED} means the {@code retired} guard blocked it (the CAS
     * only refuses FAILED through that route); {@code RETRY_WAITING} means a likely duplicate POST.
     * Losing the race to another mutation between the CAS and the read changes the message, not the
     * outcome: whoever won the CAS won it.
     */
    @Override
    public Optional<Execution> retry(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        if (workQueue.rearmForManualRetry(executionId, clock.instant())) {
            log.info("execution {} manually rearmed for retry — rejoins the claim path bypassing the retries budget", executionId.value());
            return historyStore.find(executionId, clock.instant());
        }
        Execution current = historyStore.find(executionId, clock.instant()).orElse(null);
        if (current == null) {
            return Optional.empty();
        }
        throw switch (current.state()) {
            case FAILED -> current.batchId() != null
                    ? batchMemberNotRetryable(executionId, current.batchId())
                    : new IllegalStateException("execution " + executionId + " belongs to a removed job — "
                            + "a retried execution of a retired job would never be claimed");
            case ENQUEUED, RETRY_WAITING -> new IllegalStateException("execution " + executionId
                    + " is already queued to run again — likely a duplicate retry request");
            default -> new IllegalStateException("execution " + executionId + " is " + current.state()
                    + " — only FAILED executions can be manually retried (a cancelled execution was an explicit "
                    + "decision; the other states are owned by the engine)");
        };
    }

    @Override
    public List<Execution> executions(ExecutionQuery query) {
        Objects.requireNonNull(query, "query");
        // A blank cursor (?cursor= from REST) means the first page, not an IAE from ExecutionId.of
        String rawCursor = query.cursor();
        ExecutionId cursor = rawCursor == null || rawCursor.isBlank() ? null : ExecutionId.of(rawCursor);
        return historyStore.findPage(query.jobKey(), query.status(), query.from(), query.to(), cursor, query.limit(), clock.instant());
    }

    /** Most recent first (ties broken by nodeId — an order exposed through an API is contract, never the table's physical order): the living matter before the suspect, and the heartbeat's age IS the information. */
    @Override
    public List<NodeSnapshot> nodes() {
        return nodeStore.findAll().stream()
                .sorted(Comparator.comparing(StoredNode::lastHeartbeatAt).reversed()
                        .thenComparing(StoredNode::nodeId))
                .map(stored -> new NodeSnapshot(stored.nodeId(), stored.state(), stored.lastHeartbeatAt()))
                .toList();
    }

    /** The only facade read that does not touch the database: a runner is this process's thread pool (see {@link RunnerSnapshot}). */
    @Override
    public List<RunnerSnapshot> runners() {
        return runnerRegistry.snapshots();
    }

    /** Ordered by name: the list is read by people, and a stable order between calls is the minimum for comparing two snapshots. */
    @Override
    public List<RateLimitSnapshot> rateLimits() {
        try (var declared = rateLimitStore.findAll()) {
            return declared.sorted(Comparator.comparing(snapshot -> snapshot.rateLimit().name())).toList();
        }
    }

    /**
     * Deliberately a two-step adjustment: {@link RateLimitStore#upsert} would create the limit if it
     * did not exist, and creating a limit is not what a PATCH does — declaring is an act of boot.
     *
     * <p>The race between the find and the upsert is accepted: two operators adjusting the SAME limit
     * at the same instant is "last write wins", which is what a PATCH promises.
     */
    @Override
    public Optional<RateLimitSnapshot> adjustRateLimit(String name, int max, Duration window) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(window, "window");
        if (rateLimitStore.find(name).isEmpty()) {
            return Optional.empty();
        }
        rateLimitStore.upsert(new RateLimit(name, max, window));
        return rateLimitStore.find(name);
    }

    /**
     * Pure composition of the port's three counts — live work, the requested window's throughput and
     * the short window's; normalisation (zeros, live states only) belongs to {@link OverviewSnapshot}
     * itself.
     *
     * <p>The window is validated BEFORE touching the database (Effective Java, Item 49): with
     * {@code ?window=} exposed over REST, letting the snapshot reject it at the end would cost three
     * queries — one of them with {@code since} in the future — to arrive at the same IAE, and the rule
     * remains a single one, the record's.
     */
    @Override
    public OverviewSnapshot overview(Duration throughputWindow) {
        Objects.requireNonNull(throughputWindow, "throughputWindow");
        if (!throughputWindow.isPositive()) {
            throw new IllegalArgumentException("throughputWindow must be positive, got " + throughputWindow);
        }
        // ONE clock for all three reads: with now re-read between them, the short window could end
        // after the long one and the rate would come from an interval other than the one it declares
        Instant now = clock.instant();
        return new OverviewSnapshot(historyStore.countActiveByState(now),
                reading(throughputWindow, now), reading(RECENT_WINDOW, now));
    }

    /**
     * One throughput reading. The SHORT window exists to be divided: the live counts are instantaneous
     * gauges and, by Little's Law, sit at zero for a fast job — without a rate, the dashboard has no
     * way to tell "idle" from "working quickly".
     */
    private ThroughputReading reading(Duration window, Instant now) {
        Map<ExecutionState, Long> outcomes = historyStore.countTerminalOutcomesSince(now.minus(window));
        return new ThroughputReading(window,
                outcomes.getOrDefault(ExecutionState.SUCCEEDED, 0L),
                outcomes.getOrDefault(ExecutionState.FAILED, 0L));
    }

    @Override
    public MohsLifecycle lifecycle() {
        return lifecycle;
    }

    /**
     * {@code nextFireAt} is the trigger's real state, not a recomputation against the clock — which
     * lied for fixed-delay (the next firing is unknown until the execution finishes) and ignored
     * misfire. A paused job shows {@code null}: a pause blocks the trigger.
     */
    private JobSnapshot toSnapshot(StoredJob stored) {
        return new JobSnapshot(stored.definition(), stored.paused(), stored.paused() ? null : stored.nextFireAt());
    }
}
