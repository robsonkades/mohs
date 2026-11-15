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
import org.springframework.core.task.AsyncTaskExecutor;

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
import io.mohs.core.SubmissionOptions;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.core.schedule.RecurringOptions;
import io.mohs.core.schedule.RecurringSchedule;
import io.mohs.core.ThroughputReading;
import io.mohs.core.definition.DefinitionSource;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.BatchCompleted;
import io.mohs.core.event.Enqueued;
import io.mohs.core.event.ExecutionListener;
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
 * {@link LeaseStore}) with typed acceptance and operational controls.
 *
 * <p>{@link #batch} writes the batch row and its members — history plus queue — in a SINGLE
 * transaction ({@link SubmissionStore}): the partial failure of "a batch written with only some of
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

    /** Default application audit identity for batch members. */
    static final String DEFAULT_ACTOR = "application";

    private final JobStore jobStore;
    private final WorkQueue workQueue;
    private final HistoryStore historyStore;
    private final LeaseStore leaseStore;
    private final SubmissionStore submissionStore;
    private final NodeStore nodeStore;
    private final RateLimitStore rateLimitStore;
    private final HandlerRegistry handlerRegistry;
    private final Clock clock;
    private final MohsLifecycle lifecycle;
    private final BatchStore batchStore;
    private final RunnerRegistry runnerRegistry;
    /** The local wake-up tier: wakes the local engine's loop after the commit — {@code Engine#signalWorkScheduled}; best-effort by contract. */
    private final Runnable localWakeSignal;
    /** The same listeners the dispatcher publishes to — {@code Enqueued} is the one event born on this side of the engine. */
    private final ExecutionEventPublisher events;

    /**
     * Creates a {@code MohsImpl} with the supplied values.
     *
     * @param jobStore the persistence port for job definitions and triggers
     * @param workQueue the persistence port for ready work
     * @param historyStore the persistence port for execution history
     * @param leaseStore the persistence port for execution ownership
     * @param submissionStore the transaction boundary shared by store operations
     * @param nodeStore the persistence port for node heartbeats
     * @param rateLimitStore the persistence port for shared rate-limit buckets
     * @param handlerRegistry the registry of local job handlers and payload types
     * @param clock the time source used by the component
     * @param lifecycle the local engine lifecycle controls
     * @param batchStore the persistence port for batch counters
     * @param runnerRegistry the registry of local execution resources
     * @param localWakeSignal the callback that wakes the local poll loop after durable enqueue
     * @param listeners the listeners receiving execution events
     * @param eventExecutor the executor that delivers execution events
     */
    public MohsImpl(JobStore jobStore, WorkQueue workQueue, HistoryStore historyStore, LeaseStore leaseStore,
            SubmissionStore submissionStore, NodeStore nodeStore, RateLimitStore rateLimitStore,
            HandlerRegistry handlerRegistry, Clock clock, MohsLifecycle lifecycle, BatchStore batchStore,
            RunnerRegistry runnerRegistry, Runnable localWakeSignal,
            List<ExecutionListener> listeners, AsyncTaskExecutor eventExecutor) {
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.workQueue = Objects.requireNonNull(workQueue, "workQueue");
        this.historyStore = Objects.requireNonNull(historyStore, "historyStore");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.submissionStore = Objects.requireNonNull(submissionStore, "submissionStore");
        this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
        this.rateLimitStore = Objects.requireNonNull(rateLimitStore, "rateLimitStore");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "handlerRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
        this.runnerRegistry = Objects.requireNonNull(runnerRegistry, "runnerRegistry");
        this.localWakeSignal = Objects.requireNonNull(localWakeSignal, "localWakeSignal");
        this.events = new ExecutionEventPublisher(Objects.requireNonNull(listeners, "listeners"),
                Objects.requireNonNull(eventExecutor, "eventExecutor"));
    }

    @Override
    public <T> Enqueued enqueue(JobRef<T> job, T input, SubmissionOptions options) {
        return scheduleAt(job, input, clock.instant(), options);
    }

    @Override
    public <T> Enqueued scheduleAfter(JobRef<T> job, T input, Duration delay, SubmissionOptions options) {
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative()) throw new IllegalArgumentException("delay must not be negative");
        final Instant when;
        try {
            when = clock.instant().plus(delay);
        } catch (java.time.DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException("delay exceeds the supported instant range", e);
        }
        return scheduleAt(job, input, when, options);
    }

    @Override
    public <T> Enqueued scheduleAt(JobRef<T> job, T input, Instant when, SubmissionOptions options) {
        Objects.requireNonNull(when, "when");
        Objects.requireNonNull(options, "options");
        requireExecutable(job, input);
        JobPayload encoded = prepare(job, input);
        ExecutionId id = ExecutionId.of(UUIDv7.randomUUIDString());
        Enqueued receipt = new Enqueued(id, job.key(), when, options.actor());
        var execution = new SubmissionStore.NewExecution(id, job.key(), Shards.of(id), options.priority().value(),
                receipt.scheduledAt(), clock.instant(), options.actor(), null, options.idempotencyKey(), encoded,
                SubmissionStore.Origin.MANUAL, null);
        try {
            submissionStore.accept(List.of(execution), null, () -> {
                events.publish(receipt);
                if (!receipt.scheduledAt().isAfter(clock.instant())) localWakeSignal.run();
            });
            return receipt;
        } catch (org.springframework.dao.DuplicateKeyException conflict) {
            if (options.idempotencyKey() == null) throw conflict;
            return historyStore.findReceiptByIdempotencyKey(job.key(), options.idempotencyKey()).orElseThrow(() -> conflict);
        }
    }

    @Override
    public <T> RecurringSchedule scheduleRecurring(String scheduleId, JobRef<T> job, T input,
            Schedule recurrence, RecurringOptions options) {
        Objects.requireNonNull(recurrence, "recurrence");
        Objects.requireNonNull(options, "options");
        JobKey key = JobKey.of(scheduleId);
        if (recurrence instanceof OnDemandSpec) {
            throw new IllegalArgumentException("scheduleRecurring requires CronSpec or IntervalSpec");
        }
        ResolvedExecutable executable = requireExecutable(job, input);
        if (key.equals(job.key())) throw new IllegalArgumentException("schedule ID must differ from executable ID");
        if (!job.key().equals(executable.key())) {
            throw new IllegalArgumentException("a recurrence must reference executable code, not another recurrence");
        }
        JobOptions policy = options.jobOptions() == null ? JobDefinitions.options(executable.definition()) : options.jobOptions();
        JobDefinition definition = JobDefinitions.definition(key, policy, recurrence, options.misfire(), options.startPaused())
                .withExecutableKey(job.key()).withInputContract(job.inputContract());
        handlerRegistry.validatePolicy(definition);
        JobPayload encoded = prepare(job, input);
        StoredJob stored = jobStore.upsertBound(definition, new JobBinding(key, job.key(), encoded));
        return new RecurringSchedule(key, job.key(), stored.definition().schedule(), stored.paused(), stored.nextFireAt());
    }

    private ResolvedExecutable requireExecutable(JobRef<?> ref, Object input) {
        Objects.requireNonNull(ref, "job");
        Objects.requireNonNull(input, "input");
        StoredJob stored = jobStore.find(ref.key()).orElseThrow(() ->
                new IllegalArgumentException("no active job '" + ref.key().value()
                        + "'; declare a JobRegistration bean or an @OnDemandJob handler"));
        JobKey executable = stored.definition().executableKey();
        if (handlerRegistry.find(executable).isEmpty()) {
            throw new IllegalArgumentException("job '" + ref.key().value()
                    + "' has no registered executable; declare a JobRegistration bean or an @OnDemandJob handler");
        }
        Class<?> expected = org.springframework.util.ClassUtils.resolvePrimitiveIfNecessary(
                handlerRegistry.payloadType(executable).orElseThrow(() ->
                new IllegalArgumentException("job '" + ref.key().value() + "' declares no typed input")));
        if (!expected.equals(org.springframework.util.ClassUtils.resolvePrimitiveIfNecessary(ref.payloadType()))
                || !expected.isInstance(input)) {
            throw new IllegalArgumentException("job '" + ref.key().value() + "' expects " + expected.getName()
                    + "; reference declares " + ref.payloadType().getName() + "; received " + input.getClass().getName());
        }
        handlerRegistry.requireContract(executable, ref.inputContract());
        return new ResolvedExecutable(executable, stored.definition());
    }

    private record ResolvedExecutable(JobKey key, JobDefinition definition) {}

    private JobPayload prepare(JobRef<?> ref, Object input) {
        try {
            return InputCodec.JSON.encode(ref, input);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid input for job '" + ref.key().value() + "': " + e.getMessage(), e);
        }
    }

    /**
     * The batch's total is fixed at creation, so members are collected BEFORE the row exists: there is
     * no "still accepting members" state to track, and the batch is born already knowing how many
     * completions will close it. An empty batch is refused at the door — it would never complete, and
     * a forever-open batch is worse than an error.
     */
    @Override
    public Batch batch(String name, Consumer<BatchBuilder> configurer) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(configurer, "configurer");
        requireBatchName(name);
        List<Member> members = collectMembers(configurer);
        if (members.isEmpty()) {
            throw new IllegalArgumentException("a batch needs at least one member — an empty batch would never complete");
        }

        requireAllDefined(members);

        String batchId = UUIDv7.randomUUIDString();
        Instant scheduledAt = clock.instant();
        List<SubmissionStore.NewExecution> prepared = new ArrayList<>(members.size());
        List<Enqueued> receipts = new ArrayList<>(members.size());
        for (Member member : members) {
            ExecutionId id = ExecutionId.of(UUIDv7.randomUUIDString());
            JobPayload encoded = InputCodec.JSON.encode(member.job(), member.payload());
            var execution = new SubmissionStore.NewExecution(id, member.job().key(), Shards.of(id), Priority.NORMAL.value(),
                    scheduledAt, scheduledAt, DEFAULT_ACTOR, batchId, null, encoded, SubmissionStore.Origin.MANUAL, null);
            prepared.add(execution);
            receipts.add(new Enqueued(id, member.job().key(), scheduledAt, DEFAULT_ACTOR));
        }
        submissionStore.accept(prepared, new SubmissionStore.NewBatch(batchId, name), () -> {
            receipts.forEach(events::publish);
            localWakeSignal.run();
        });
        return new Batch(batchId);
    }

    /**
     * Validated HERE, and not only in BatchSnapshot/BatchCompleted's compact constructor: a blank name
     * crossed the write, became durable, and only blew up on READ — BatchCompleted's constructor
     * threw inside the event channel, where the exception is swallowed by design, and the user's
     * onCompletion never ran. A value the API accepts must not be a value it cannot read back.
     */
    private static void requireBatchName(String name) {
        if (name.isBlank()) {
            throw new IllegalArgumentException("a batch name must not be blank — it is the label the operator reads"
                    + " instead of the batchId");
        }
        if (name.length() > MAX_BATCH_NAME_LENGTH) {
            throw new IllegalArgumentException("a batch name must be at most " + MAX_BATCH_NAME_LENGTH
                    + " characters (the column limit on every supported database), got " + name.length());
        }
    }

    private static List<Member> collectMembers(Consumer<BatchBuilder> configurer) {
        CollectingBatchBuilder collected = new CollectingBatchBuilder();
        configurer.accept(collected);
        return collected.members;
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
        Map<JobKey, JobDefinition> definitions = new java.util.HashMap<>();
        for (Member member : members) {
            JobDefinition definition = definitions.computeIfAbsent(member.job().key(), key ->
                    jobStore.find(key).orElseThrow(() ->
                            new IllegalArgumentException("no active job '" + key.value() + "'")).definition());
            handlerRegistry.validateInput(definition.executableKey(), member.payload(), member.job().payloadType());
            handlerRegistry.requireContract(definition.executableKey(), member.job().inputContract());
        }
    }

    @Override
    public Optional<BatchSnapshot> findBatch(String batchId) {
        io.mohs.core.GeneratedId.require(batchId, "batchId");
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

    private record Member(JobRef<?> job, Object payload) {
    }

    /** Accumulates the members; nothing is persisted while the total is not closed. */
    private static final class CollectingBatchBuilder implements BatchBuilder {

        private final List<Member> members = new ArrayList<>();

        @Override
        public <T> void add(JobRef<T> ref, T payload) {
            Objects.requireNonNull(ref, "ref");
            Objects.requireNonNull(payload, "payload");
            members.add(new Member(ref, payload));
        }
    }

    /** An unknown (or already retired) job is a no-op — the same stance as {@link Mohs#pause}. */
    @Override
    public void remove(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        jobStore.remove(jobKey);
    }

    @Override
    public boolean reactivate(JobKey jobKey) {
        return jobStore.reactivate(Objects.requireNonNull(jobKey, "jobKey"), handlerRegistry.freeze());
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
        if (!(schedule instanceof OnDemandSpec)
                && jobStore.findBinding(jobKey).filter(binding -> binding.input() == null).isPresent()
                && jobStore.find(jobKey).filter(job -> job.definition().schedule() instanceof OnDemandSpec).isPresent()) {
            throw new IllegalArgumentException("job '" + jobKey.value()
                    + "' has no recurring input; use scheduleRecurring with parameters and a distinct schedule ID");
        }

        if (!jobStore.reschedule(jobKey, schedule)) {
            return Optional.empty();
        }

        return findJob(jobKey);
    }

    @Override
    public Optional<JobRef<?>> jobRef(JobKey jobKey) {
        Objects.requireNonNull(jobKey, "jobKey");
        return jobStore.find(jobKey).flatMap(stored -> {
            var executable = stored.definition().executableKey();
            if (handlerRegistry.find(executable).isEmpty()) return Optional.empty();
            var local = handlerRegistry.reference(executable);
            return Optional.of(new JobRef<>(jobKey, local.payloadType(), local.inputContract()));
        });
    }

    /** The store arbitrates queued cancellation and a running cancellation request in one transaction. */
    @Override
    public Optional<Execution> cancel(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        workQueue.cancel(executionId, clock.instant());
        return historyStore.find(executionId, clock.instant());
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
                    : new IllegalStateException("execution " + executionId + " cannot be reopened — only standalone manual executions of an active job can be retried; "
                            + "submit a new execution to repeat a completed automatic occurrence");
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
        ExecutionId cursor = query.cursor() == null ? null : ExecutionId.of(query.cursor());
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
