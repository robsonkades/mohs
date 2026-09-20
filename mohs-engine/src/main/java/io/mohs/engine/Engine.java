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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import io.github.robsonkades.uuidv7.UUIDv7;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;

import io.mohs.core.EngineState;
import io.mohs.core.MohsLifecycle;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;

/**
 * Coordinates session renewal, bounded recovery/firing/claim stages and local handler lifecycle.
 * The database owns durable transitions; this process owns runner permits, dispatch tracking and
 * cooperative interruption. One rotating round budget covers heartbeat and maintenance checkpoints.
 * <p>Candidate policy pages advance through excluded jobs. Database job locks enforce positive
 * cluster grant caps; {@link WorkAdmission} groups shared resource decisions after acquisition.
 * The adaptive poll bounds sleep, not end-to-end dispatch latency.
 * <p>Expired sessions cannot renew. Every result carries execution/session/token/attempt fencing.
 * Finite retries do not guarantee eventual successful delivery, and stale handlers may still perform
 * external effects. Applications must provide business idempotency where duplication matters.
 */
public final class Engine implements MohsLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    /**
     * The ceiling on triggers fired per tick — a boot after a long downtime must not become an
     * unbounded sweep; the surplus stays due and drains over the following ticks, oldest first.
     */
    static final int FIRE_LIMIT = 500;

    /**
     * The ceiling on reclaims per tick — a mass node death must not become a transaction with
     * unbounded locks; the surplus drains over the following ticks, oldest first
     * ({@code claimed_at}).
     */
    static final int RECLAIM_LIMIT = 500;

    /**
     * Reclaims per transaction: enough to drain a mass death in a handful of ticks, small enough that the
     * reaper's transaction deadline bounds a chunk rather than the whole sweep (see
     * {@link #reapOrphanedLeases}). An order of magnitude, not a measurement: fifty rows each paying a
     * rearm and a batch-counter round trip fit a 3 s deadline with room at any plausible latency; the
     * number that would tune it is the cost of one chunk under contention, which nobody has measured.
     */
    static final int RECLAIM_CHUNK = 50;

    /**
     * The heartbeat row's retention, in multiples of {@code node-lease-ttl} — derived on purpose, with
     * no new knob. Ten leases (2.5 minutes by default) keeps a dead node VISIBLE as stale for more than
     * enough time for {@code GET /nodes} and alerts.
     *
     * <p>It multiplies the NODE's lease, not the execution's: node liveness is what this retention is
     * about, and deriving it from the execution TTL let an operator who lowered that one delete the row
     * of a node whose promise was still alive (see {@link #purgeStaleNodeRows}).
     */
    static final int STALE_NODE_RETENTION_LEASES = 10;

    private static final String NODE_DEAD_ERROR = "node lease expired — node presumed dead";



    /**
     * How often the backlog is counted for {@code mohs.queue.depth}. Ten seconds sits under every
     * usual scrape interval (15–60s), so the gauge is never stale by more than one scrape, and far
     * above the tick's floor of 25ms, so the count is a rounding error next to the claim: at three
     * nodes it is 0.3 statements per second against the 4.0 an idle node already issues.
     */
    private static final Duration QUEUE_DEPTH_SAMPLE_INTERVAL = Duration.ofSeconds(10);

    /**
     * Idle cleanup cadence. A full batch resumes on a short continuation cadence until drained;
     * each pass shares the coordinator budget and has its own two-second ceiling.
     */
    private static final Duration IDEMPOTENCY_PRUNE_INTERVAL = Duration.ofHours(1);
    private static final Duration IDEMPOTENCY_PRUNE_CONTINUATION = Duration.ofSeconds(1);
    private static final Duration IDEMPOTENCY_PRUNE_BUDGET = Duration.ofSeconds(2);

    /** The history sweep's cadence — hourly for the same reason as the idempotency prune's: the window it enforces is measured in days. */
    private static final Duration HISTORY_PRUNE_INTERVAL = Duration.ofHours(1);

    /**
     * Rows per statement of one sweep pass. Bounded so a pass never escalates row locks into a table
     * lock on SQL Server (the escalation threshold sits near five thousand) and never holds any lock
     * long: history rows are cold, but the tables are the schema's largest.
     */
    private static final int HISTORY_PRUNE_BATCH = 1_000;

    /**
     * The monotonic ceiling on ONE tick's sweeping. The tick carries the heartbeat, so a backlog
     * (a window enforced for the first time over months of history) must drain across many hourly
     * slots rather than inside one long tick — after the budget, whatever remains waits.
     */
    private static final Duration HISTORY_PRUNE_BUDGET = Duration.ofSeconds(2);

    private final WorkQueue workQueue;
    private final Dispatcher dispatcher;
    private final HistoryStore historyStore;
    private final LeaseStore leaseStore;
    private final JobStore jobStore;
    private final NodeStore nodeStore;
    private final TriggerFirer triggerFirer;
    private final FiringPlanner firingPlanner;
    private final ExecutionWindowRegistry windowRegistry;
    private final RateLimitStore rateLimitStore;
    private final Clock clock;
    private final EngineSettings settings;
    private final NodeMembership membership;
    private final EngineMetrics metrics;
    private final RetryPolicyRegistry retryPolicies;

    /**
     * Sampled by the loop thread, read by whatever thread scrapes the registry — an {@code AtomicLong}
     * for the safe publication, not for atomicity: there is exactly one writer.
     */
    private final AtomicLong queueDepth = new AtomicLong();
    private final Cadence queueDepthSample = new Cadence(QUEUE_DEPTH_SAMPLE_INTERVAL);
    /** Expiration lives on each receipt; full batches resume shortly without blocking a heartbeat. */
    private final Cadence idempotencyPrune;
    private final @Nullable Cadence historyPrune;

    private final AtomicReference<EngineState> state = new AtomicReference<>(EngineState.CREATED);
    private final Set<CompletableFuture<Void>> inFlight = ConcurrentHashMap.newKeySet();
    /**
     * The executions in flight on THIS node — the basis of the watchdog and of the timeout and
     * cancellation ladder on every tick.
     *
     * <p>An entry is born at the submit and dies ONLY at the completion ({@code whenComplete}): the
     * watchdog's ownership release MARKS the incarnation ({@code ownershipReleased}) rather than
     * removing it — the zombie stays reachable by the drain escalation and by the cancel poll. One
     * exception: a re-claim by THIS node for the same id replaces the marked entry at the submit's
     * {@code put} (the map deliberately holds one incarnation per id).
     */
    private final ConcurrentHashMap<ExecutionId, InFlightAttempt> inFlightAttempts = new ConcurrentHashMap<>();
    /** Candidates of the reconciliation pass ({@link #reconcileOwnStrayLeases}) — touched only by the tick's thread (JCIP 3.3). */
    private LeaseStore.@Nullable LeaseCursor strayLeaseCursor;
    private java.util.@Nullable Iterator<Map.Entry<ExecutionId, InFlightAttempt>> cancelPollCursor;
    private java.util.@Nullable Iterator<Map.Entry<ExecutionId, InFlightAttempt>> watchdogCursor;
    /**
     * The claim lap's rotation cursor — it advances on every probed shard and PERSISTS between ticks,
     * so each lap's first shard rotates rather than always restarting at the same one: without it, a
     * tick that exhausts its budget on shard 0 would starve the last ones in the list. Touched only by
     * the tick's thread (JCIP 3.3); overflow is benign through {@code Math.floorMod}.
     */
    private int shardCursor;
    static final int CANDIDATE_JOBS_PER_PAGE = 128;
    private final JobKey[] candidateCursors = new JobKey[Shards.SHARD_COUNT];
    private int stageCursor;
    private static final int ROUND_STAGES = 10;
    /**
     * Did the last COMPLETE lap over the node's own shards come back empty? That is what arms the idle
     * gate (see {@link #claimAndDispatch}); "complete" is the distinction {@link LapOutcome} exists to
     * make — claiming zero because dispatch saturated or the budget ran out is not an empty queue.
     *
     * <p>Touched only by the tick's thread (JCIP 3.3). It starts {@code false}: the first tick of an
     * engine that has just come up has no right to assume the queue is empty.
     */
    private boolean queueLooksEmpty;

    /** The latch for the "I own no shard" WARN: the condition is persistent, the log must not be. Confined to the tick's thread. */
    private boolean warnedAboutOwningNoShard;
    private final RunnerRegistry runnerRegistry;

    /**
     * The loop's alarm clock: a {@code ReentrantLock} plus a {@code Condition} rather than
     * {@code synchronized}/{@code wait} (JCIP 13 — a timeout on the await is what the backoff needs),
     * and the {@code wakeRequested} flag absorbs a signal arriving BEFORE the await (a {@code signal}
     * with nobody waiting is lost; the flag is not — JCIP 14.2's condition-predicate pattern).
     */
    private final ReentrantLock wakeLock = new ReentrantLock();
    private final Condition wakeCondition = wakeLock.newCondition();
    /** Guarded by {@code wakeLock}. */
    private boolean wakeRequested;

    /** {@code volatile}: written by {@link #start} and read by {@link #stop}, which may come from different threads ({@code MohsLifecycle} is public API) — safe publication, JCIP 3.1. */
    private volatile @Nullable Thread loopThread;

    /** The grace-escalation WARN is one per shutdown — the SIGNAL is not guarded by it (see {@link #escalateAfterDrainGrace}). */
    private final AtomicBoolean drainEscalated = new AtomicBoolean();

    /**
     * Creates a {@code Engine} with the supplied values.
     *
     * @param workQueue the persistence port for ready work
     * @param dispatcher the component that submits claimed executions to handlers
     * @param historyStore the persistence port for execution history
     * @param leaseStore the persistence port for execution ownership
     * @param jobStore the persistence port for job definitions and triggers
     * @param nodeStore the persistence port for node heartbeats
     * @param triggerFirer the atomic trigger-advance and enqueue operation
     * @param windowRegistry the registry of firing exclusion windows
     * @param rateLimitStore the persistence port for shared rate-limit buckets
     * @param clock the time source used by the component
     * @param settings the engine timing and batching configuration
     * @param runnerRegistry the registry of local execution resources
     * @param metrics the engine metrics recorder
     */
    public Engine(
            WorkQueue workQueue,
            Dispatcher dispatcher,
            HistoryStore historyStore,
            LeaseStore leaseStore,
            JobStore jobStore,
            NodeStore nodeStore,
            TriggerFirer triggerFirer,
            ExecutionWindowRegistry windowRegistry,
            RateLimitStore rateLimitStore,
            Clock clock,
            EngineSettings settings,
            RunnerRegistry runnerRegistry,
            EngineMetrics metrics
    ) {
        this(workQueue, dispatcher, historyStore, leaseStore, jobStore, nodeStore, triggerFirer, windowRegistry,
                rateLimitStore, clock, settings, runnerRegistry, metrics, RetryPolicyRegistry.empty());
    }

    /**
     * The form that knows the declared {@link io.mohs.core.execution.RetryPolicy} beans — what the starter builds.
     *
     * @param workQueue the persistence port for ready work
     * @param dispatcher the component that submits claimed executions to handlers
     * @param historyStore the persistence port for execution history
     * @param leaseStore the persistence port for execution ownership
     * @param jobStore the persistence port for job definitions and triggers
     * @param nodeStore the persistence port for node heartbeats
     * @param triggerFirer the atomic trigger-advance and enqueue operation
     * @param windowRegistry the registry of firing exclusion windows
     * @param rateLimitStore the persistence port for shared rate-limit buckets
     * @param clock the time source used by the component
     * @param settings the engine timing and batching configuration
     * @param runnerRegistry the registry of local execution resources
     * @param metrics the engine metrics recorder
     * @param retryPolicies the named retry-policy registry
     */
    public Engine(
            WorkQueue workQueue,
            Dispatcher dispatcher,
            HistoryStore historyStore,
            LeaseStore leaseStore,
            JobStore jobStore,
            NodeStore nodeStore,
            TriggerFirer triggerFirer,
            ExecutionWindowRegistry windowRegistry,
            RateLimitStore rateLimitStore,
            Clock clock,
            EngineSettings settings,
            RunnerRegistry runnerRegistry,
            EngineMetrics metrics,
            RetryPolicyRegistry retryPolicies
    ) {
        this.retryPolicies = Objects.requireNonNull(retryPolicies, "retryPolicies");
        this.workQueue = Objects.requireNonNull(workQueue, "workQueue");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.historyStore = Objects.requireNonNull(historyStore, "historyStore");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore");
        this.triggerFirer = Objects.requireNonNull(triggerFirer, "triggerFirer");
        this.windowRegistry = Objects.requireNonNull(windowRegistry, "windowRegistry");
        this.rateLimitStore = Objects.requireNonNull(rateLimitStore, "rateLimitStore");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.firingPlanner = new FiringPlanner(new NextFireCalculator(), settings.misfireThreshold());
        this.runnerRegistry = Objects.requireNonNull(runnerRegistry, "runnerRegistry");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.membership = new NodeMembership(nodeStore, settings.nodeLeaseTtl(), dispatcher::catalog);
        metrics.bindNodeGauges(inFlightAttempts::size, settings.dispatchConcurrency());
        metrics.bindQueueDepthGauge(queueDepth::get);
        this.idempotencyPrune = new Cadence(IDEMPOTENCY_PRUNE_INTERVAL);
        this.historyPrune = settings.historyRetention().isPositive() ? new Cadence(HISTORY_PRUNE_INTERVAL) : null;
    }

    @Override
    public EngineState state() {
        return state.get();
    }

    /** A guarded CAS (the project's discipline throughout), not a lock: a transition only applies if the source state is still the expected one. */
    @Override
    public void start() {
        long startupBegan = System.nanoTime();
        EngineState initial = settings.startupDelay().isZero() ? EngineState.RUNNING : EngineState.STARTING;
        if (!state.compareAndSet(EngineState.CREATED, initial)) {
            throw new IllegalStateException("start() only valid from CREATED, was " + state.get());
        }
        warnIfPollIntervalOutrunsHeartbeat();
        // ONE platform thread, never a scheduler or a virtual one: latency-critical, immune to carrier
        // starvation, and it appears with a name of its own in any profiler or thread dump — which is
        // what matters at 3 a.m. Daemon: a leaked engine never holds up the JVM's exit (a crash is
        // already covered semantics — the node's lease expires and the reaper reclaims).
        Thread thread = Thread.ofPlatform().name("mohs-engine-loop").daemon(true).unstarted(() -> {
            if (awaitStartup(startupBegan)) {
                runLoop();
            }
        });
        loopThread = thread;
        thread.start();
        if (state.get() == EngineState.STOPPED) { // stop() won the race during start-up — wake the loop it never saw
            wake();
        }
    }

    /** Monotonic, one-time gate. Work signals and spurious wakes cannot shorten the delay. */
    private boolean awaitStartup(long startupBegan) {
        if (settings.startupDelay().isZero()) {
            return state.get() != EngineState.STOPPED;
        }
        log.info("engine waiting for startup delay {}", settings.startupDelay());
        long delayNanos = settings.startupDelay().toNanos();
        wakeLock.lock();
        try {
            while (state.get() == EngineState.STARTING) {
                long remaining = delayNanos - (System.nanoTime() - startupBegan);
                if (remaining <= 0) {
                    if (state.compareAndSet(EngineState.STARTING, EngineState.RUNNING)) {
                        log.info("engine startup delay completed");
                        return true;
                    }
                    return false;
                }
                try {
                    wakeCondition.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    // Same owner policy as awaitWork: stop is state plus wake, not interruption.
                }
            }
            return false;
        } finally {
            wakeLock.unlock();
        }
    }

    /**
     * The liveness cap in {@link #awaitWork} also swallows the FLOOR — the operator asked for ticks
     * spaced further apart than the liveness promise allows; liveness wins, but doing so in silence
     * would be a tuning mystery.
     */
    private void warnIfPollIntervalOutrunsHeartbeat() {
        Duration heartbeatCadence = heartbeatCadence();
        if (settings.pollInterval().compareTo(heartbeatCadence) > 0) {
            log.warn("effective tick cadence is capped at node-lease-ttl/3 ({}) — mohs.engine.poll-interval ({}) "
                    + "exceeds it; the heartbeat each tick carries is what keeps this node alive to its peers",
                    heartbeatCadence, settings.pollInterval());
        }
    }

    /** {@code nodeLeaseTtl/3}: the ceiling on the loop's sleep — see {@link #runLoop} for why liveness caps the backoff. */
    private Duration heartbeatCadence() {
        return settings.nodeLeaseTtl().dividedBy(3);
    }

    @Override
    public void pause() {
        if (!state.compareAndSet(EngineState.RUNNING, EngineState.PAUSED)) {
            throw new IllegalStateException("pause() only valid from RUNNING, was " + state.get());
        }
    }

    @Override
    public void resume() {
        if (!state.compareAndSet(EngineState.PAUSED, EngineState.RUNNING)) {
            throw new IllegalStateException("resume() only valid from PAUSED, was " + state.get());
        }
        // A long pause leaves the backoff at its ceiling — the operator who resumed should not have to
        // wait for the ceiling to expire before seeing the first claim
        wake();
    }

    /** It schedules no new claim or reclaim and waits for in-flight work up to {@code grace}. A drain is not a cancel. */
    @Override
    public void drain(Duration grace) {
        requireDrainGrace(grace);
        EngineState current = state.get();
        if (current != EngineState.RUNNING && current != EngineState.PAUSED) {
            throw new IllegalStateException("drain() only valid from RUNNING/PAUSED, was " + current);
        }
        if (!state.compareAndSet(current, EngineState.DRAINING)) {
            throw new IllegalStateException("concurrent state change during drain(), was " + state.get());
        }
        // The INFO exists because this wait is the MOST EXPENSIVE stretch of the shutdown and was the
        // only one with no trace: whoever read the log saw the web server's graceful shutdown (which
        // runs afterwards) and blamed it for the whole time. The "at least" is literal, not modesty:
        // with the loop still alive, a tick between the runAsync and the inFlight.add does not yet
        // appear in this count (the same window that forces the stop to wait twice)
        log.info("draining: at least {} dispatch(es) in flight, waiting up to {}", inFlight.size(), grace);
        awaitInFlight(DrainDeadline.startingNow(grace));
    }

    /**
     * {@link #drain} followed by ending the loop, in this order: drain, {@code STOPPED}, join the
     * loop, wait for in-flight work, final heartbeat. {@code runnerRegistry} stays alive — it is not
     * this class's to shut down (see the class Javadoc).
     *
     * <p>The join waits for the current tick up to {@code nodeLeaseTtl/4} (the tick's own lap budget) —
     * shrinking the window of the race documented in {@link #writeFinalStoppedHeartbeat} without
     * coupling the shutdown to a hung tick.
     *
     * <p>Waiting for in-flight work again after the join is not redundant with the drain: the drain's
     * wait observes {@code inFlight} with the loop still running, and a tick that has already claimed
     * a batch registers its futures AFTER the {@code runAsync} — finding the set empty there proves the
     * work has not been registered yet, not that it finished. With the loop already stopped no new
     * submit enters and empty means empty (a timed-out join shrinks that window rather than closing
     * it).
     *
     * <p>Without the second wait, the final heartbeat would declare the lease expired with executions
     * still running, and a peer's reaper would reclaim them: the successful result is discarded by the
     * fence and becomes a redelivery — or a terminal loss, when there is no retry budget.
     *
     * <p>Both deadlines of ONE {@code stop} are born at the same instant — this one opens its
     * {@link DrainDeadline} and {@link #drain} opens its own right after — so the shutdown stays
     * bounded by the requested {@code grace}: the one here is the older of the two, and it is what the
     * post-loop wait charges against. By contrast, {@code drain(g)} followed by {@code stop(g)} are two
     * SEPARATE deadlines in time — that sequence can cost up to {@code 2xg} when the drain escalates
     * and the handler ignores the interrupt.
     *
     * <p>Two things the post-loop wait does NOT cover, which decide the behaviour under the defaults:
     * <ul>
     *   <li><b>Nobody renews the node's lease during it</b> — {@code membership.heartbeat} only runs on the
     *       tick, which has stopped. The protection ends at most {@code node-lease-ttl} after the last
     *       tick — less in practice, because the remainder was already running: with a 15s TTL and a
     *       30s {@code grace}, more than half the wait is uncovered and a peer may reclaim what is
     *       still running here.</li>
     *   <li><b>The signalling ladder is suspended</b> — with no tick there is no
     *       {@code signalJobTimeouts}, no {@code pollCancelRequests} and no Watchdog Bound. A job with
     *       a {@code timeout} shorter than the {@code grace} stops being interrupted at its own
     *       deadline: during this window only the {@code grace} decides.</li>
     * </ul>
     */
    @Override
    public void stop(Duration grace) {
        requireDrainGrace(grace);
        if (state.compareAndSet(EngineState.STARTING, EngineState.STOPPED)) {
            wake();
            joinLoopThread();
            log.info("engine stopped during startup delay");
            return;
        }
        EngineState current = state.get();
        if (current == EngineState.STOPPED) {
            throw new IllegalStateException("already STOPPED");
        }
        long startNanos = System.nanoTime();
        DrainDeadline deadline = DrainDeadline.startingNow(grace);
        if (current != EngineState.DRAINING && current != EngineState.FAILED) {
            try {
                drain(grace);
            } catch (IllegalStateException concurrentChange) {
                // An unexpected loop death may race the transition into drain. Its work still needs stopping.
                if (state.get() != EngineState.FAILED) throw concurrentChange;
            }
        }
        state.set(EngineState.STOPPED);
        wake();
        joinLoopThread();
        awaitInFlight(deadline);
        // A mandatory order: durability BEFORE announcing death. The final heartbeat zeroes expires_at,
        // and from then on every peer considers us dead — a result still in the group commit's queue
        // would be reclaimed as an orphan.
        // The deadline is whatever is left of the grace, with a floor of 1s: zero would turn the join
        // into a no-op and lose the durability this step exists to guarantee
        dispatcher.drainCompletions(Duration.ofMillis(Math.max(1_000, deadline.remainingMillis())));
        writeFinalStoppedHeartbeat();
        // The counterpart of drain()'s INFO: it closes the interval the operator needs in order to
        // separate the engine's cost from the web server's during a shutdown
        log.info("engine stopped in {}", Duration.ofNanos(System.nanoTime() - startNanos));
    }

    /**
     * Waits for the current tick to finish, up to {@code nodeLeaseTtl/4} — the tick's own lap budget. A
     * complete join is what authorises {@link #stop} to read an empty {@code inFlight} as finished
     * work: with no loop, there is no new submit. A timed-out join hands control back anyway — a
     * shutdown must not hang on a stuck tick.
     */
    private void joinLoopThread() {
        Thread thread = loopThread;
        // NEW: start() publishes the thread BEFORE starting it, and stop() may have won the race (the
        // one start() itself handles). In that state join(Duration) throws
        // IllegalThreadStateException — and there is no tick in progress to wait for anyway: runLoop
        // tests STOPPED at the top of the while and never ticks.
        if (thread == null || thread.getState() == Thread.State.NEW) {
            return;
        }
        Duration budget = settings.nodeLeaseTtl().dividedBy(4);
        try {
            // join(Duration) rather than join(millis): it has no join(0)-waits-forever trap (which
            // forced a max(1, ...) with a TTL below 4ms) and it returns WHETHER the loop stopped — and
            // it is precisely the false case that marks the degraded mode, where the following wait can
            // again miss a dispatch that has not been registered yet
            if (!thread.join(budget)) {
                log.warn("engine loop did not stop within {} — the in-flight wait below can still miss a dispatch "
                        + "the tick in progress had not registered yet; work may be re-delivered", budget);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Best-effort irreversible stop of this session. A concurrent late renewal cannot revive a stopped identity. */
    private void writeFinalStoppedHeartbeat() {
        try {
            // A stopped node promises no liveness — expires_at = now (an already expired lease) tells
            // the peers "what was mine is free"
            membership.stop();
        } catch (RuntimeException e) {
            log.warn("could not write the final STOPPED heartbeat for node {} — the row will read as its last "
                    + "ticked state until the stale purge collects it", nodeId(), e);
        }
    }

    /** Delete a bounded page of receipts whose persisted expiry has passed according to database time. */
    private void pruneIdempotencyKeys(TickBudget budget) {
        long started = System.nanoTime();
        long pruned = 0;
        boolean backlog = true;
        try {
            do {
                int count = historyStore.pruneExpiredIdempotency();
                pruned += count;
                backlog = count >= HistoryStore.IDEMPOTENCY_PRUNE_BATCH;
            } while (backlog && !budget.spent()
                    && System.nanoTime() - started < IDEMPOTENCY_PRUNE_BUDGET.toNanos());
        } finally {
            if (backlog) idempotencyPrune.retryAfter(IDEMPOTENCY_PRUNE_CONTINUATION);
            metrics.idempotencyPruned(pruned, System.nanoTime() - started, backlog);
        }
        if (pruned > 0) log.info("pruned {} expired idempotency receipts", pruned);
    }

    /**
     * The history retention window ({@code mohs.engine.history-retention}), enforced in bounded
     * passes: batches of {@link #HISTORY_PRUNE_BATCH} per table, repeated only while a table keeps
     * coming back full and the monotonic {@link #HISTORY_PRUNE_BUDGET} lasts — the same
     * budget-on-top-of-a-counter shape as the claim rounds, and for the same reason: the tick
     * already spent its heartbeat, and sweeping must never outspend the promise. Whatever the budget
     * leaves stays for the next hourly slot; a months-deep backlog drains in hours, invisibly.
     *
     * <p>Every node issues the same sweep, exactly like {@link #pruneIdempotencyKeys} — isolated by
     * {@code runMaintenance}, and the loser of any row race merely deletes nothing.
     */
    private void pruneHistory(Instant now, TickBudget budget) {
        Duration retention = settings.historyRetention();
        Instant cutoff = now.minus(retention);
        long deadlineNanos = System.nanoTime() + HISTORY_PRUNE_BUDGET.toNanos();
        int executions = 0;
        int batches = 0;
        while (true) {
            HistoryStore.PrunedHistory pruned = historyStore.pruneHistoryBefore(cutoff, HISTORY_PRUNE_BATCH);
            executions += pruned.executions();
            batches += pruned.batches();
            if (pruned.drained(HISTORY_PRUNE_BATCH) || budget.spent() || System.nanoTime() - deadlineNanos >= 0) {
                break;
            }
        }
        if (executions > 0 || batches > 0) {
            log.info("pruned history older than {} — {} execution(s), {} member-less batch(es)",
                    retention, executions, batches);
        }
    }

    /**
     * Collects heartbeats older than {@code node-lease-ttl x }{@link #STALE_NODE_RETENTION_LEASES} —
     * riding along on the tick. This is not death detection (that is derived at READ time): it merely
     * collects rows no reader has any use for, because each boot generates a new {@code node_id} and
     * the table would grow forever.
     */
    private void purgeStaleNodeRows() {
        // The store also requires revocation and no outstanding ownership before deleting a session.
        Instant cutoff = membership.observedAt().minus(settings.nodeLeaseTtl().multipliedBy(STALE_NODE_RETENTION_LEASES));
        int purged = nodeStore.deleteHeartbeatsBefore(cutoff);
        if (purged > 0) {
            log.info("purged {} stale node heartbeat row(s) — instances dead or restarted more than {} lease TTLs ago",
                    purged, STALE_NODE_RETENTION_LEASES);
        }
    }

    /** Refuse invalid budgets before changing lifecycle state. */
    private static void requireDrainGrace(Duration grace) {
        Objects.requireNonNull(grace, "grace");
        if (grace.isNegative()) throw new IllegalArgumentException("grace must be nonnegative");
        try {
            grace.toNanos();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("grace is too large for a monotonic runtime budget", failure);
        }
    }

    /** One monotonic deadline shared by both waits within stop. */
    private record DrainDeadline(Duration requestedGrace, long deadlineNanos) {

        /**
         * {@code System.nanoTime}, not {@code clock.instant()}: a duration is measured with monotonic
         * time — the injected Clock may be the DatabaseClock, whose offset jumps on every resync and
         * would shorten or stretch the shutdown grace.
         */
        static DrainDeadline startingNow(Duration grace) {
            return new DrainDeadline(grace, System.nanoTime() + grace.toNanos());
        }

        long remainingMillis() {
            return (deadlineNanos - System.nanoTime()) / 1_000_000;
        }
    }

    /**
     * Waits for {@code inFlight} to empty in a loop, not on a single snapshot — a tick that had already
     * passed its claim at the exact instant of the CAS to {@code DRAINING} will still submit the
     * executions it just claimed, and the next turn of the loop catches what the previous snapshot
     * missed. It stops when {@code inFlight} genuinely empties or the deadline expires, whichever comes
     * first.
     *
     * <p>The loop only sees what has ALREADY been registered: with the set empty at the entry test it
     * returns immediately, even if a tick is exactly between the {@code runAsync} and the
     * {@code inFlight.add} in {@link #submitDispatch}. That window only closes with the loop stopped —
     * which is what {@link #stop} does, waiting again after the join.
     */
    private void awaitInFlight(DrainDeadline deadline) {
        while (!inFlight.isEmpty()) {
            long remainingMillis = deadline.remainingMillis();
            if (remainingMillis <= 0) {
                escalateAfterDrainGrace(deadline);
                return;
            }
            CompletableFuture<?>[] snapshot = inFlight.toArray(CompletableFuture[]::new);
            try {
                CompletableFuture.allOf(snapshot).get(remainingMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                escalateAfterDrainGrace(deadline);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                log.warn("unexpected exception waiting for in-flight dispatch during drain", e);
            }
        }
    }

    /**
     * An expired grace escalates through the cancellation machinery — flag plus interrupt on everything
     * left; the attempts fail asynchronously with a NodeShutdown cause and follow the normal retry
     * through the usual completion path.
     *
     * <p>There is no second configurable wait window (YAGNI); a handler deaf to the interrupt is
     * orphaned once the ticks end — the heartbeat stops, the node's lease expires and another node's
     * reaper reclaims. During the grace none of this happens: a drain is not a cancel.
     */
    private void escalateAfterDrainGrace(DrainDeadline deadline) {
        // The guard fences only the LOG, never the loop: the signal is first-wins per attempt
        // (CancellationSignal), but a dispatch registered AFTER the first escalation — exactly the
        // runAsync/inFlight.add window the second wait exists to cover — would never have been
        // signalled if the guard skipped the loop along with it
        if (drainEscalated.compareAndSet(false, true)) {
            // Both counts on purpose: inFlight (the futures) is what holds the grace; the map is what
            // the escalation reaches — with the ownership-released mark (a zombie stays in the map),
            // they diverge only during the whenComplete's completion window
            log.warn("drain grace period ({}) elapsed with {} dispatch(es) still in flight ({} still signallable) — "
                    + "signalling cancellation and interrupting them; their attempts will fail with a node-shutdown cause "
                    + "and follow the retry policy",
                    deadline.requestedGrace(), inFlight.size(), inFlightAttempts.size());
        } else {
            log.debug("drain grace already escalated — re-signalling {} in-flight attempt(s)", inFlightAttempts.size());
        }
        for (InFlightAttempt attempt : inFlightAttempts.values()) {
            attempt.signal.requestCancellation(CancellationSignal.Reason.SHUTDOWN, true);
        }
    }

    /**
     * The engine's loop: tick, then sleep — the interval starts at the floor ({@code pollInterval}),
     * doubles on every tick that found no work up to {@code maxPollInterval}, and returns to the floor
     * on the first that did.
     *
     * <p>The sleep is interruptible by {@link #wake} (a local enqueue; and stop/resume) and is ALWAYS
     * bounded by {@code nodeLeaseTtl/3}: the heartbeat runs once per tick, so the tick's cadence is the
     * liveness promise's cadence — a badly configured backoff ceiling must not let an idle node be
     * declared dead by its peers' reaper.
     *
     * <p>On top of that, the per-trigger cap ({@link #cappedByNextFire}): the backoff is blind to
     * deadlines, and a recurring job has a known one — {@code next_fire_at}. Both caps shorten the
     * SLEEP without touching the backoff's state.
     */
    private void runLoop() {
        Duration delay = settings.pollInterval();
        try {
            while (state.get() != EngineState.STOPPED) {
                TickOutcome outcome = tick();
                delay = outcome.workFound() ? settings.pollInterval() : nextBackoff(delay);
                awaitWork(cappedByNextFire(delay, outcome.earliestNextFireAt(), clock.instant(), settings.pollInterval()));
            }
        } catch (Throwable t) {
            state.updateAndGet(current -> current == EngineState.STOPPED ? current : EngineState.FAILED);
            // JCIP §7.3: every service thread declares what happens when it dies, and this one IS the
            // engine. tick() handles RuntimeException; an Error (OOME, a NoClassDefFoundError from a
            // listener loaded late, a StackOverflow in a recursive interceptor) used to escape here in
            // silence — the thread died, state stayed RUNNING forever, MohsLifecycle#state and
            // GET /nodes lied, and the only clue was the JVM's default handler
            log.error("engine loop died — this node stops claiming. Its node lease will expire and peers will"
                    + " recover outstanding grants through their retry budget. Restart the instance.", t);
            throw t;
        }
    }

    private Duration nextBackoff(Duration current) {
        Duration doubled = current.multipliedBy(2);
        return doubled.compareTo(settings.maxPollInterval()) > 0 ? settings.maxPollInterval() : doubled;
    }

    /**
     * Shortens the sleep down to the nearest armed trigger — the one deadline the engine KNOWS in
     * advance, and which the plain backoff ignored.
     *
     * <p>Without it the backoff wakes at {@code 25, 75, 175, 375, 775, 1575ms} and an occurrence due at
     * {@code +1000ms} waits until {@code 1575} (measured on the demo: a PT1S job firing in pairs of
     * ~0.4s and ~1.6s, never at 1.0s). It gets the same treatment as {@link #awaitWork}'s liveness cap:
     * it shortens the SLEEP, never the backoff's state — a tick brought forward by a trigger does not
     * count as a tick with work, and the progression stays intact.
     *
     * <p>{@code floor} is the {@code poll-interval}, and it is what stops the cap from REDEFINING the
     * tick's cadence: without it, what determines the loop's frequency stops being configuration and
     * becomes the schedule's density — N recurring jobs of interval T would wake the node roughly every
     * {@code T/N}, each trigger paying for a whole tick alone instead of coming out in the same batch as
     * its neighbours. With the floor, a trigger's maximum delay goes back to what the knob promises
     * (25ms by default) and {@code poll-interval} remains the sleep's floor.
     *
     * <p>The floor also subsumes the busy-spin guard: an already-due trigger sleeps the poll, never
     * {@code await(0)} in a loop against the database. That it only SHORTENS depends on
     * {@code floor <= delay}, an invariant of {@link EngineSettings}
     * ({@code max-poll-interval >= poll-interval}): without it, the floor would lengthen the sleep above
     * the backoff.
     *
     * <p>{@code now} comes from the injected {@link Clock} and the distance from {@code Duration.between},
     * not from {@code nanoTime}: the distance to a wall-clock INSTANT only exists on the wall-clock
     * scale — the wait itself stays monotonic ({@code awaitNanos}). It is the same calculation as
     * {@link RetrySchedule}, not a measurement of elapsed duration.
     */
    static Duration cappedByNextFire(Duration delay, @Nullable Instant earliestNextFireAt, Instant now, Duration floor) {
        if (earliestNextFireAt == null) {
            return delay;
        }
        Duration untilDue = Duration.between(now, earliestNextFireAt);
        if (untilDue.compareTo(delay) >= 0) {
            return delay;
        }
        return untilDue.compareTo(floor) > 0 ? untilDue : floor;
    }

    private void awaitWork(Duration delay) {
        Duration heartbeatCadence = heartbeatCadence();
        Duration bounded = delay.compareTo(heartbeatCadence) > 0 ? heartbeatCadence : delay;
        Duration untilCleanup = idempotencyPrune.untilDue();
        if (untilCleanup.compareTo(settings.pollInterval()) < 0) untilCleanup = settings.pollInterval();
        if (untilCleanup.compareTo(bounded) < 0) bounded = untilCleanup;
        wakeLock.lock();
        try {
            if (!wakeRequested) {
                // A single await, not a loop: waking without a signal (spurious or timeout) only brings
                // a tick forward — the real predicate is the queue in the database
                wakeCondition.awaitNanos(bounded.toNanos());
            }
            wakeRequested = false;
        } catch (InterruptedException e) {
            // The Engine OWNS this thread (JCIP 7.1.3): the stop protocol is state plus wake — an
            // interrupt means nothing here. Re-arming the flag would become a busy-spin (every
            // subsequent awaitNanos would throw on entry). Swallowing IS the owner's interruption
            // policy, not an empty catch.
        } finally {
            wakeLock.unlock();
        }
    }

    /** Wakes the loop now — from stop/resume and from the local wake-up tier. Safe from any thread. */
    void wake() {
        wakeLock.lock();
        try {
            wakeRequested = true;
            wakeCondition.signal();
        } finally {
            wakeLock.unlock();
        }
    }

    /**
     * The local wake-up tier: an enqueue from THIS JVM that is already due wakes the loop without
     * waiting for the poll. The enqueue unit calls this once its writes are durable — after the
     * host's commit when it joined one, right after its own otherwise — so a wake never precedes the
     * row it announces (a lap that still does not see it). Best-effort by contract: a lost signal is
     * covered by the poll tier, never by correctness here.
     */
    public void signalWorkScheduled() {
        wake();
    }

    /**
     * It always publishes a heartbeat reflecting the current state — even outside {@code RUNNING}
     * (useful for {@code GET /nodes} to show "paused" or "draining"). Claim and reclaim only run in
     * {@code RUNNING}.
     *
     * @return what the loop needs to know in order to decide the next sleep ({@link TickOutcome})
     */
    private TickOutcome tick() {
        TickBudget budget = TickBudget.start(settings.nodeLeaseTtl());
        try {
            membership.heartbeat(state.get());
            runMaintenance("signal-job-timeouts", this::signalJobTimeouts);
            Round round = new Round(budget, clock.instant());
            for (int visited = 0; visited < ROUND_STAGES && !budget.spent(); visited++) {
                int stage = stageCursor;
                stageCursor = (stageCursor + 1) % ROUND_STAGES;
                runMaintenance(stageName(stage), () -> runStage(stage, round));
            }
            return new TickOutcome(round.workFound, round.earliestFire);
        } catch (RuntimeException e) {
            metrics.tickStepFailed("tick");
            log.error("engine tick failed — will retry next tick", e);
            return TickOutcome.idle();
        }
    }

    /** One loop-thread snapshot. Reads happen only when a stage actually needs them. */
    private final class Round {
        final TickBudget budget;
        final Instant now;
        private @Nullable Map<JobKey, StoredJob> definitions;
        private @Nullable List<StoredNode> nodes;
        boolean workFound;
        @Nullable Instant earliestFire;

        Round(TickBudget budget, Instant now) {
            this.budget = budget;
            this.now = now;
        }

        Map<JobKey, StoredJob> definitions() {
            if (definitions == null) definitions = new HashMap<>();
            return definitions;
        }

        List<StoredNode> nodes() {
            if (nodes == null) nodes = nodeStore.findAll();
            return nodes;
        }
    }

    private static String stageName(int stage) {
        return switch (stage) {
            case 0 -> "reap-orphaned-leases";
            case 1 -> "fire-due-triggers";
            case 2 -> "claim-and-dispatch";
            case 3 -> "poll-cancel-requests";
            case 4 -> "reconcile-stray-leases";
            case 5 -> "stale-node-purge";
            case 6 -> "queue-depth-sample";
            case 7 -> "idempotency-prune";
            case 8 -> "history-prune";
            case 9 -> "signal-watchdog-overruns";
            default -> throw new IllegalArgumentException("unknown round stage " + stage);
        };
    }

    /** Advance the stage cursor before I/O: an exhausted stage cannot monopolize later rounds. */
    private void runStage(int stage, Round round) {
        boolean running = state.get() == EngineState.RUNNING;
        switch (stage) {
            case 0 -> {
                if (running) reapOrphanedLeases(round.definitions(), round.nodes(), round.now, round.budget);
            }
            case 1 -> {
                if (running) {
                    FiringOutcome firing = fireDueTriggers(round.budget);
                    round.workFound |= firing.fired();
                    round.earliestFire = firing.rearmedAt();
                    if (!round.budget.spent()) round.earliestFire = jobStore.nextFireAfter(round.now).orElse(null);
                }
            }
            case 2 -> {
                if (running) round.workFound |= claimAndDispatch(round.definitions(),
                        Shards.ownedBy(nodeId(), shardEligibleNodeIds(round.nodes(), membership.observedAt())), round.budget) > 0;
            }
            case 3 -> pollCancelRequests();
            case 4 -> { if (running) reconcileOwnStrayLeases(round.now, round.budget); }
            case 5 -> purgeStaleNodeRows();
            case 6 -> { if (queueDepthSample.due()) queueDepth.set(workQueue.countVisible(round.now)); }
            case 7 -> { if (idempotencyPrune.due()) pruneIdempotencyKeys(round.budget); }
            case 8 -> { if (historyPrune != null && historyPrune.due()) pruneHistory(round.now, round.budget); }
            case 9 -> signalWatchdogOverruns(round.budget);
            default -> throw new IllegalArgumentException("unknown round stage " + stage);
        }
    }

    /**
     * Runs one maintenance step without letting its failure reach the claim. The granularity of error
     * handling is the granularity of degradation: a {@code try} covering independent steps turns any one
     * of them into a single point of failure for the whole set.
     */
    private void runMaintenance(String step, Runnable maintenance) {
        try {
            maintenance.run();
        } catch (RuntimeException e) {
            metrics.tickStepFailed(step);
            log.warn("tick step '{}' failed — later stages remain eligible in this or a subsequent round", step, e);
        }
    }

    /**
     * Lets a tick that runs every 25ms host work whose natural cadence is seconds or hours.
     *
     * <p>Monotonic ({@code System.nanoTime}), never the injected {@link Clock}: this is the distance
     * between two events in this process, and {@code DatabaseClock}'s offset jumps at every resync —
     * a backwards jump would suppress the step for as long as the jump lasted. Confined to the loop
     * thread, which is the only caller, so it needs no synchronization.
     */
    private static final class Cadence {

        private final long intervalNanos;
        private long nextDueNanos = System.nanoTime();

        Cadence(Duration interval) {
            this.intervalNanos = interval.toNanos();
        }

        /** True at most once per interval — and always on the first call, so the first tick samples. */
        boolean due() {
            long now = System.nanoTime();
            // Subtraction, not <: the difference is correct across nanoTime's rollover, the comparison is not
            if (now - nextDueNanos < 0) {
                return false;
            }
            nextDueNanos = now + intervalNanos;
            return true;
        }

        void retryAfter(Duration delay) {
            nextDueNanos = System.nanoTime() + delay.toNanos();
        }

        Duration untilDue() {
            return Duration.ofNanos(Math.max(0, nextDueNanos - System.nanoTime()));
        }
    }

    /**
     * The tick's verdict for the loop: whether it found work in the database (a claim or a trigger) —
     * which resets the backoff; signals and maintenance do not count — and the nearest armed trigger,
     * the horizon that shortens the sleep. {@code null} means no armed trigger ahead: only the backoff
     * governs.
     */
    private record TickOutcome(boolean workFound, @Nullable Instant earliestNextFireAt) {

        /** A tick with no work and no horizon — outside RUNNING, or a tick that failed before determining either. */
        static TickOutcome idle() {
            return new TickOutcome(false, null);
        }
    }



    /**
     * The nearest of the horizon determined so far and one more candidate — the step
     * {@link JobStore#nextFireAfter} and {@link #fireDueTriggers} repeat over different sources.
     *
     * <p>Only a candidate STILL IN THE FUTURE counts: a disarmed trigger ({@code null}) has no deadline
     * to honour, and a due one we did not fire is already late — it drains on the next tick, without
     * shortening any sleep.
     */
    private static @Nullable Instant earlierFutureFire(@Nullable Instant earliest, @Nullable Instant candidate, Instant now) {
        if (candidate == null || !candidate.isAfter(now)) {
            return earliest;
        }
        return earliest == null || candidate.isBefore(earliest) ? candidate : earliest;
    }





    /** Revoke expired sessions first, then reclaim bounded grant chunks under the shared round budget. */
    private void reapOrphanedLeases(Map<JobKey, StoredJob> definitions, List<StoredNode> nodes, Instant now, TickBudget budget) {
        for (StoredNode node : nodes) {
            if (budget.spent()) return;
            if (!node.revoked() && !node.validAt(membership.observedAt())) {
                nodeStore.revokeExpired(node.nodeId());
            }
        }
        if (budget.spent()) return;
        List<LeaseStore.Lease> orphans = leaseStore.findOrphaned(RECLAIM_LIMIT);
        if (orphans.isEmpty()) {
            return;
        }
        jobStore.findByKeys(orphans.stream().map(LeaseStore.Lease::jobKey).distinct().toList())
                .forEach(stored -> definitions.put(stored.definition().key(), stored));
        if (budget.spent()) return;
        List<ReclaimDecision> decisions = orphans.stream()
                .map(orphan -> decideReclaim(orphan, definitions.get(orphan.jobKey()), now))
                .toList();
        // One transaction per chunk, not per sweep: the reclaim runs under the tick's deadline, and a
        // mass death is up to RECLAIM_LIMIT rows each paying a rearm and a batch counter round trip —
        // a sweep that could not finish inside the deadline would roll back whole, find the same
        // orphans next tick, and never reclaim anything. A chunk that fails loses only itself: the
        // ones after it still run, and what it lost is found again next tick. The loop itself has the
        // claim laps' budget — ten chunks each waiting out a 3 s deadline would outlast the lease

        for (int from = 0; from < decisions.size(); from += RECLAIM_CHUNK) {
            if (budget.spent()) {
                log.warn("reclaim sweep stopped after {} of {} orphaned lease(s) — the tick's budget is spent, "
                        + "the rest are reclaimed next tick", from, decisions.size());
                break;
            }
            List<ReclaimDecision> chunk = decisions.subList(from, Math.min(from + RECLAIM_CHUNK, decisions.size()));
            try {
                reclaimChunk(chunk);
            } catch (RuntimeException e) {
                metrics.tickStepFailed("reap-orphaned-leases");
                log.warn("reclaim of {} orphaned lease(s) failed — the remaining chunks continue, this one is retried next tick",
                        chunk.size(), e);
            }
        }
    }

    /** One reclaim transaction; only the decisions whose fence won are counted and published. */
    private void reclaimChunk(List<ReclaimDecision> chunk) {
        Map<ExecutionId, LeaseStore.Completion> verdicts =
                leaseStore.reclaim(chunk.stream().map(ReclaimDecision::result).toList());
        for (ReclaimDecision decision : chunk) {
            LeaseStore.Completion verdict = verdicts.get(decision.result().executionId());
            if (verdict != null && verdict.owned()) {
                var applied = Objects.requireNonNull(verdict.result());
                var durable = applied.outcome() == ExecutionState.CANCELLED
                        ? new ReclaimDecision(applied, decision.metricKey(), ExecutionState.CANCELLED, false, null)
                        : applied.terminalState() == ExecutionState.FAILED && decision.result().terminalState() == null
                                ? new ReclaimDecision(applied, decision.metricKey(), ExecutionState.FAILED, false, null) : decision;
                metrics.leaseReclaimed(durable.reclaimedState(), durable.attemptsExhausted());
                dispatcher.observeCompletion(decision.metricKey(), decision.result(), verdict, () -> publishReclaimOutcome(durable));
            }
        }
    }

    /** One page of this session's grants. After grace, local incarnation and in-transit completion checks
     * identify strays; fenced requeue arbitrates any completion race. No second-observation cache. */
    private void reconcileOwnStrayLeases(Instant now, TickBudget budget) {
        List<LeaseStore.Lease> page = leaseStore.findByNodePage(nodeId(), strayLeaseCursor, CANDIDATE_JOBS_PER_PAGE);
        strayLeaseCursor = page.size() == CANDIDATE_JOBS_PER_PAGE
                ? new LeaseStore.LeaseCursor(page.getLast().claimedAt(), page.getLast().executionId()) : null;
        Instant strayBefore = now.minus(strayLeaseGrace());
        List<WorkQueue.Requeue> strays = new ArrayList<>();
        for (LeaseStore.Lease lease : page) {
            InFlightAttempt attempt = inFlightAttempts.get(lease.executionId());
            if ((attempt != null && attempt.grant.claimToken().equals(lease.claimToken()))
                    || dispatcher.completionInTransit(lease.executionId(), lease.claimToken())
                    || lease.claimedAt().isAfter(strayBefore)) continue;
            strays.add(new WorkQueue.Requeue(lease.executionId(), nodeId(), lease.claimToken(), lease.attemptNumber(), now));
        }
        if (!strays.isEmpty() && !budget.spent()) {
            int requeued = workQueue.requeue(strays);
            if (requeued > 0) {
                metrics.claimRequeued("stray-lease", requeued);
                log.warn("requeued {} grants without a local incarnation or pending completion after the grace period", requeued);
            }
        }
    }

    /** Minimum age before local stray recovery: max(two seconds, four poll intervals). */
    private Duration strayLeaseGrace() {
        Duration fourPolls = settings.pollInterval().multipliedBy(4);
        Duration floor = Duration.ofSeconds(2);
        return fourPolls.compareTo(floor) > 0 ? fourPolls : floor;
    }

    private boolean isAlive(StoredNode node, Instant now) {
        return node.validAt(now);
    }

    /**
     * Who takes part in the shard assignment: alive AND {@code RUNNING} —
     * {@code PAUSED}/{@code DRAINING}/{@code STOPPED} do not claim, so giving them shards would leave
     * 1/n of the queue stalled while their peers have headroom.
     *
     * <p>This node always enters by the time it reaches the claim: its own tick's heartbeat has just
     * written RUNNING. If the list comes out empty, or without this node, {@link Shards#ownedBy}
     * degrades to "owns them all" — an overlap is the pre-shard behaviour, whereas a stalled queue is
     * not an option.
     */
    private List<String> shardEligibleNodeIds(List<StoredNode> nodes, Instant now) {
        return nodeIdsMatching(nodes, node -> node.state() == EngineState.RUNNING && isAlive(node, now));
    }

    /** The ids that pass the predicate, ALWAYS including this node — the invariant shared by the reaper and the shard assignment (see the callers). */
    private List<String> nodeIdsMatching(List<StoredNode> nodes, Predicate<StoredNode> included) {
        List<String> ids = new ArrayList<>();
        for (StoredNode node : nodes) {
            if (included.test(node)) {
                ids.add(node.nodeId());
            }
        }
        if (!ids.contains(nodeId())) {
            ids.add(nodeId());
        }
        return ids;
    }

    /** One decided reclaim outcome — the fenced result plus what to publish and measure if the fence wins. */
    private record ReclaimDecision(LeaseStore.CompletionResult result, JobKey metricKey, ExecutionState reclaimedState,
            boolean attemptsExhausted, @Nullable Instant retryAt) {
    }

    private ReclaimDecision decideReclaim(LeaseStore.Lease orphan, @Nullable StoredJob storedJob, Instant now) {
        ExecutionId id = orphan.executionId();
        JobKey jobKey = orphan.jobKey();
        boolean retired = storedJob == null;
        JobDefinition definition = storedJob == null ? null : storedJob.definition();

        if (orphan.cancelRequested()) {
            // The node died, but the operator's order was already given — rescheduling would disobey it;
            // a CANCELLED attempt with a null error
            return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.claimToken(),
                    orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.CANCELLED, null, null, null),
                    Dispatcher.metricKey(definition), ExecutionState.CANCELLED, false, null);
        }
        Optional<Instant> nextRetry = retired
                ? Optional.empty() // a retired job never reschedules: the entry would be stuck forever (the queue does not filter retired — retirement drains the queue)
                // No Throwable: the failure happened in a JVM that is no longer running — a custom
                // policy sees null and decides on the attempt number alone
                : retryPolicies.nextRetryAt(Objects.requireNonNull(definition), orphan.attemptNumber(), null, now);
        if (nextRetry.isPresent()) {
            Instant retryAt = nextRetry.orElseThrow();
            return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.claimToken(),
                    orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.FAILED,
                    IllegalStateException.class.getName(), NODE_DEAD_ERROR, retryAt),
                    Dispatcher.metricKey(definition), ExecutionState.RETRY_WAITING, false, retryAt);
        }
        return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.claimToken(),
                orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.FAILED,
                IllegalStateException.class.getName(), NODE_DEAD_ERROR, null),
                Dispatcher.metricKey(definition), ExecutionState.FAILED, !retired, null);
    }

    /**
     * Reclaim outcomes publish the same events as the dispatch path, mirroring the {@link Dispatcher}'s
     * pairs exactly: a retry gives {@code AttemptFailed} plus {@code RetryScheduled}; an honoured
     * pending cancel gives {@code Cancelled}; a terminal one gives {@code Failed} alone. It is the
     * node-death alerting hook {@link Failed}'s Javadoc announces.
     */
    private void publishReclaimOutcome(ReclaimDecision decision) {
        LeaseStore.CompletionResult result = decision.result();
        ExecutionEventPublisher events = dispatcher.events();
        int attemptNumber = result.attemptNumber();
        switch (decision.reclaimedState()) {
            case CANCELLED -> events.publish(new Cancelled(result.executionId(), result.jobKey(), attemptNumber));
            case RETRY_WAITING -> {
                Exception error = new IllegalStateException(NODE_DEAD_ERROR);
                events.publish(new AttemptFailed(result.executionId(), result.jobKey(), attemptNumber, error));
                events.publish(new RetryScheduled(result.executionId(), result.jobKey(), attemptNumber + 1,
                        Objects.requireNonNull(decision.retryAt())));
            }
            default -> events.publish(new Failed(result.executionId(), result.jobKey(), attemptNumber,
                    new IllegalStateException(NODE_DEAD_ERROR), decision.attemptsExhausted()));
        }
    }

    /**
     * The tick's "acquisition to dispatch" step, with the idle gate in front: while the previous round
     * came back empty, an existence probe answers for the whole lap — 1 statement in place of
     * {@link Shards#SHARD_COUNT}, which was measured at 96% of an idle node's query cost. When the
     * probe finds work, the lap runs on THIS same tick: a saving, never added latency.
     */
    private int claimAndDispatch(Map<JobKey, StoredJob> definitions, List<Integer> ownedShards, TickBudget budget) {
        if (ownedShards.isEmpty()) {
            warnOnceAboutOwningNoShard();
            return 0;
        }
        warnedAboutOwningNoShard = false;
        if (queueLooksEmpty && !probeSaysThereIsWork(ownedShards)) {
            return 0;
        }
        LapOutcome outcome = claimLaps(definitions, ownedShards, budget);
        // Saturated dispatch and an exhausted budget also return zero WITHOUT the queue being empty:
        // arming on those would make the gate pay for a probe per tick on the hot path, the opposite of
        // what it exists to do
        queueLooksEmpty = outcome.claimed() == 0 && outcome.sweptEveryOwnedShard();
        return outcome.claimed();
    }

    /**
     * Owning zero shards is NOT an empty queue — it is this node being outside the partition.
     * {@link Shards#ownedBy} returns an empty list for an index {@code >= SHARD_COUNT}, and the inner
     * lap then does not run: {@link LapOutcome} said "I swept everything and found nothing", armed
     * {@code queueLooksEmpty}, and the node sat idle FOREVER announcing RUNNING and occupying a shard
     * slice of its peers', with not one line of log.
     *
     * <p>Once per transition, not per tick: the condition is PERSISTENT (it only changes when the
     * cluster shrinks or SHARD_COUNT rises), and at the 25ms floor it would be ~40 lines/s per node —
     * the log that diagnoses the problem burying everything else.
     */
    private void warnOnceAboutOwningNoShard() {
        if (warnedAboutOwningNoShard) {
            return;
        }
        warnedAboutOwningNoShard = true;
        log.warn("this node owns no shard of {} — it will never claim. The cluster has more RUNNING nodes"
                + " than shards; reduce the node count or raise Shards.SHARD_COUNT.", Shards.SHARD_COUNT);
    }

    /**
     * The gate's probe, made proof against itself: it is a saving, never an authority — a probe that
     * fails returns the tick to the lap. Without this, a PERSISTENT failure of it would leave this node
     * alive, heartbeating and owning 1/n of the shards while never claiming anything — and nobody would
     * reap it, because it is not dead.
     */
    private boolean probeSaysThereIsWork(List<Integer> ownedShards) {
        try {
            return workQueue.hasVisibleWork(ownedShards, clock.instant());
        } catch (RuntimeException e) {
            log.warn("idle-gate probe failed — falling back to the full claim lap this tick", e);
            return true;
        }
    }

    /**
     * One claim round's result: how much came back, and whether the pass over the node's own shards was
     * COMPLETE. Only a complete, empty pass is proof of an empty queue — the early exits (dispatch
     * headroom exhausted, time budget, leaving {@code RUNNING}) return zero for other reasons.
     *
     * <p>Exiting through exhausted {@code claimRounds} returns {@code false} out of conservatism, and it
     * is unobservable: reaching there requires every lap to have filled its budget, which guarantees
     * {@code claimed > 0}. Do not "tidy" it to {@code true} — the value changes nothing and the "only
     * arm with proof" rule stops being legible.
     */
    private record LapOutcome(int claimed, boolean sweptEveryOwnedShard) {
    }

    /** Probe bounded job-key pages in owned shards. Cursors advance even through rejected pages;
     * global capacity remains inside the claim transaction. All rounds share the coordinator deadline. */
    private LapOutcome claimLaps(Map<JobKey, StoredJob> definitions, List<Integer> ownedShards, TickBudget budget) {

        int totalClaimed = 0;
        for (int lap = 0; lap < settings.claimRounds(); lap++) {
            if (mustStopClaiming(budget)) {
                return new LapOutcome(totalClaimed, false);
            }
            int lapBudget = Math.min(settings.batchSize(), settings.dispatchConcurrency() - inFlight.size());
            if (lapBudget <= 0) {
                return new LapOutcome(totalClaimed, false);
            }
            Instant now = clock.instant();
            // Policies are loaded per candidate page; authoritative caps are checked inside claim.

            // The LAP: one OWN shard per statement, round-robin with a cursor that persists between
            // ticks — a multi-shard predicate would kill the index's ordering (a measured lesson); a complete
            // empty pass ends it
            int remaining = lapBudget;
            for (int probe = 0; probe < ownedShards.size() && remaining > 0; probe++) {
                // The same lap-boundary guard, PER PROBE: one lap is up to 64 statements, and a time
                // budget only protects at the granularity it is checked — a degraded database at
                // 300ms/claim would blow the NODE's lease mid-tick; the tick's first probe always runs
                if (mustStopClaiming(budget)) {
                    return new LapOutcome(totalClaimed, false);
                }
                int shard = ownedShards.get(Math.floorMod(shardCursor++, ownedShards.size()));
                List<JobKey> page = workQueue.findCandidateJobs(shard, candidateCursors[shard], CANDIDATE_JOBS_PER_PAGE, now);
                candidateCursors[shard] = page.size() == CANDIDATE_JOBS_PER_PAGE ? page.getLast() : null;
                if (page.isEmpty()) continue;
                if (mustStopClaiming(budget)) return new LapOutcome(totalClaimed, false);
                Map<JobKey, StoredJob> pageDefinitions = new HashMap<>();
                jobStore.findByKeys(page).forEach(stored -> pageDefinitions.put(stored.definition().key(), stored));
                definitions.putAll(pageDefinitions);
                Set<JobKey> excluded = WorkAdmission.excluded(pageDefinitions, windowRegistry, rateLimitStore, now);
                List<JobKey> eligible = pageDefinitions.keySet().stream().filter(key -> !excluded.contains(key)).toList();
                if (eligible.isEmpty()) continue;
                if (mustStopClaiming(budget)) return new LapOutcome(totalClaimed, false);
                long claimStartNanos = System.nanoTime();
                List<WorkQueue.ClaimedWork> claimed =
                        workQueue.claim(shard, nodeId(), remaining, eligible, now);
                metrics.claimRound(System.nanoTime() - claimStartNanos, claimed.size());
                if (claimed.isEmpty()) {
                    continue;
                }
                // What was claimed counts against the budget EVEN when admit returns losers: deliberate —
                // simplicity over utilisation (admission churn is rare by construction and self-limited
                // to one lap)
                remaining -= claimed.size();
                totalClaimed += claimed.size();
                List<WorkQueue.ClaimedWork> admitted = admit(claimed, definitions, now);
                if (!admitted.isEmpty() && !dispatchClaimedBatch(admitted, definitions, now)) {
                    return new LapOutcome(totalClaimed, false);
                }
            }
            if (remaining > 0) {
                // The lap did not fill the budget — the queue has drained enough; one more lap would be a
                // pass of SELECTs over already empty queues. Reaching here requires the inner loop to
                // have exhausted the node's own shards: the pass was COMPLETE, and zero here IS an empty
                // queue
                return new LapOutcome(totalClaimed, true);
            }
        }
        return new LapOutcome(totalClaimed, false);
    }



    /**
     * The guard between claims of the same tick: a drain or pause breaks the chain, and so does the
     * tick's budget — the reason for each checkpoint (the lap boundary and each probe) is at the call
     * sites.
     */
    private boolean mustStopClaiming(TickBudget budget) {
        return state.get() != EngineState.RUNNING || budget.spent();
    }

    /**
     * One monotonic node-TTL/4 deadline for the entire round, including heartbeat and maintenance.
     * Checkpoints stop new operations; a driver or pool wait already in progress can exceed it.
     * The persistent stage cursor gives skipped stages the first opportunity in later rounds.
     */
    private record TickBudget(long startNanos, long budgetNanos) {

        static TickBudget start(Duration nodeLeaseTtl) {
            return new TickBudget(System.nanoTime(), nodeLeaseTtl.toNanos() / 4);
        }

        boolean spent() {
            return System.nanoTime() - startNanos >= budgetNanos;
        }
    }

    /** Recheck windows, charge each shared rate group, and fence rejected items back to READY at the same attempt. */
    private List<WorkQueue.ClaimedWork> admit(List<WorkQueue.ClaimedWork> claimed, Map<JobKey, StoredJob> definitions,
            Instant now) {
        var decision = WorkAdmission.afterClaim(claimed, definitions, windowRegistry, rateLimitStore, now);
        Instant visibleAt = now;
        if (!decision.rejected().isEmpty()) {
            workQueue.requeue(decision.rejected().stream().map(rejected -> {
                var work = rejected.work();
                return new WorkQueue.Requeue(work.executionId(), nodeId(), work.claimToken(), work.attemptNumber(), visibleAt);
            }).toList());
            decision.rejected().stream().collect(java.util.stream.Collectors.groupingBy(WorkAdmission.Rejected::reason,
                    java.util.stream.Collectors.counting())).forEach((reason, count) -> metrics.claimRequeued(reason, count.intValue()));
        }
        return decision.admitted();
    }

    /**
     * ONE payload-plus-header query per round, not one per execution. A failure of the query is
     * infrastructure, never a verdict on the executions: the batch keeps its lease up and another node's
     * reaper returns it if this one dies — a hiccup never becomes an immediate TERMINAL failure.
     *
     * @return {@code false} if the query failed — the caller ends the tick's rounds (the already claimed
     *         batch is left to the reaper)
     */
    private boolean dispatchClaimedBatch(List<WorkQueue.ClaimedWork> claimed, Map<JobKey, StoredJob> definitions, Instant claimInstant) {
        HistoryStore.PayloadBatch payloads;
        try {
            payloads = historyStore.findPayloads(claimed.stream().map(WorkQueue.ClaimedWork::executionId).toList());
        } catch (RuntimeException e) {
            log.warn("could not load the payloads of the claimed batch ({} execution(s)) — already leased, "
                    + "they will stand until a reaper reclaims them if this node dies", claimed.size(), e);
            return false;
        }
        for (WorkQueue.ClaimedWork work : claimed) {
            try {
                submitDispatch(work, payloads, definitions, claimInstant);
            } catch (IllegalArgumentException e) {
                // A row this node cannot even turn into an Execution (a priority outside the enum, an
                // actor the record refuses — what a value object rejects, and only that) is terminally
                // broken, like an unreadable payload, and must fail alone. Without this catch it unwound
                // the whole batch and the tick: every execution already claimed stayed leased with
                // nothing dispatching it, and the same row was re-claimed and re-thrown on every tick.
                // Anything wider stays uncaught on purpose: a DataAccessException from the fresh
                // definition read is infrastructure, and a verdict written on it would turn a stale
                // pooled connection into a terminal FAILED with no retry — the lease stands for the
                // reconcile instead. Its own message: the operator reading Attempt.error must see the
                // column that broke, not a payload that was never read
                failBeforeDispatchGuarded(executionFor(work, null, claimInstant), null, new IllegalStateException(
                        "claimed row could not become an execution: " + Objects.requireNonNullElse(e.getMessage(), e.toString()), e),
                        new Dispatcher.Grant(nodeId(), work.claimToken(), work.attemptNumber(), claimInstant));
            }
        }
        return true;
    }

    /**
     * Fire-and-forget with respect to the next tick: a slow handler does not block the next claim round.
     * The runner's executor may reject ({@link MohsExecutors#ioBoundExecutor} rejects above the
     * concurrency ceiling, on purpose — real backpressure) — without catching here, a single rejection
     * mid-batch would abort the {@code for} and leave the following, already owned executions orphaned
     * until a reaper.
     */
    private void submitDispatch(WorkQueue.ClaimedWork work, HistoryStore.PayloadBatch payloads, Map<JobKey, StoredJob> definitions, Instant claimInstant) {
        ExecutionId id = work.executionId();
        HistoryStore.PayloadRow row = payloads.rows().get(id);
        Dispatcher.Grant grant = new Dispatcher.Grant(nodeId(), work.claimToken(), work.attemptNumber(), claimInstant);
        // The acquired page carries its policy snapshot; retirement does not erase it.
        StoredJob storedJob = definitions.get(work.jobKey());
        if (storedJob == null) {
            failBeforeDispatchGuarded(executionFor(work, row, claimInstant), null, new IllegalStateException("job definition for " + work.jobKey() + " was removed after this execution was claimed (e.g. Mohs.remove between claim and dispatch)"), grant);
            return;
        }
        JobDefinition definition = storedJob.definition();

        AsyncTaskExecutor executor;
        try {
            executor = runnerRegistry.resolve(definition.runner());
        } catch (NoSuchElementException e) {
            failBeforeDispatchGuarded(executionFor(work, row, claimInstant), definition, new IllegalStateException(
                    "runner could not be resolved: " + Objects.requireNonNullElse(e.getMessage(), e.toString()), e), grant);
            return;
        }

        // The payload arrived in the round's batched read. A row that did not deserialise is terminal by
        // nature (a corrupt payload does not heal by re-reading); a TRANSIENT failure of the query never
        // reaches here (it is handled in the round, and the whole batch is left to the reaper).
        RuntimeException unreadable = payloads.unreadable().get(id);
        if (unreadable != null) {
            failUnreadablePayload(executionFor(work, row, claimInstant), definition, unreadable, grant);
            return;
        }
        if (row == null) {
            failUnreadablePayload(executionFor(work, null, claimInstant), definition, new IllegalStateException(
                    "execution " + id + " has a lease but no history row — the enqueue unit was broken: the execution,"
                            + " its queue entry and its idempotency key must be written in ONE transaction, and something"
                            + " upstream split them"), grant);
            return;
        }
        Execution execution = executionFor(work, row, claimInstant);

        // Registered BEFORE the runAsync: the ownership has existed since the claim — the tick's sweep
        // (timeout, cancel, watchdog) has to reach it even if the executor leaves it queued for a whole
        // tick. Removals always use the two-argument remove: the same ExecutionId may be re-claimed by
        // this node after a requeue, and a zombie's late whenComplete must not erase the new
        // incarnation's entry (an in-memory ABA — InFlightAttempt has identity equality).
        InFlightAttempt attempt = new InFlightAttempt(definition.timeout(), execution, definition, grant);
        inFlightAttempts.put(id, attempt);
        CompletableFuture<Void> future;
        try {
            future = CompletableFuture.runAsync(() -> dispatcher.dispatch(execution, definition, Objects.requireNonNull(row).payload(), attempt.signal, grant), executor);
        } catch (RuntimeException e) {
            inFlightAttempts.remove(id, attempt);
            log.warn("runner executor rejected execution {} — already leased, it will stand until a reaper reclaims it", id, e);
            return;
        }
        inFlight.add(future);
        future.whenComplete((_, thrown) -> {
            // Dispatcher.dispatch handles the known failures — a throwable here means the outcome write
            // itself threw; swallowing it would hide the zombie's only trace
            if (thrown != null) {
                log.error("dispatch of execution {} threw outside the normal failure paths", id, thrown);
            }
            inFlightAttempts.remove(id, attempt);
            inFlight.remove(future);
        });
    }

    /** The public view of the in-flight incarnation — built from history's header; without attempts (nothing counts attempts on the hot path). */
    private Execution executionFor(WorkQueue.ClaimedWork work, HistoryStore.@Nullable PayloadRow row, Instant claimInstant) {
        if (row == null) {
            // A history row that is missing or unreadable: the minimum that is true for the
            // events — identity, state and ownership; the rest is unknown
            return new Execution(
                    work.executionId(),
                    work.jobKey(),
                    ExecutionState.RUNNING,
                    claimInstant,
                    claimInstant,
                    Optional.empty(),
                    "unknown",
                    Priority.NORMAL,
                    null,
                    null,
                    nodeId()
            );
        }
        HistoryStore.ExecutionHead head = row.head();
        return new Execution(work.executionId(), work.jobKey(), ExecutionState.RUNNING, head.scheduledAt(), claimInstant,
                Optional.empty(), head.actor(), Priority.fromValue(head.priority()), null, head.batchId(), nodeId());
    }

    /**
     * The "due trigger to acquisition" step — BEFORE the claim, on the same tick, so a freshly
     * materialised occurrence is already claimable without waiting for the next poll.
     *
     * <p>{@link FiringPlanner} decides what the job's misfire policy fires; {@link TriggerFirer#fire} is
     * the transactional CAS that resolves the race between nodes — losing is routine, not an error. One
     * job's failure does not take down the sweep of the others.
     */
    private FiringOutcome fireDueTriggers(TickBudget budget) {
        var page = jobStore.findDueRecurring(clock.instant(), FIRE_LIMIT);
        Instant now = page.observedAt();
        List<StoredJob> due = page.jobs();
        // The same budget as the claim laps: each CAS is bounded by the tick's statement ceiling, but
        // up to FIRE_LIMIT of them in a row would still outlast the node's lease when a host
        // transaction holds a handful of definition rows. What does not fire now fires next tick

        Instant rearmedAt = null;
        int fired = 0;
        for (StoredJob job : due) {
            if (budget.spent()) {
                log.warn("firing sweep stopped after {} of {} due trigger(s) — the tick's budget is spent, "
                        + "the rest fire next tick", fired, due.size());
                break;
            }
            fired++;
            JobDefinition definition = job.definition();
            try {
                // Inside the try: a contract violated by a custom store fails ONLY this job, not the sweep nor the tick's claim
                Instant observed = Objects.requireNonNull(job.nextFireAt(), "findDueRecurring returned an unarmed trigger");
                rearmedAt = earlierFutureFire(rearmedAt, fireTrigger(definition, job.scheduleRevision(), observed, now), now);
            } catch (RuntimeException e) {
                log.error("firing job '{}' failed — will retry next tick", definition.key().value(), e);
            }
        }
        // A due trigger means work in the database, EVEN on losing the CAS (another node materialised
        // occurrences that may land in this node's shards) — reset the backoff on the conservative side
        return new FiringOutcome(!due.isEmpty(), rearmedAt);
    }

    /**
     * What the trigger sweep returns to the tick: whether there was a due trigger — which resets the
     * backoff — and the nearest of the {@code next_fire_at} values this sweep's CAS operations have just
     * armed ({@code null} means none: nobody fired, everyone lost the CAS, or all were fixed-delay
     * awaiting an end).
     *
     * <p>The definitions snapshot does not know this: it predates the firing (see
     * {@link JobStore#nextFireAfter}).
     */
    private record FiringOutcome(boolean fired, @Nullable Instant rearmedAt) {
    }

    /**
     * One due trigger: the misfire policy's plan becomes occurrences materialised by the CAS — losing
     * the CAS exits quietly (see {@link #fireDueTriggers}).
     *
     * @return the {@code next_fire_at} THIS call armed, or {@code null} if the CAS was lost or the plan
     *         disarmed the trigger (fixed-delay awaiting the end)
     */
    private @Nullable Instant fireTrigger(JobDefinition definition, long scheduleRevision, Instant observed, Instant now) {
        FiringPlanner.Plan plan = firingPlanner.plan(definition.schedule(), definition.misfire(), observed, now);
        List<Execution> occurrences = plan
                .occurrences()
                .stream()
                .map(occurrenceAt -> newOccurrence(definition.key(), occurrenceAt))
                .toList();
        if (!triggerFirer.fire(definition.key(), scheduleRevision, observed, plan.nextFireAt(), occurrences, now)) {
            return null;
        }
        if (plan.misfired()) {
            log.warn("job '{}' missed occurrence(s) — next_fire_at was {} at tick time {}; misfire policy {} applied: "
                    + "{} occurrence(s) materialized, next fire at {}",
                    definition.key().value(), observed, now, definition.misfire(), occurrences.size(), plan.nextFireAt());
        } else {
            log.debug("job '{}' fired {} occurrence(s), next fire at {}",
                    definition.key().value(), occurrences.size(), plan.nextFireAt());
        }
        return plan.nextFireAt();
    }



    /**
     * An occurrence materialised by the trigger: {@code scheduled_at} is the occurrence's instant (the
     * firing's identity), {@code actor} distinguishes the scheduler's chain from manual firings, and the
     * payload is empty.
     */
    private static Execution newOccurrence(JobKey jobKey, Instant occurrenceAt) {
        return Execution.enqueued(
                ExecutionId.of(UUIDv7.randomUUIDString()),
                jobKey,
                occurrenceAt,
                Execution.SCHEDULER_ACTOR,
                Priority.NORMAL
        );
    }

    /**
     * {@code JobDefinition.timeout}'s deadline, checked as a passenger on the tick's sweep — a delay of
     * up to one loop interval (between the floor and {@code min(max-poll-interval, node-lease-ttl/3)},
     * depending on the backoff), zero new threads, and active in PAUSED and DRAINING too.
     *
     * <p>It raises the flag and interrupts; the outcome is passive — recorded when the handler stops
     * ({@link Dispatcher}), never here. The clock runs from the handler's REAL start: time queued in a
     * runner does not count.
     */
    private void signalJobTimeouts() {
        inFlightAttempts.forEach((id, attempt) -> {
            if (attempt.markTimeoutIfDue()) {
                log.warn("execution {} exceeded its job timeout {} — cancellation signalled and the handler interrupted; "
                        + "the outcome follows when the handler stops", id, attempt.timeout);
                attempt.signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, true);
            }
        });
    }

    /** Inspect at most 1,000 local entries per visit. The weakly consistent cursor continues next round. */
    private void pollCancelRequests() {
        if (cancelPollCursor == null || !cancelPollCursor.hasNext()) cancelPollCursor = inFlightAttempts.entrySet().iterator();
        List<ExecutionId> unsignalled = new ArrayList<>();
        for (int visited = 0; visited < 1000 && cancelPollCursor.hasNext(); visited++) {
            var entry = cancelPollCursor.next();
            if (!entry.getValue().signal.cancellationRequested()) unsignalled.add(entry.getKey());
        }
        if (unsignalled.isEmpty()) return;
        for (ExecutionId id : leaseStore.findCancelRequested(unsignalled)) {
            InFlightAttempt attempt = inFlightAttempts.get(id);
            if (attempt != null) attempt.signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
        }
    }

    /** Continue a bounded local scan across rounds, releasing overrun grants through the normal fenced completion. */
    private void signalWatchdogOverruns(TickBudget budget) {
        if (watchdogCursor == null || !watchdogCursor.hasNext()) watchdogCursor = inFlightAttempts.entrySet().iterator();
        for (int visited = 0; visited < 1000 && watchdogCursor.hasNext() && !budget.spent(); visited++) {
            var entry = watchdogCursor.next();
            InFlightAttempt attempt = entry.getValue();
            if (inFlightAttempts.get(entry.getKey()) == attempt && !attempt.ownershipReleased
                    && watchdogBoundExceeded(attempt.submittedNanos)) releaseOverrunOwnership(entry.getKey(), attempt);
        }
    }

    /** One bound overrun: it releases the fenced ownership; a failed release does not mark — the next tick tries again. */
    private void releaseOverrunOwnership(ExecutionId id, InFlightAttempt attempt) {
        try {
            dispatcher.abandonOwnership(attempt.execution, attempt.definition, attempt.grant,
                    "watchdog bound " + settings.watchdogTimeout() + " exceeded — node released ownership; "
                            + "the local handler keeps running as a zombie and its result will be discarded");
            attempt.ownershipReleased = true;
            log.warn("execution {} exceeded mohs.engine.watchdog-timeout {} — ownership released (retry budget "
                    + "applied); the local handler keeps running as a zombie until it finishes",
                    id, settings.watchdogTimeout());
        } catch (RuntimeException e) {
            log.warn("could not release ownership of watchdog-overrun execution {} — will retry next tick",
                    id, e);
        }
    }

    /** Runtime by monotonic time ({@code System.nanoTime}) — a duration never comes from the injected {@code Clock}, which may jump on a resync. */
    private boolean watchdogBoundExceeded(long startedNanos) {
        Duration bound = settings.watchdogTimeout();
        return bound != null && Duration.ofNanos(System.nanoTime() - startedNanos).compareTo(bound) >= 0;
    }

    /**
     * One in-flight dispatch incarnation on this node: the submit's monotonic instant (the Watchdog
     * Bound's basis — queue time counts on purpose), the job's {@code timeout} ({@code null} means no
     * timeout), the {@link CancellationSignal} and the ownership ({@link Dispatcher.Grant}).
     *
     * <p>A class, not a record, and deliberately without {@code equals}: equality IS identity — each
     * dispatch is a distinct incarnation, and the two-argument {@code remove(id, attempt)} may only
     * remove THIS one.
     */
    private static final class InFlightAttempt {

        final long submittedNanos = System.nanoTime();
        final @Nullable Duration timeout;
        final CancellationSignal signal = new CancellationSignal();
        /** The incarnation itself — what the watchdog needs in order to release ownership without a fresh query. */
        final Execution execution;
        final JobDefinition definition;
        final Dispatcher.Grant grant;
        /**
         * Ownership released by the watchdog — written only by the tick's thread. The entry stays in the
         * map until completion: a marked zombie still receives the shutdown's interrupt and the cancel
         * signal.
         */
        volatile boolean ownershipReleased;
        /** Only the coordinator writes this; cancellation's first reason does not disable timeout delivery. */
        private boolean timeoutSignalled;

        InFlightAttempt(@Nullable Duration timeout, Execution execution, JobDefinition definition, Dispatcher.Grant grant) {
            this.timeout = timeout;
            this.execution = execution;
            this.definition = definition;
            this.grant = grant;
        }

        /** The tick sweep's trigger: it has a timeout of its own, the handler has been running longer than that, and the signal has not been raised yet. */
        boolean markTimeoutIfDue() {
            if (timeout == null || timeoutSignalled || !signal.handlerRunningLongerThan(timeout)) return false;
            timeoutSignalled = true;
            return true;
        }
    }

    /**
     * A terminal failure decided while still on the tick's thread — a removed definition or a runner with
     * no match fails only this execution, never the tick: if the failure write itself throws, the rest of
     * the batch still needs dispatching — log it and leave the ownership standing for a reaper.
     */
    private void failBeforeDispatchGuarded(Execution execution, @Nullable JobDefinition definition, IllegalStateException error, Dispatcher.Grant grant) {
        try {
            dispatcher.failBeforeDispatch(execution, definition, error, grant);
        } catch (RuntimeException e) {
            log.warn("could not record the terminal failure of execution {} ({}) — its lease stands until a reaper reclaims it", execution.id(), error.getMessage(), e);
        }
    }

    /**
     * A corrupt payload, or a class gone from the classpath, does not stall the cycle — it fails only
     * this execution, directly, without going through the handler.
     *
     * <p>Only {@code findPayloads}'s PER-ROW verdict arrives here (a transient failure of the batched
     * query is left to the reaper and never becomes terminal), plus the lease that has no history row
     * at all. {@link Dispatcher#failBeforeDispatch} synthesises the attempt and publishes
     * {@code Failed} — the same path as any terminal failure.
     */
    private void failUnreadablePayload(Execution execution, @Nullable JobDefinition definition, RuntimeException cause, Dispatcher.Grant grant) {
        String message = "payload could not be read: " + Objects.requireNonNullElse(cause.getMessage(), cause.toString());
        failBeforeDispatchGuarded(execution, definition, new IllegalStateException(message, cause), grant);
    }

    /** Package-private — only {@code EngineTest} uses this, to confirm the node identity behind the heartbeat and the claim. */
    String nodeId() {
        return membership.nodeId();
    }
}
