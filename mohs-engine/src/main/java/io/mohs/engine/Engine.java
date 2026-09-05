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
import java.util.LinkedHashMap;
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
 * The engine over the persistence ports: it wires {@link WorkQueue}, {@link Dispatcher},
 * {@link HistoryStore}, {@link LeaseStore} and {@link NodeStore} into a cycle that actually runs. It
 * implements {@link MohsLifecycle} directly: "exposed through {@code mohs.lifecycle()}" is already
 * the same state machine the poll loop needs.
 *
 * <p>An ADAPTIVE poll with local hand-off: the loop sleeps between the floor and the ceiling
 * depending on whether it finds work, and an enqueue from THIS JVM that is already due wakes it
 * ({@link #signalWorkScheduled}). A cross-node event-driven wakeup (LISTEN/NOTIFY) was implemented,
 * MEASURED and withdrawn — the notifying transaction does not take part in group commit and
 * serialised the ingest; the adaptive poll is the only cross-node backstop, with latency bounded by
 * {@code max-poll-interval}. {@code nodeId} is a UUID generated per instance.
 *
 * <p><b>Admission guards:</b> the predicates the single-table era paid for PER CANDIDATE in the
 * claim's SQL (window, rate limit, concurrency cap) became a PER-JOB inadmissible list, computed in
 * memory before each round — and the remainder is resolved AFTER the claim through the
 * admission-loss path ({@link WorkQueue#requeue}, without consuming budget). The cap is DERIVED from
 * {@code mohs_lease}: counting live ownership replaces the hot {@code running_execution_count}
 * counter; over-admission between nodes is bounded to one round times the node count and corrects
 * itself on the next round.
 *
 * <p><b>Liveness:</b> per NODE — a heartbeat promising {@code now + node-lease-ttl}; this engine's
 * reaper reclaims dead nodes' leases ({@link LeaseStore#findOrphaned}) deciding by the retry budget,
 * and EVERY completion is fenced by {@code (node_id, epoch, attempt_number)}. Under node failure the guarantee is
 * <b>at-least-once</b> when {@code retries > 0} — {@link JobDefinition#retries()}'s default; with a
 * zero budget a reclaimed orphan has nowhere to reschedule and the guarantee drops to at-most-once.
 *
 * <p><b>Timeout and cancellation:</b> checked as a passenger on the tick — the flag plus an interrupt
 * through {@link CancellationSignal}, with a passive outcome in the {@link Dispatcher}. A handler
 * that ignores the interrupt stays a zombie until it finishes on its own and has its result discarded
 * by the fence — that is the Watchdog Bound's case, the next rung of the ladder.
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
     * The ceiling on the inadmissible list that goes into the claim's {@code NOT IN} — a comfortable
     * margin under the ~2100-parameter limit of SQL Server's driver (the tightest ceiling of the four
     * databases), with the rest of the bind already discounted.
     */
    static final int MAX_INADMISSIBLE_FILTER = 1000;

    /**
     * How often the backlog is counted for {@code mohs.queue.depth}. Ten seconds sits under every
     * usual scrape interval (15–60s), so the gauge is never stale by more than one scrape, and far
     * above the tick's floor of 25ms, so the count is a rounding error next to the claim: at three
     * nodes it is 0.3 statements per second against the 4.0 an idle node already issues.
     */
    private static final Duration QUEUE_DEPTH_SAMPLE_INTERVAL = Duration.ofSeconds(10);

    /**
     * How often {@code mohs_idempotency} is pruned. The retention is measured in days, so an hour is
     * already two orders of magnitude finer than the window it enforces — pruning more often would
     * only issue the same DELETE against rows that are not old enough yet.
     */
    private static final Duration IDEMPOTENCY_PRUNE_INTERVAL = Duration.ofHours(1);

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
    private final String nodeId;
    private final EngineMetrics metrics;
    private final RetryPolicyRegistry retryPolicies;

    /**
     * Sampled by the loop thread, read by whatever thread scrapes the registry — an {@code AtomicLong}
     * for the safe publication, not for atomicity: there is exactly one writer.
     */
    private final AtomicLong queueDepth = new AtomicLong();
    private final Cadence queueDepthSample = new Cadence(QUEUE_DEPTH_SAMPLE_INTERVAL);
    /** {@code null} when {@code mohs.engine.idempotency-retention} is zero: the operator opted out of pruning. */
    private final @Nullable Cadence idempotencyPrune;
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
    private final Set<ExecutionId> strayLeaseCandidates = new HashSet<>();
    /**
     * The claim lap's rotation cursor — it advances on every probed shard and PERSISTS between ticks,
     * so each lap's first shard rotates rather than always restarting at the same one: without it, a
     * tick that exhausts its budget on shard 0 would starve the last ones in the list. Touched only by
     * the tick's thread (JCIP 3.3); overflow is benign through {@code Math.floorMod}.
     */
    private int shardCursor;
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
    /** Guardado por {@code wakeLock}. */
    private boolean wakeRequested;

    /** {@code volatile}: written by {@link #start} and read by {@link #stop}, which may come from different threads ({@code MohsLifecycle} is public API) — safe publication, JCIP 3.1. */
    private volatile @Nullable Thread loopThread;

    /** The grace-escalation WARN is one per shutdown — the SIGNAL is not guarded by it (see {@link #escalateAfterDrainGrace}). */
    private final AtomicBoolean drainEscalated = new AtomicBoolean();

    /**
     * The NODE's incarnation and lease — written only by the tick's thread ({@link #renewNodeLease}).
     * The epoch starts at 1 and only rises when the node itself notices its lease expired; it is also
     * the fence on every completion.
     *
     * <p>{@code volatile} on the epoch only because of the outside reader:
     * {@link #writeFinalStoppedHeartbeat} reads it from another thread during the stop (JCIP 3.1; JLS
     * 17.7 for a {@code long}) — the same reason as {@code tickHandle}.
     */
    private volatile long nodeEpoch = 1;
    private @Nullable Instant nodeLeaseExpiresAt;

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

    /** The form that knows the declared {@link io.mohs.core.execution.RetryPolicy} beans — what the starter builds. */
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
        this.nodeId = UUIDv7.randomUUID().toString();
        metrics.bindNodeGauges(inFlightAttempts::size, settings.dispatchConcurrency());
        metrics.bindQueueDepthGauge(queueDepth::get);
        Duration retention = settings.idempotencyRetention();
        this.idempotencyPrune = retention.isPositive() ? new Cadence(IDEMPOTENCY_PRUNE_INTERVAL) : null;
        this.historyPrune = settings.historyRetention().isPositive() ? new Cadence(HISTORY_PRUNE_INTERVAL) : null;
    }

    @Override
    public EngineState state() {
        return state.get();
    }

    /** A guarded CAS (the project's discipline throughout), not a lock: a transition only applies if the source state is still the expected one. */
    @Override
    public void start() {
        if (!state.compareAndSet(EngineState.CREATED, EngineState.RUNNING)) {
            throw new IllegalStateException("start() only valid from CREATED, was " + state.get());
        }
        // ONE platform thread, never a scheduler or a virtual one: latency-critical, immune to carrier
        // starvation, and it appears with a name of its own in any profiler or thread dump — which is
        // what matters at 3 a.m. Daemon: a leaked engine never holds up the JVM's exit (a crash is
        // already covered semantics — the node's lease expires and the reaper reclaims).
        Duration heartbeatCadence = settings.nodeLeaseTtl().dividedBy(3);
        if (settings.pollInterval().compareTo(heartbeatCadence) > 0) {
            // The liveness cap in awaitWork also swallows the FLOOR — the operator asked for ticks
            // spaced further apart than the liveness promise allows; liveness wins, but doing so in
            // silence would be a tuning mystery
            log.warn("effective tick cadence is capped at node-lease-ttl/3 ({}) — mohs.engine.poll-interval ({}) "
                    + "exceeds it; the heartbeat each tick carries is what keeps this node alive to its peers",
                    heartbeatCadence, settings.pollInterval());
        }
        Thread thread = Thread.ofPlatform().name("mohs-engine-loop").daemon(true).unstarted(this::runLoop);
        loopThread = thread;
        thread.start();
        if (state.get() == EngineState.STOPPED) { // stop() won the race during start-up — wake the loop it never saw
            wake();
        }
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
        Objects.requireNonNull(grace, "grace");
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
     *   <li><b>Nobody renews the node's lease during it</b> — {@code renewNodeLease} only runs on the
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
        Objects.requireNonNull(grace, "grace");
        EngineState current = state.get();
        if (current == EngineState.STOPPED) {
            throw new IllegalStateException("already STOPPED");
        }
        long startNanos = System.nanoTime();
        DrainDeadline deadline = DrainDeadline.startingNow(grace);
        if (current != EngineState.DRAINING) {
            drain(grace);
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

    /**
     * The last heartbeat of a graceful shutdown writes STOPPED — without it, a clean stop and a crash
     * would be indistinguishable in the database. Best-effort on purpose: a shutdown never fails
     * because the database is down; whoever dies without managing to write is covered by staleness and
     * by the purge (see {@link #purgeStaleNodeRows}).
     *
     * <p>A deliberately accepted race: a tick that read the state before {@code state.set(STOPPED)} may
     * commit its heartbeat AFTER this one — the row stays RUNNING/DRAINING with a fresh promise of
     * +{@code node-lease-ttl}, and this node's orphans wait that extra TTL for the reaper (the same
     * cost as a crash). Closing it would require the stop to wait for the current tick, coupling
     * shutdown latency to a window that staleness plus the purge already cover.
     */
    private void writeFinalStoppedHeartbeat() {
        try {
            // A stopped node promises no liveness — expires_at = now (an already expired lease) tells
            // the peers "what was mine is free"
            Instant now = clock.instant();
            nodeStore.heartbeat(nodeId, EngineState.STOPPED, nodeEpoch, now, now);
        } catch (RuntimeException e) {
            log.warn("could not write the final STOPPED heartbeat for node {} — the row will read as its last "
                    + "ticked state until the stale purge collects it", nodeId, e);
        }
    }

    /**
     * Collects idempotency keys older than {@code mohs.engine.idempotency-retention}, which IS the
     * deduplication window: while the row exists, reusing the key returns the original execution; once
     * it is gone, the same key starts a new one. Without this the window was the lifetime of the
     * installation, and the table grew forever.
     *
     * <p>Every node issues the same DELETE, exactly like {@link #purgeStaleNodeRows} — isolated by
     * {@code runMaintenance} for the same reason, and an order of magnitude rarer.
     */
    private void pruneIdempotencyKeys(Instant now) {
        Duration retention = settings.idempotencyRetention();
        int pruned = historyStore.pruneIdempotencyBefore(now.minus(retention));
        if (pruned > 0) {
            log.info("pruned {} idempotency key(s) older than {} — reusing one of those keys now starts a new execution",
                    pruned, retention);
        }
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
    private void pruneHistory(Instant now) {
        Duration retention = settings.historyRetention();
        Instant cutoff = now.minus(retention);
        long deadlineNanos = System.nanoTime() + HISTORY_PRUNE_BUDGET.toNanos();
        int executions = 0;
        int attempts = 0;
        int batches = 0;
        while (true) {
            HistoryStore.PrunedHistory pruned = historyStore.pruneHistoryBefore(cutoff, HISTORY_PRUNE_BATCH);
            executions += pruned.executions();
            attempts += pruned.attempts();
            batches += pruned.batches();
            if (pruned.drained(HISTORY_PRUNE_BATCH) || System.nanoTime() - deadlineNanos >= 0) {
                break;
            }
        }
        if (executions > 0 || attempts > 0 || batches > 0) {
            log.info("pruned history older than {} — {} execution(s), {} attempt(s), {} member-less batch(es)",
                    retention, executions, attempts, batches);
        }
    }

    /**
     * Collects heartbeats older than {@code node-lease-ttl x }{@link #STALE_NODE_RETENTION_LEASES} —
     * riding along on the tick. This is not death detection (that is derived at READ time): it merely
     * collects rows no reader has any use for, because each boot generates a new {@code node_id} and
     * the table would grow forever.
     */
    private void purgeStaleNodeRows() {
        // node-lease-ttl, not lease-ttl: NODE liveness is governed by the node's lease. Deriving it from
        // the EXECUTION TTL let an operator who lowered lease-ttl delete the row of a node whose promise
        // is still alive — and an absent row IS death at read time, so the reaper would reclaim work in
        // flight on a live node
        Instant cutoff = clock.instant().minus(settings.nodeLeaseTtl().multipliedBy(STALE_NODE_RETENTION_LEASES));
        int purged = nodeStore.deleteHeartbeatsBefore(cutoff);
        if (purged > 0) {
            log.info("purged {} stale node heartbeat row(s) — instances dead or restarted more than {} lease TTLs ago",
                    purged, STALE_NODE_RETENTION_LEASES);
        }
    }

    /**
     * One in-flight wait's deadline: the monotonic instant at which it ends, plus the {@code grace} the
     * operator asked for (only {@link Engine#escalateAfterDrainGrace}'s message uses the latter).
     *
     * <p>It is an object because {@link Engine#stop} waits twice and both measure against the SAME
     * deadline — waiting twice must not charge the grace twice.
     */
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
            // JCIP §7.3: every service thread declares what happens when it dies, and this one IS the
            // engine. tick() handles RuntimeException; an Error (OOME, a NoClassDefFoundError from a
            // listener loaded late, a StackOverflow in a recursive interceptor) used to escape here in
            // silence — the thread died, state stayed RUNNING forever, MohsLifecycle#state and
            // GET /nodes lied, and the only clue was the JVM's default handler
            log.error("engine loop died — this node stops claiming. Its node lease will expire and peers will"
                    + " reclaim the work still running here (at-least-once). Restart the instance.", t);
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
        Duration heartbeatCadence = settings.nodeLeaseTtl().dividedBy(3);
        Duration bounded = delay.compareTo(heartbeatCadence) > 0 ? heartbeatCadence : delay;
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
        try {
            EngineState current = state.get();
            Instant now = clock.instant();
            // renewNodeLease stays OUTSIDE the isolation on purpose: with no heartbeat the node is
            // already considered dead by its peers, so continuing the tick would help nobody. The three
            // signals are maintenance like the others — a pollCancelRequests failing persistently
            // (permissions, a missing table) would produce exactly the symptom runMaintenance exists to
            // prevent: a live node, RUNNING, owning shards, claiming nothing
            renewNodeLease(current, now);
            runMaintenance("signal-job-timeouts", this::signalJobTimeouts);
            runMaintenance("poll-cancel-requests", this::pollCancelRequests);
            runMaintenance("signal-watchdog-overruns", this::signalWatchdogOverruns);
            // Both are cadenced rather than per-tick, and both belong ABOVE the RUNNING check: a paused
            // or draining node still owes the operator the backlog it can see, and housekeeping is not
            // claiming
            if (queueDepthSample.due()) {
                runMaintenance("queue-depth-sample", () -> queueDepth.set(workQueue.countVisible(now)));
            }
            if (idempotencyPrune != null && idempotencyPrune.due()) {
                runMaintenance("idempotency-prune", () -> pruneIdempotencyKeys(now));
            }
            if (historyPrune != null && historyPrune.due()) {
                runMaintenance("history-prune", () -> pruneHistory(now));
            }
            if (current != EngineState.RUNNING) {
                // No trigger horizon outside RUNNING: what does not fire has no deadline to honour —
                // PAUSED/DRAINING owe only the heartbeat
                return TickOutcome.idle();
            }
            Map<JobKey, StoredJob> definitions = loadDefinitions();
            // ONE read of mohs_nodes per tick serves both the reaper (the living) and the shard
            // assignment (the eligible) — after the heartbeat, so the freshly renewed row is in the
            // picture
            List<StoredNode> nodes = nodeStore.findAll();
            // Maintenance is best-effort and runs ISOLATED: the claim is the reason the tick exists and
            // must not be its hostage. Previously a single try covered all seven steps — and
            // purgeStaleNodeRows issues the SAME delete on every node on every tick, a classic deadlock
            // candidate on SQL Server. A purge failing persistently stole fire and claim forever, with
            // the node alive, heartbeating and owning 1/n of the shards: exactly the failure mode
            // probeSaysThereIsWork's Javadoc describes and shields only the probe from
            runMaintenance("reap-orphaned-leases", () -> reapOrphanedLeases(definitions, aliveNodeIds(nodes, now), now));
            runMaintenance("reconcile-stray-leases", () -> reconcileOwnStrayLeases(now));
            runMaintenance("stale-node-purge", this::purgeStaleNodeRows);
            FiringOutcome firing = fireDueTriggers();
            int claimed = claimAndDispatch(definitions, Shards.ownedBy(nodeId, shardEligibleNodeIds(nodes, now)));
            return new TickOutcome(firing.fired() || claimed > 0,
                    earliestArmedFire(definitions.values(), firing.rearmedAt(), now));
        } catch (RuntimeException e) {
            // Counted like a maintenance step, under its own label: the alert on mohs.tick.failed is
            // what separates "idle because the queue is empty" from "idle because every tick dies",
            // and a death in the heartbeat, the definitions read, the firing or the claim is the
            // one that matters most
            metrics.tickStepFailed("tick");
            log.error("engine tick failed — will retry next tick", e);
            // A failure is not an empty queue, but backoff is the right answer to both: a database that
            // is down does not improve by being hammered at the 25ms floor — nor by the trigger cap,
            // whose horizon this tick never got to determine
            return TickOutcome.idle();
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
            log.warn("tick step '{}' failed — claim and dispatch continue this tick", step, e);
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
     * The nearest armed {@code next_fire_at} after this tick.
     *
     * <p>Two sources, because the definitions snapshot predates the firing: the triggers THIS tick did
     * not touch count as the snapshot says, and the ones we fired count as of the instant the CAS just
     * armed ({@code rearmedAt}, determined by {@link #fireDueTriggers}, which already hands it over
     * filtered to the future — which is why it enters as the seed rather than as one more candidate).
     */
    static @Nullable Instant earliestArmedFire(Collection<StoredJob> snapshot, @Nullable Instant rearmedAt, Instant now) {
        Instant earliest = rearmedAt;
        for (StoredJob job : snapshot) {
            // The same filter as JobStore#findDueRecurring: a paused or orphaned job has an armed
            // next_fire_at that NOBODY fires — shortening the sleep for it would mean waking up for a
            // trigger that does not exist. The snapshot comes from findAll, which only discards retired
            // ones
            if (!job.paused() && !job.orphaned()) {
                earliest = earlierFutureFire(earliest, job.nextFireAt(), now);
            }
        }
        return earliest;
    }

    /**
     * The nearest of the horizon determined so far and one more candidate — the step
     * {@link #earliestArmedFire} and {@link #fireDueTriggers} repeat over different sources.
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

    /**
     * Noticing this node's OWN node lease has expired BEFORE renewing it — this node was "dead" to the
     * cluster (a pause or stall longer than node-lease-ttl) and peers may have reclaimed what was in
     * flight. The epoch bump records the reincarnation and, since the fence is {@code (node_id, epoch, attempt_number)},
     * it defeats the zombies' writes by itself.
     *
     * <p>The heartbeat goes out in any state — PAUSED and DRAINING included (a drain is not a cancel).
     */
    private void renewNodeLease(EngineState current, Instant now) {
        // !isBefore, not isAfter: a PEER's predicate (aliveNodeIds) considers the node dead already at
        // equality — the self-diagnosis has to be symmetric, otherwise at the exact instant of
        // expiry a peer would reap without this node bumping its epoch
        if (nodeLeaseExpiresAt != null && !now.isBefore(nodeLeaseExpiresAt)) {
            nodeEpoch++;
            log.warn("node lease expired at {} while this node was stalled — epoch bumped to {}; peers may have "
                    + "reclaimed in-flight work (their re-runs stand; this node's fenced completions will be discarded)",
                    nodeLeaseExpiresAt, nodeEpoch);
            // One reincarnation per observed death: the expired promise is consumed here, so a heartbeat
            // that keeps failing (a network partition) does not bump the epoch — and log this WARN — on
            // every tick until the database is reachable again. The next promise the cluster actually
            // sees is recorded below, after a successful heartbeat.
            nodeLeaseExpiresAt = null;
        }
        // A NEGATIVE clock jump: the node's lease is wall-clock by nature (it is a promise peers read),
        // so nanoTime does not help here. What can be done is to make it diagnosable — without this,
        // "why did the cluster re-execute 60 jobs at 04:12?" has no log line connecting symptom to
        // cause
        Instant promised = now.plus(settings.nodeLeaseTtl());
        if (nodeLeaseExpiresAt != null && promised.isBefore(nodeLeaseExpiresAt)) {
            log.warn("clock moved backwards between heartbeats — the new lease promise ({}) is EARLIER than the"
                    + " previous one ({}). Peers may declare this node dead and reclaim work still running here"
                    + " (duplicate execution). Check NTP, or mohs.time.mode/sync-interval if the database clock"
                    + " is the source.", promised, nodeLeaseExpiresAt);
        }
        nodeStore.heartbeat(nodeId, current, nodeEpoch, now, promised);
        // Only the promise the cluster ACTUALLY saw counts for detecting one's own death: if the
        // heartbeat fails (the tick's catch), the field retains the last persisted promise — or the
        // null an observed expiry left — and the bump fires once, on the first tick that finds the
        // persisted promise in the past
        nodeLeaseExpiresAt = promised;
    }

    /**
     * The tick's definitions snapshot: one scan of the cold definitions table serves admission
     * (window, cap and rate per job), dispatch and the reaper — in place of N finds per job_key.
     *
     * <p>It becomes an in-memory cache with version-based invalidation when that work arrives; the
     * staleness of one tick is the same as the previous memoisation's.
     */
    private Map<JobKey, StoredJob> loadDefinitions() {
        Map<JobKey, StoredJob> byKey = new HashMap<>();
        try (var all = jobStore.findAll()) {
            all.forEach(stored -> byKey.put(stored.definition().key(), stored));
        }
        return byKey;
    }

    /**
     * The tick snapshot's {@link StoredJob}, with a fresh query on a miss: the snapshot may PRECEDE a
     * define-plus-schedule from the same instant (the claim sees the fresh queue, the snapshot does
     * not) — without the fresh query, a newly defined job would be treated as removed (a real bug:
     * about 50% failure in the starter's end-to-end test that defines and schedules in sequence).
     *
     * <p>A genuinely removed job still yields {@code null} — each caller decides the outcome. The cost
     * falls only on a miss: newborn or removed, never the hot path.
     *
     * <p>A cured find enters the snapshot (a mutable map confined to the tick's thread, JCIP 3.3):
     * without the memoisation, the {@code Admission} of the SAME tick's later rounds would not include
     * the newborn in {@code capped} — {@code leaseCount} would return 0 and
     * {@code maxConcurrentExecutions}'s mutex would become a no-op up to
     * {@code claimRounds x cap} concurrent executions on a single node. A retired job is not memoised:
     * {@code null} is not a find.
     */
    private @Nullable StoredJob storedJobFor(JobKey jobKey, Map<JobKey, StoredJob> definitions) {
        StoredJob stored = definitions.get(jobKey);
        if (stored != null) {
            return stored;
        }
        StoredJob fresh = jobStore.find(jobKey).orElse(null);
        if (fresh != null) {
            definitions.put(jobKey, fresh);
        }
        return fresh;
    }

    /**
     * Reclaims dead nodes' leases — alive means an unexpired promise ({@code expires_at > now}) or, on
     * a row from an older jar with no {@code expires_at}, a heartbeat newer than {@code lease-ttl}
     * (mixed-version tolerance); absent from {@code mohs_nodes} is dead by definition.
     *
     * <p>Each orphaned lease becomes a {@link LeaseStore.CompletionResult} fenced by the DEAD node's
     * ownership: with budget, a synthetic FAILED attempt plus a rebirth in the queue in the same
     * transaction; without budget, or retired, a terminal FAILED; with {@code cancel_requested},
     * CANCELLED (the operator's order beats the budget). The synthetic attempt consumes budget like any
     * failure, and the fence guarantees a healthy new incarnation is never killed.
     */
    private void reapOrphanedLeases(Map<JobKey, StoredJob> definitions, List<String> aliveNodeIds, Instant now) {
        List<LeaseStore.Lease> orphans = leaseStore.findOrphaned(aliveNodeIds, RECLAIM_LIMIT);
        if (orphans.isEmpty()) {
            return;
        }
        Map<ExecutionId, HistoryStore.ExecutionHead> heads = new HashMap<>();
        historyStore.findHeads(orphans.stream().map(LeaseStore.Lease::executionId).toList())
                .forEach(head -> heads.put(head.executionId(), head));

        List<ReclaimDecision> decisions = orphans.stream()
                // A snapshot miss falls back to a fresh query (storedJobFor): a newly defined job is not
                // a retired job
                .map(orphan -> decideReclaim(orphan, storedJobFor(orphan.jobKey(), definitions),
                        heads.get(orphan.executionId()), now))
                .toList();
        // One transaction per chunk, not per sweep: the reclaim runs under the tick's deadline, and a
        // mass death is up to RECLAIM_LIMIT rows each paying a rearm and a batch counter round trip —
        // a sweep that could not finish inside the deadline would roll back whole, find the same
        // orphans next tick, and never reclaim anything. A chunk that fails loses only itself: the
        // ones after it still run, and what it lost is found again next tick. The loop itself has the
        // claim laps' budget — ten chunks each waiting out a 3 s deadline would outlast the lease
        TickBudget budget = TickBudget.start(settings.nodeLeaseTtl());
        for (int from = 0; from < decisions.size(); from += RECLAIM_CHUNK) {
            if (from > 0 && budget.spent()) {
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
                leaseStore.reclaim(chunk.stream().map(ReclaimDecision::result).toList(), jobStore);
        for (ReclaimDecision decision : chunk) {
            LeaseStore.Completion verdict = verdicts.get(decision.result().executionId());
            if (verdict != null && verdict.owned()) {
                metrics.leaseReclaimed(decision.reclaimedState(), decision.attemptsExhausted());
                publishReclaimOutcome(decision);
            }
        }
    }

    /**
     * The successor to the per-execution expiry the table split retired: a lease belonging to THIS node
     * with no incarnation in {@code inFlightAttempts} is work lost between claim and dispatch (a failed
     * payload query, an executor rejection, a terminal-failure write that threw) — and the reaper would
     * never reach it, because this node is alive. Without this pass, the only remedy would be restarting
     * the node (a liveness violation — DDIA ch. 8: every ownership needs a reachable expiry path). It
     * returns the work to the queue with the SAME attempt (nothing ran, the budget is intact), through
     * the usual fenced requeue.
     *
     * <p>Two guards against false positives, because the normal case at high throughput is the lease of
     * a completion IN TRANSIT in the {@link CompletionBatcher} (the entry leaves the map when the
     * dispatch returns; the lease only drops at the flush's commit): (1) a grace on {@code claimed_at}
     * ({@link #strayLeaseGrace()}), because at 10k+/s there are ALWAYS dozens of leases in transit and
     * the rounds scheme alone caught them by the thousand (measured in the bench: 199k WARNs, phantom
     * requeues contending for the lock with the flush until deadlock); and (2) two consecutive rounds
     * absent.
     *
     * <p>A legitimate orphan has a frozen {@code claimed_at} — its recovery costs the grace plus two
     * rounds: about 2s at the bench's operating point (20ms poll), about 30s at the 5s default poll —
     * the per-execution era also recovered in a 30s lease-ttl, so never worse, and orders of magnitude
     * better with a short poll. If a completion still loses the race, the requeue's fence decides and
     * the worst case is one re-execution (at-least-once), never corruption. The state is confined to the
     * tick's thread (JCIP 3.3).
     */
    private void reconcileOwnStrayLeases(Instant now) {
        Instant strayBefore = now.minus(strayLeaseGrace());
        Set<ExecutionId> absentNow = new HashSet<>();
        List<WorkQueue.Requeue> strays = new ArrayList<>();
        for (LeaseStore.Lease lease : leaseStore.findByNodes(List.of(nodeId))) {
            // Three guards, in the order state, state, time: an in-flight incarnation; a completion in
            // transit in the batcher (which covers a job running LONGER than the grace); and the grace
            // on claimed_at, which is left for the one case with no consultable state (work lost between
            // claim and dispatch)
            if (inFlightAttempts.containsKey(lease.executionId())
                    || dispatcher.completionInTransit(lease.executionId())
                    || lease.claimedAt().isAfter(strayBefore)) {
                continue;
            }
            absentNow.add(lease.executionId());
            if (strayLeaseCandidates.contains(lease.executionId())) {
                strays.add(new WorkQueue.Requeue(lease.executionId(), nodeId, lease.epoch(), lease.attemptNumber(),
                        new WorkQueue.ReadyEntry(lease.executionId(), lease.jobKey(), Shards.of(lease.executionId()),
                                lease.priority(), lease.attemptNumber(), now)));
            }
        }
        strayLeaseCandidates.clear();
        strayLeaseCandidates.addAll(absentNow);
        if (!strays.isEmpty()) {
            int requeued = workQueue.requeue(strays);
            if (requeued > 0) {
                metrics.claimRequeued("stray-lease", requeued);
                log.warn("requeued {} lease(s) this node was holding with no in-flight incarnation — "
                        + "work lost between claim and dispatch (same attempt, retry budget untouched)", requeued);
            } else {
                // The fence lost on all of them: the completions committed between the findByNodes and
                // the requeue — routine at high throughput, not a finding
                log.debug("stray-lease candidates all vanished before the requeue — completions won the race");
            }
        }
    }

    /**
     * The minimum age for a lease with no incarnation to become a candidate: {@code max(2s, 4xpoll)} —
     * above the worst plausible transit through the batcher.
     *
     * <p>The 2s floor is not theoretical: during the bench's cold start (JIT and warm-up) the flusher
     * fell behind by more than 500ms and the reconcile requeued WHOLE BATCHES of completions in transit
     * (blocks of 256 and 512, 10.7k lost fences in one cold round); with 2s the cold regime stays clean.
     * The cost (a legitimate orphan's recovery waiting out the grace) depends on the poll: about 2s at
     * 20ms, about 20s at the 5s default — never worse than the per-execution era's 30s lease-ttl. It
     * derives from poll-interval, so no new knob.
     */
    private Duration strayLeaseGrace() {
        Duration fourPolls = settings.pollInterval().multipliedBy(4);
        Duration floor = Duration.ofSeconds(2);
        return fourPolls.compareTo(floor) > 0 ? fourPolls : floor;
    }

    /** The living as of this instant — it ALWAYS includes this node (whose promise was just renewed on this same tick, before the reaper: the ordering that kills self-reaping). */
    private List<String> aliveNodeIds(List<StoredNode> nodes, Instant now) {
        return nodeIdsMatching(nodes, node -> isAlive(node, now));
    }

    private boolean isAlive(StoredNode node, Instant now) {
        Instant expiresAt = node.expiresAt();
        // A row from an older jar with no expires_at: staleness of lease-ttl (mixed-version tolerance)
        return expiresAt != null ? expiresAt.isAfter(now) : node.lastHeartbeatAt().isAfter(now.minus(settings.leaseTtl()));
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
        if (!ids.contains(nodeId)) {
            ids.add(nodeId);
        }
        return ids;
    }

    /** One decided reclaim outcome — the fenced result plus what to publish and measure if the fence wins. */
    private record ReclaimDecision(LeaseStore.CompletionResult result, ExecutionState reclaimedState,
            boolean attemptsExhausted, @Nullable Instant retryAt) {
    }

    private ReclaimDecision decideReclaim(LeaseStore.Lease orphan, @Nullable StoredJob storedJob,
            HistoryStore.@Nullable ExecutionHead head, Instant now) {
        ExecutionId id = orphan.executionId();
        JobKey jobKey = orphan.jobKey();
        String batchId = head == null ? null : head.correlationId();
        boolean retired = storedJob == null;
        JobDefinition definition = storedJob == null ? null : storedJob.definition();

        if (orphan.cancelRequested()) {
            // The node died, but the operator's order was already given — rescheduling would disobey it;
            // a CANCELLED attempt with a null error
            return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.epoch(),
                    orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.CANCELLED, null, null,
                    ExecutionState.CANCELLED, null, batchId, rearmFor(head, definition, retired, now)),
                    ExecutionState.CANCELLED, false, null);
        }
        Optional<Instant> nextRetry = retired
                ? Optional.empty() // a retired job never reschedules: the entry would be stuck forever (the queue does not filter retired — retirement drains the queue)
                // No Throwable: the failure happened in a JVM that is no longer running — a custom
                // policy sees null and decides on the attempt number alone
                : retryPolicies.nextRetryAt(Objects.requireNonNull(definition), orphan.attemptNumber(), null, now);
        if (nextRetry.isPresent()) {
            Instant retryAt = nextRetry.orElseThrow();
            return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.epoch(),
                    orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.FAILED,
                    IllegalStateException.class.getName(), NODE_DEAD_ERROR,
                    null,
                    new WorkQueue.ReadyEntry(id, jobKey, Shards.of(id), orphan.priority(), orphan.attemptNumber() + 1, retryAt),
                    batchId, null),
                    ExecutionState.RETRY_WAITING, false, retryAt);
        }
        return new ReclaimDecision(new LeaseStore.CompletionResult(id, jobKey, orphan.nodeId(), orphan.epoch(),
                orphan.attemptNumber(), orphan.claimedAt(), now, ExecutionState.FAILED,
                IllegalStateException.class.getName(), NODE_DEAD_ERROR,
                ExecutionState.FAILED, null, batchId, rearmFor(head, definition, retired, now)),
                ExecutionState.FAILED, !retired, null);
    }

    /**
     * A terminal reclaim of a scheduler occurrence on a fixed-delay job rearms the chain — a zombie's
     * "end" is unknown, so {@code now} (the reaper's observation) anchors it. A manual execution is not
     * the chain; a retired job never rearms.
     */
    private static @Nullable Instant rearmFor(HistoryStore.@Nullable ExecutionHead head,
            @Nullable JobDefinition definition, boolean retired, Instant now) {
        return head != null && !retired && Execution.SCHEDULER_ACTOR.equals(head.actor())
                && definition != null && definition.schedule() instanceof IntervalSpec interval && interval.afterFinish()
                ? now.plus(interval.interval())
                : null;
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
    private int claimAndDispatch(Map<JobKey, StoredJob> definitions, List<Integer> ownedShards) {
        if (ownedShards.isEmpty()) {
            // Once per transition, not per tick: the condition is PERSISTENT (it only changes when the
            // cluster shrinks or SHARD_COUNT rises), and at the 25ms floor it would be ~40 lines/s per
            // node — the log that diagnoses the problem burying everything else
            if (!warnedAboutOwningNoShard) {
                warnedAboutOwningNoShard = true;
            // Owning zero shards is NOT an empty queue — it is this node being outside the partition.
            // Shards.ownedBy returns an empty list for an index >= SHARD_COUNT, and the inner lap then
            // does not run: LapOutcome said "I swept everything and found nothing", armed
            // queueLooksEmpty, and the node sat idle FOREVER announcing RUNNING and occupying a shard
            // slice of its peers', with not one line of log
                log.warn("this node owns no shard of {} — it will never claim. The cluster has more RUNNING nodes"
                        + " than shards; reduce the node count or raise Shards.SHARD_COUNT.", Shards.SHARD_COUNT);
            }
            return 0;
        }
        warnedAboutOwningNoShard = false;
        if (queueLooksEmpty && !probeSaysThereIsWork(ownedShards)) {
            return 0;
        }
        LapOutcome outcome = claimLaps(definitions, ownedShards);
        // Saturated dispatch and an exhausted budget also return zero WITHOUT the queue being empty:
        // arming on those would make the gate pay for a probe per tick on the hot path, the opposite of
        // what it exists to do
        queueLooksEmpty = outcome.claimed() == 0 && outcome.sweptEveryOwnedShard();
        return outcome.claimed();
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

    /**
     * The claim laps proper. Each claim is bounded by the dispatch headroom — a saturated node does not
     * claim; the surplus stays in the queue, claimable by any node with headroom.
     *
     * <p>Under backlog, one tick chains up to {@code claimRounds} claims, with two guards between
     * rounds: a drain or pause breaks the chain, and a monotonic budget of {@code nodeLeaseTtl/4} bounds
     * the total duration — the heartbeat runs ONCE per tick, before this, and rounds approaching the TTL
     * would let the NODE's lease expire mid-tick (a lease is failure detection — DDIA).
     *
     * <p>The inadmissible list is recomputed per round (a closed window, a cap with no headroom —
     * derived from {@code mohs_lease} — an exhausted rate limit), and post-claim admission leftovers go
     * back to the queue WITHOUT consuming budget ({@link #admit}).
     */
    private LapOutcome claimLaps(Map<JobKey, StoredJob> definitions, List<Integer> ownedShards) {
        TickBudget budget = TickBudget.start(settings.nodeLeaseTtl());
        int totalClaimed = 0;
        for (int lap = 0; lap < settings.claimRounds(); lap++) {
            if (lap > 0 && mustStopClaiming(budget)) {
                return new LapOutcome(totalClaimed, false);
            }
            int lapBudget = Math.min(settings.batchSize(), settings.dispatchConcurrency() - inFlight.size());
            if (lapBudget <= 0) {
                return new LapOutcome(totalClaimed, false);
            }
            Instant now = clock.instant();
            // Admission ONCE per lap, never per statement: the over-admission bound stays "nodes x 1
            // lap", with the error in the same direction
            Admission admission = Admission.compute(definitions, windowRegistry, rateLimitStore, leaseStore, now);
            // The SQL filter is a churn optimisation, not correctness — the post-claim admit() is the
            // authority. Above the IN parameter ceiling (SQL Server breaks around 2100) the filter is
            // TRUNCATED, never switched off: degradation has to be monotonic. Switching it off made the
            // claim bring back jobs with a closed window and admit return them with visible_at = now,
            // immediately re-claimable — 2,000 jobs with a business window closing at midnight became a
            // requeue livelock consuming the claim budget of the admissible work
            Collection<JobKey> inadmissibleFilter = admission.inadmissible().size() > MAX_INADMISSIBLE_FILTER
                    ? admission.inadmissible().stream().limit(MAX_INADMISSIBLE_FILTER).toList()
                    : admission.inadmissible();
            // The LAP: one OWN shard per statement, round-robin with a cursor that persists between
            // ticks — a multi-shard predicate would kill the index's ordering (a measured lesson); a complete
            // empty pass ends it
            int remaining = lapBudget;
            for (int probe = 0; probe < ownedShards.size() && remaining > 0; probe++) {
                // The same lap-boundary guard, PER PROBE: one lap is up to 64 statements, and a time
                // budget only protects at the granularity it is checked — a degraded database at
                // 300ms/claim would blow the NODE's lease mid-tick; the tick's first probe always runs
                if ((lap > 0 || probe > 0) && mustStopClaiming(budget)) {
                    return new LapOutcome(totalClaimed, false);
                }
                int shard = ownedShards.get(Math.floorMod(shardCursor++, ownedShards.size()));
                long claimStartNanos = System.nanoTime();
                List<WorkQueue.ClaimedWork> claimed =
                        workQueue.claim(shard, nodeId, nodeEpoch, remaining, inadmissibleFilter, now);
                metrics.claimRound(System.nanoTime() - claimStartNanos, claimed.size());
                if (claimed.isEmpty()) {
                    continue;
                }
                // What was claimed counts against the budget EVEN when admit returns losers: deliberate —
                // simplicity over utilisation (admission churn is rare by construction and self-limited
                // to one lap)
                remaining -= claimed.size();
                totalClaimed += claimed.size();
                List<WorkQueue.ClaimedWork> admitted = admit(claimed, definitions, admission, now);
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
     * How long one step of the tick may keep issuing statements: a quarter of {@code node-lease-ttl},
     * on monotonic time. The heartbeat runs ONCE per tick, before the firing and the claim, and a step
     * that ran on towards the TTL would let the NODE's lease expire mid-tick — a lease is failure
     * detection (DDIA), and a node reaped while alive is the failure mode this bounds.
     *
     * <p>One budget PER STEP (reap, firing, claim), not one for the whole tick, by decision: each step
     * is guaranteed its own progress — a heavy reclaim after a mass death never starves the claim of
     * the same tick. The price is that the three quarters add up, and a tick where all three run out
     * at once overruns the TTL; the lease floor's Javadoc in the auto-configuration reasons about
     * that margin explicitly and accepts it, because only a node whose every step is saturated pays it.
     */
    private record TickBudget(long startNanos, long budgetNanos) {

        static TickBudget start(Duration nodeLeaseTtl) {
            return new TickBudget(System.nanoTime(), nodeLeaseTtl.toNanos() / 4);
        }

        boolean spent() {
            return System.nanoTime() - startNanos >= budgetNanos;
        }
    }

    /**
     * The resolution of the admission race: the inadmissible list is a snapshot from the start of the
     * round — a cap may have been partial (headroom 2, the round brought 5) and the rate limit is
     * charged NOW, all-or-nothing per job ({@code RateLimitStore.charge}: charge what was claimed, never
     * deliver without a token — the one unacceptable violation).
     *
     * <p>A loser goes back to the queue with the SAME attempt (nothing ran, the budget is intact) and
     * {@code visible_at = now} — the next round of this or another node decides; the churn is bounded to
     * one round per guard flip and is counted ({@code mohs.claim.requeued}).
     */
    private List<WorkQueue.ClaimedWork> admit(List<WorkQueue.ClaimedWork> claimed, Map<JobKey, StoredJob> definitions,
            Admission admission, Instant now) {
        Map<JobKey, List<WorkQueue.ClaimedWork>> byJob = new LinkedHashMap<>();
        for (WorkQueue.ClaimedWork work : claimed) {
            byJob.computeIfAbsent(work.jobKey(), _ -> new ArrayList<>()).add(work);
        }
        List<WorkQueue.ClaimedWork> admitted = new ArrayList<>(claimed.size());
        List<WorkQueue.Requeue> losers = new ArrayList<>();
        for (Map.Entry<JobKey, List<WorkQueue.ClaimedWork>> entry : byJob.entrySet()) {
            JobKey jobKey = entry.getKey();
            List<WorkQueue.ClaimedWork> ofJob = entry.getValue();
            // A fresh query on a miss (storedJobFor): the newborn goes through the SAME guards — the
            // window included; without it, it would run inside a closed window, the bug the starter's
            // end-to-end test caught. A genuinely removed one goes on to the dispatch, to fail with the
            // right message
            StoredJob stored = storedJobFor(jobKey, definitions);
            Admitted share = stored == null
                    ? Admitted.all(ofJob.size())
                    : admitFor(stored.definition(), ofJob.size(), admission, now);
            admitted.addAll(ofJob.subList(0, share.count()));
            admission.consume(jobKey, share.count());
            for (WorkQueue.ClaimedWork loser : ofJob.subList(share.count(), ofJob.size())) {
                losers.add(new WorkQueue.Requeue(loser.executionId(), nodeId, nodeEpoch, loser.attemptNumber(),
                        new WorkQueue.ReadyEntry(loser.executionId(), jobKey, Shards.of(loser.executionId()),
                                loser.priority(), loser.attemptNumber(), now)));
            }
            if (!share.reason().isEmpty()) {
                metrics.claimRequeued(share.reason(), ofJob.size() - share.count());
            }
        }
        if (!losers.isEmpty()) {
            workQueue.requeue(losers);
        }
        return admitted;
    }

    /** How much of the round a job took and — when not all of it — which guard blocked it (the label of {@code mohs.claim.requeued}). */
    private record Admitted(int count, String reason) {

        static Admitted all(int requested) {
            return new Admitted(requested, "");
        }
    }

    /**
     * The post-claim guards, in order: the window, the cap's headroom (derived from
     * {@code mohs_lease}) and the rate limit's all-or-nothing charge — whichever reduced last signs the
     * {@code reason}.
     *
     * <p>The window here is a second line of defence, not redundancy: the pre-claim filter is a snapshot
     * (and is DISCARDED above {@link #MAX_INADMISSIBLE_FILTER}) — a newborn job, or a round with no
     * filter, arrives here with this as the only barrier between the queue and a closed window.
     */
    private Admitted admitFor(JobDefinition definition, int requested, Admission admission, Instant now) {
        if (windowRegistry.excludes(definition.window(), now)) {
            return new Admitted(0, "window-closed");
        }
        int allowed = requested;
        String reason = "";
        if (!definition.allowConcurrentExecutions()) {
            int headroom = Math.max(0, definition.maxConcurrentExecutions() - admission.leaseCount(definition.key()));
            if (headroom < allowed) {
                allowed = headroom;
                reason = "concurrency-cap";
            }
        }
        String rateLimit = definition.rateLimit();
        if (rateLimit != null && allowed > 0) {
            int granted = Math.min(allowed, rateLimitStore.available(rateLimit, now));
            if (granted > 0 && !rateLimitStore.charge(rateLimit, granted, now)) {
                granted = 0; // another node took the balance between the two phases — the round returns everything for this job
            }
            if (granted < allowed) {
                allowed = granted;
                reason = "rate-limit";
            }
        }
        return new Admitted(allowed, reason);
    }

    /**
     * The lap's per-job guards: who is out of this lap, and the ownership counts that decide the cap's
     * headroom.
     *
     * <p>A class, not a record: the lap MUTATES the counts ({@link #consume}) and a record communicates
     * an immutable value (Effective Java 17 — the project's convention reserves records for value
     * objects). Confined to the tick's thread (JCIP 3.3).
     */
    private static final class Admission {

        private final Set<JobKey> inadmissible;
        private final Map<JobKey, Integer> leaseCounts;

        private Admission(Set<JobKey> inadmissible, Map<JobKey, Integer> leaseCounts) {
            this.inadmissible = inadmissible;
            this.leaseCounts = leaseCounts;
        }

        Set<JobKey> inadmissible() {
            return inadmissible;
        }

        int leaseCount(JobKey jobKey) {
            return leaseCounts.getOrDefault(jobKey, 0);
        }

        /**
         * Deducts from the headroom what this lap has just admitted — the snapshot is ONE per lap, but
         * the lap has N statements: without the deduction, every probe would re-grant the same headroom
         * and a cap of 1 would admit 1 per shard (the loser returned with {@code visible_at = now} in a
         * shard ahead of the cursor would be re-admitted on the SAME pass). With it, the over-admission
         * bound stays "nodes x 1 lap", which is what was promised.
         */
        void consume(JobKey jobKey, int count) {
            if (count > 0) {
                leaseCounts.merge(jobKey, count, Integer::sum);
            }
        }

        static Admission compute(Map<JobKey, StoredJob> definitions, ExecutionWindowRegistry windowRegistry,
                RateLimitStore rateLimitStore, LeaseStore leaseStore, Instant now) {
            Set<JobKey> inadmissible = new HashSet<>();
            List<JobKey> capped = new ArrayList<>();
            for (StoredJob stored : definitions.values()) {
                JobDefinition definition = stored.definition();
                if (windowRegistry.excludes(definition.window(), now)) {
                    inadmissible.add(definition.key());
                    continue;
                }
                if (!definition.allowConcurrentExecutions()) {
                    capped.add(definition.key());
                }
                String rateLimit = definition.rateLimit();
                if (rateLimit != null && rateLimitStore.available(rateLimit, now) <= 0) {
                    inadmissible.add(definition.key());
                }
            }
            // A mutable copy: the lap deducts what it admits ({@link #consume})
            Map<JobKey, Integer> leaseCounts = new HashMap<>(capped.isEmpty() ? Map.of() : leaseStore.countByJob(capped));
            for (JobKey jobKey : capped) {
                StoredJob stored = definitions.get(jobKey);
                if (stored != null
                        && leaseCounts.getOrDefault(jobKey, 0) >= stored.definition().maxConcurrentExecutions()) {
                    inadmissible.add(jobKey);
                }
            }
            return new Admission(inadmissible, leaseCounts);
        }
    }

    /**
     * ONE payload-plus-header query per round, not one per execution. A failure of the query is
     * infrastructure, never a verdict on the executions: the batch keeps its lease up and another node's
     * reaper returns it if this one dies — a hiccup never becomes an immediate TERMINAL failure.
     *
     * @return {@code false} if the query failed — the caller ends the tick's rounds (the already claimed
     *         batch is left to the reaper)
     */
    private boolean dispatchClaimedBatch(List<WorkQueue.ClaimedWork> claimed, Map<JobKey, StoredJob> definitions,
            Instant claimInstant) {
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
                        new Dispatcher.Grant(nodeId, nodeEpoch, work.attemptNumber(), claimInstant));
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
    private void submitDispatch(WorkQueue.ClaimedWork work, HistoryStore.PayloadBatch payloads,
            Map<JobKey, StoredJob> definitions, Instant claimInstant) {
        ExecutionId id = work.executionId();
        HistoryStore.PayloadRow row = payloads.rows().get(id);
        Dispatcher.Grant grant = new Dispatcher.Grant(nodeId, nodeEpoch, work.attemptNumber(), claimInstant);
        // A fresh query on a miss (storedJobFor) — without it, a newborn job would die a terminal FAILED
        // as if it had been removed
        StoredJob storedJob = storedJobFor(work.jobKey(), definitions);
        if (storedJob == null) {
            failBeforeDispatchGuarded(executionFor(work, row, claimInstant), null, new IllegalStateException(
                    "job definition for " + work.jobKey() + " was removed after this execution was claimed (e.g. Mohs.remove between claim and dispatch)"), grant);
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
            log.warn("runner executor rejected execution {} — already leased, it will stand until a reaper reclaims it",
                    id, e);
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
            return new Execution(work.executionId(), work.jobKey(), ExecutionState.RUNNING, claimInstant, claimInstant,
                    List.of(), "unknown", Priority.NORMAL, null, null, nodeId);
        }
        HistoryStore.ExecutionHead head = row.head();
        return new Execution(work.executionId(), work.jobKey(), ExecutionState.RUNNING, head.scheduledAt(), claimInstant,
                List.of(), head.actor(), Priority.fromValue(head.priority()), null, head.correlationId(), nodeId);
    }

    /**
     * The "due trigger to acquisition" step — BEFORE the claim, on the same tick, so a freshly
     * materialised occurrence is already claimable without waiting for the next poll.
     *
     * <p>{@link FiringPlanner} decides what the job's misfire policy fires; {@link TriggerFirer#fire} is
     * the transactional CAS that resolves the race between nodes — losing is routine, not an error. One
     * job's failure does not take down the sweep of the others.
     */
    private FiringOutcome fireDueTriggers() {
        Instant now = clock.instant();
        List<StoredJob> due = jobStore.findDueRecurring(now, FIRE_LIMIT);
        // The same budget as the claim laps: each CAS is bounded by the tick's statement ceiling, but
        // up to FIRE_LIMIT of them in a row would still outlast the node's lease when a host
        // transaction holds a handful of definition rows. What does not fire now fires next tick
        TickBudget budget = TickBudget.start(settings.nodeLeaseTtl());
        Instant rearmedAt = null;
        int fired = 0;
        for (StoredJob job : due) {
            if (fired > 0 && budget.spent()) {
                log.warn("firing sweep stopped after {} of {} due trigger(s) — the tick's budget is spent, "
                        + "the rest fire next tick", fired, due.size());
                break;
            }
            fired++;
            JobDefinition definition = job.definition();
            try {
                // Inside the try: a contract violated by a custom store fails ONLY this job, not the sweep nor the tick's claim
                Instant observed = Objects.requireNonNull(job.nextFireAt(), "findDueRecurring returned an unarmed trigger");
                rearmedAt = earlierFutureFire(rearmedAt, fireTrigger(definition, observed, now), now);
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
     * {@link #earliestArmedFire}).
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
    private @Nullable Instant fireTrigger(JobDefinition definition, Instant observed, Instant now) {
        FiringPlanner.Plan plan = firingPlanner.plan(definition.schedule(), definition.misfire(), observed, now);
        List<Execution> occurrences = plan.occurrences().stream()
                .map(occurrenceAt -> newOccurrence(definition.key(), occurrenceAt))
                .toList();
        if (!triggerFirer.fire(definition.key(), observed, plan.nextFireAt(), occurrences, emptyPayload(), now)) {
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
     * The empty payload of scheduler occurrences — a concrete class on purpose, never {@code Map.of()}:
     * {@code payload_type} persists the exact class, and reading it back needs a type Jackson can
     * instantiate again.
     */
    private static LinkedHashMap<String, Object> emptyPayload() {
        return new LinkedHashMap<>();
    }

    /**
     * An occurrence materialised by the trigger: {@code scheduled_at} is the occurrence's instant (the
     * firing's identity), {@code actor} distinguishes the scheduler's chain from manual firings, and the
     * payload is empty.
     */
    private static Execution newOccurrence(JobKey jobKey, Instant occurrenceAt) {
        return Execution.enqueued(ExecutionId.of(UUIDv7.randomUUIDString()), jobKey, occurrenceAt,
                Execution.SCHEDULER_ACTOR, Priority.NORMAL);
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
            if (attempt.needsTimeoutSignal()) {
                log.warn("execution {} exceeded its job timeout {} — cancellation signalled and the handler interrupted; "
                        + "the outcome follows when the handler stops", id, attempt.timeout);
                attempt.signal.requestCancellation(CancellationSignal.Reason.TIMEOUT, true);
            }
        });
    }

    /**
     * Observes {@code POST /executions/{id}/cancel} — it reads the {@code cancel_requested} flag of its
     * own in-flight work in a batch (on the ownership, and only when there is in-flight work) and raises
     * {@code MANUAL} as a pure flag, without an interrupt: cancellation is cooperative by contract.
     *
     * <p>Effective staleness is at most one loop interval (ceiling
     * {@code min(max-poll-interval, node-lease-ttl/3)}) plus the tick's duration. Active in PAUSED and
     * DRAINING too.
     */
    private void pollCancelRequests() {
        List<ExecutionId> unsignalled = inFlightAttempts.entrySet().stream()
                .filter(entry -> !entry.getValue().signal.cancellationRequested())
                .map(Map.Entry::getKey)
                .toList();
        if (unsignalled.isEmpty()) {
            return;
        }
        for (ExecutionId id : leaseStore.findCancelRequested(unsignalled)) {
            InFlightAttempt attempt = inFlightAttempts.get(id);
            if (attempt != null) {
                log.info("execution {} has a standing cancel request — cooperative cancellation signalled to the handler", id);
                attempt.signal.requestCancellation(CancellationSignal.Reason.MANUAL, false);
            }
        }
    }

    /**
     * The Watchdog Bound: an execution whose monotonic runtime passed
     * {@code mohs.engine.watchdog-timeout} has its ownership explicitly RELEASED
     * ({@link Dispatcher#abandonOwnership} — a synthetic attempt through the retry budget, fenced by
     * this incarnation's ownership).
     *
     * <p>It MARKS the incarnation rather than removing it: the zombie stays in the map so the drain
     * escalation and the cancel poll can still reach it. A failed release does not mark — the next tick
     * tries again.
     */
    private void signalWatchdogOverruns() {
        inFlightAttempts.forEach((id, attempt) -> {
            if (!attempt.ownershipReleased && watchdogBoundExceeded(attempt.submittedNanos)) {
                releaseOverrunOwnership(id, attempt);
            }
        });
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
        return bound != null && System.nanoTime() - startedNanos >= bound.toNanos();
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

        InFlightAttempt(@Nullable Duration timeout, Execution execution, JobDefinition definition, Dispatcher.Grant grant) {
            this.timeout = timeout;
            this.execution = execution;
            this.definition = definition;
            this.grant = grant;
        }

        /** The tick sweep's trigger: it has a timeout of its own, the handler has been running longer than that, and the signal has not been raised yet. */
        boolean needsTimeoutSignal() {
            return timeout != null && !signal.cancellationRequested() && signal.handlerRunningLongerThan(timeout);
        }
    }

    /**
     * A terminal failure decided while still on the tick's thread — a removed definition or a runner with
     * no match fails only this execution, never the tick: if the failure write itself throws, the rest of
     * the batch still needs dispatching — log it and leave the ownership standing for a reaper.
     */
    private void failBeforeDispatchGuarded(Execution execution, @Nullable JobDefinition definition,
            IllegalStateException error, Dispatcher.Grant grant) {
        try {
            dispatcher.failBeforeDispatch(execution, definition, error, grant);
        } catch (RuntimeException e) {
            log.warn("could not record the terminal failure of execution {} ({}) — its lease stands until a reaper reclaims it",
                    execution.id(), error.getMessage(), e);
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
    private void failUnreadablePayload(Execution execution, @Nullable JobDefinition definition, RuntimeException cause,
            Dispatcher.Grant grant) {
        String message = "payload could not be read: " + Objects.requireNonNullElse(cause.getMessage(), cause.toString());
        failBeforeDispatchGuarded(execution, definition, new IllegalStateException(message, cause), grant);
    }

    /** Package-private — only {@code EngineTest} uses this, to confirm the node identity behind the heartbeat and the claim. */
    String nodeId() {
        return nodeId;
    }
}
