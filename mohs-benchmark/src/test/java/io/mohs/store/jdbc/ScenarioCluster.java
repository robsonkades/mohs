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
package io.mohs.store.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

import tools.jackson.databind.json.JsonMapper;

import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.store.jdbc.delegate.JdbcDelegate;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.MohsRunner;
import io.mohs.engine.Dispatcher;
import io.mohs.engine.Engine;
import io.mohs.engine.EngineMetrics;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.ExecutionWindowRegistry;
import io.mohs.engine.HandlerRegistry;
import io.mohs.engine.HistoryStore;
import io.mohs.engine.JobHandler;
import io.mohs.engine.BatchCompletionCallbacks;
import io.mohs.engine.CompletionBatcher;
import io.mohs.engine.MohsExecutors;
import io.mohs.engine.MohsImpl;
import io.mohs.engine.RunnerRegistry;
import io.mohs.engine.Shards;
import io.mohs.engine.WorkQueue;

/**
 * A cluster of N nodes inside ONE JVM, against a real database in a container — PostgreSQL by
 * default, or whatever {@link ScenarioBackend} the run selected.
 *
 * <p>Each {@link Engine} has its own {@code nodeId}, epoch, lease and tick loop — from the
 * database's point of view they are N nodes, and it is the database that arbitrates claim,
 * ownership and sharding. That is the fidelity correctness scenarios need, and the one the bench
 * scripts ({@code chaos-recovery.ps1}) do not give for free: here the test observes what each
 * handler saw, in memory, without inferring it from logs.
 *
 * <p>The wiring matches {@code MohsAutoConfiguration} — group commit on with the same 256/5ms,
 * event executor at the same ceiling of 16, the same shutdown order (engine stops, batcher drains
 * afterwards). That is not fussiness: a verdict about lost work drawn from a wiring nobody runs in
 * production is not admissible as release evidence.
 *
 * <p>The TWO remaining divergences, declared:
 * <ul>
 *   <li><b>Process death</b> (kill -9, freeze) is not expressible — a node here dies through
 *       {@code stop()}, not with the carrier ripped out from under it. That stays with
 *       {@code chaos-recovery.ps1}, which exists for exactly this reason.</li>
 *   <li><b>No connection pool</b>: {@code PostgresTestSupport} hands out a
 *       {@code PGSimpleDataSource}, so every statement pays TCP + auth. Every latency and
 *       throughput figure these scenarios print is diagnostic, NEVER a release number — production
 *       uses HikariCP with {@code maximumPoolSize} 100+, and the recorded baseline is what speaks
 *       about performance.</li>
 * </ul>
 */
final class ScenarioCluster implements AutoCloseable {

    /** A single offer of 100k would blow past the driver's parameter limit. */
    private static final int OFFER_BATCH_SIZE = 1_000;

    /** How often {@link #awaitUntil} re-reads the database while waiting. */
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    /**
     * The dispatch-to-event-publication ratio the production defaults encode (64 dispatch to 16
     * events).
     *
     * <p>Holding the LITERAL 16 with a larger dispatch would mean running the wiring PERFORMANCE.md
     * advises against — and {@code ExecutionEventPublisher} DROPS the event when it saturates,
     * delivery being best-effort by contract, so the bench would go red because of its own tuning
     * rather than a defect in the product.
     */
    private static final int DISPATCH_TO_EVENT_RATIO = 4;

    /**
     * One node: the engine, the handler registry only it can see (which is what makes a rolling
     * update with a missing handler expressible) and the resources the node owns and must return
     * on {@code close}.
     */
    record Node(Engine engine, HandlerRegistry handlers, RunnerRegistry runners, CompletionBatcher batcher,
            SimpleAsyncTaskExecutor events, List<ExecutionListener> listeners) {
    }

    private final DataSource dataSource;
    private final Clock clock;
    private final JdbcTemplate jdbcTemplate;
    private final JdbcDelegate delegate;
    private final JdbcJobStore jobStore;
    private final JdbcHistoryStore historyStore;
    private final JdbcWorkQueue workQueue;
    private final JdbcLeaseStore leaseStore;
    private final JdbcRateLimitStore rateLimitStore;
    private final JdbcNodeStore nodeStore;
    private final JdbcBatchStore batchStore;
    private final List<Node> nodes = new ArrayList<>();

    /** The reference dialect — the shape every scenario had hardcoded before there was a choice. */
    ScenarioCluster(DataSource dataSource, Clock clock) {
        this(dataSource, clock, new PostgresJdbcDelegate());
    }

    ScenarioCluster(DataSource dataSource, Clock clock, JdbcDelegate delegate) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.batchStore = new JdbcBatchStore(dataSource, clock, delegate);
        this.jobStore = new JdbcJobStore(dataSource, clock, delegate);
        this.historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), delegate);
        this.workQueue = new JdbcWorkQueue(dataSource, delegate, batchStore);
        this.leaseStore = new JdbcLeaseStore(dataSource, delegate, batchStore);
        this.rateLimitStore = new JdbcRateLimitStore(dataSource, clock, delegate);
        this.nodeStore = new JdbcNodeStore(dataSource, delegate);
    }

    JdbcTemplate jdbc() {
        return jdbcTemplate;
    }

    JdbcJobStore jobs() {
        return jobStore;
    }

    JdbcRateLimitStore rateLimits() {
        return rateLimitStore;
    }

    JdbcHistoryStore history() {
        return historyStore;
    }

    List<Node> nodes() {
        return List.copyOf(nodes);
    }

    /** Registers an on-demand job definition; {@code policy} receives the spec to point at a runner, a limit or a cap. */
    void defineJob(String jobKey, Consumer<PolicySpec> policy) {
        jobStore.upsert(JobDefinition.of(jobKey, ScenarioCluster.class, spec -> policy.accept(spec.onDemand())));
    }

    /**
     * A RECURRING job — what brings trigger materialisation into the scenario, and with it the race
     * between nodes over the same occurrence.
     *
     * <p>{@code retries(0)} is declared out of the same discipline as elsewhere: the budget is not
     * this experiment's variable (a retry reincarnates the SAME {@code mohs_execution} row, so it
     * would not even change the counts), and declaring it stops a future revision of the default
     * from moving what the bench measures without anyone deciding to.
     */
    void defineRecurring(String jobKey, Duration every) {
        jobStore.upsert(JobDefinition.of(jobKey, ScenarioCluster.class, spec -> spec.every(every).retries(0)));
    }

    /**
     * A new node, still stopped. {@code handlers} starts empty: the caller registers only what THIS
     * node knows how to do — two nodes with different registries is literally the rolling update.
     */
    Node addNode(EngineSettings settings, List<ExecutionListener> listeners) {
        HandlerRegistry handlers = new HandlerRegistry();
        EngineMetrics metrics = new EngineMetrics(new SimpleMeterRegistry());
        RunnerRegistry runners = new RunnerRegistry(
                List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(settings.dispatchConcurrency()).build()));
        // Group commit ON, with the same N/T as MohsAutoConfiguration: without it completion becomes
        // synchronous, `Dispatcher#completionInTransit` returns a constant false (one of the THREE
        // guards in `Engine#reconcileOwnStrayLeases`) and the "completion committed x lease
        // released" window — the one from the incident — ceases to exist. A shutdown-loss verdict
        // drawn without this would be about a wiring no client runs.
        CompletionBatcher batcher = new CompletionBatcher(leaseStore, jobStore, 256, Duration.ofMillis(5));
        batcher.start();
        // Events scale WITH dispatch (PERFORMANCE.md: "under high throughput, 16 becomes a queue;
        // raise it along with dispatch") — the production default is the 4:1 ratio of 64/16, not
        // the literal 16
        SimpleAsyncTaskExecutor events = MohsExecutors.ioBoundExecutor("mohs-events-scenario",
                Math.max(1, settings.dispatchConcurrency() / DISPATCH_TO_EVENT_RATIO));
        Dispatcher dispatcher = new Dispatcher(leaseStore, jobStore, handlers, clock, List.of(), listeners,
                events, metrics, batcher);
        Engine engine = new Engine(workQueue, dispatcher, historyStore, leaseStore, jobStore, nodeStore,
                new JdbcTriggerFirer(dataSource, historyStore, workQueue, delegate), new ExecutionWindowRegistry(List.of()),
                rateLimitStore, clock, settings, runners, metrics);
        Node node = new Node(engine, handlers, runners, batcher, events, List.copyOf(listeners));
        nodes.add(node);
        return node;
    }

    /**
     * The public facade bound to THIS node — the write path a real application uses
     * ({@code Mohs.batch}, {@code Mohs.schedule}), with the local hand-off pointing at its loop.
     *
     * <p>A scenario seeding through {@link #seedReady} measures the engine; this one measures the
     * engine PLUS the entry path, which is where a batch is born.
     */
    MohsImpl facadeFor(Node node, BatchCompletionCallbacks callbacks) {
        // BatchCompletionCallbacks IS an ExecutionListener: left out of the node's listener list it
        // is never notified, and an onCompletion that silently never fires would make the bench
        // report a defect that does not exist. Failing here is cheaper than the investigation.
        if (!node.listeners().contains(callbacks)) {
            throw new IllegalArgumentException("callbacks must have been passed to addNode(...) as a listener — "
                    + "BatchCompletionCallbacks is an ExecutionListener, and outside the node's listener list "
                    + "onCompletion never fires");
        }
        return new MohsImpl(jobStore, workQueue, historyStore, leaseStore, new JdbcStoreTransactions(dataSource),
                nodeStore, rateLimitStore, node.handlers(), clock, node.engine(), batchStore,
                callbacks, node.runners(), node.engine()::signalWorkScheduled, List.of(), Runnable::run);
    }

    /** Registers the same handler on every node created so far. */
    void registerEverywhere(String jobKey, JobHandler handler) {
        nodes.forEach(node -> node.handlers().register(JobKey.of(jobKey), handler));
    }

    void startAll() {
        nodes.forEach(node -> node.engine().start());
    }

    /**
     * Seeds {@code count} ready executions of the job — the enqueue unit (a history row plus a
     * queue entry), with the shard DERIVED from the id as every real writer does: seeding
     * everything into shard 0 would measure a cluster that does not exist.
     */
    List<ExecutionId> seedReady(String jobKey, int count, int priority) {
        JobKey key = JobKey.of(jobKey);
        Instant now = clock.instant();
        List<ExecutionId> ids = new ArrayList<>(count);
        List<HistoryStore.NewExecution> rows = new ArrayList<>(count);
        List<WorkQueue.ReadyEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ExecutionId id = ExecutionId.of(UUIDv7.randomUUID().toString());
            int shard = Shards.of(id);
            ids.add(id);
            rows.add(new HistoryStore.NewExecution(id, key, shard, priority, now, now, "scenario", null, null, ""));
            entries.add(new WorkQueue.ReadyEntry(id, key, shard, priority, 1, now));
        }
        for (int from = 0; from < count; from += OFFER_BATCH_SIZE) {
            int to = Math.min(from + OFFER_BATCH_SIZE, count);
            historyStore.record(rows.subList(from, to));
            workQueue.offer(entries.subList(from, to));
        }
        return ids;
    }

    /** Waits until the condition holds or the ceiling expires; returns whether it held (the scenario decides whether that is a failure). */
    static boolean awaitUntil(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(POLL_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("scenario interrupted while waiting", e);
            }
        }
        return condition.getAsBoolean();
    }

    /** Queue and ownership empty at the same time: every backlog scenario's definition of "drained". */
    boolean isDrained() {
        return countReady() == 0 && countLease() == 0;
    }

    /**
     * How far behind the queue's head is — the number a burst criterion is written in, and the one
     * the size of the backlog cannot give: 10k entries one second old and 10k entries a minute old
     * are the same count and a different incident.
     *
     * <p>Read through the delegate rather than as a raw {@code Timestamp}: PostgreSQL stores
     * {@code TIMESTAMPTZ} and the other two a zoneless column, and reading the second as the first
     * shifts the answer by the JVM's offset — a bench that reports a lag of three hours because of
     * a time zone is worse than no bench.
     */
    Duration oldestQueuedAge() {
        Instant oldest = jdbcTemplate.query("SELECT MIN(visible_at) AS oldest FROM mohs_ready",
                rs -> rs.next() ? delegate.readSplitTimestamp(rs, "oldest") : null);
        return oldest == null ? Duration.ZERO : Duration.between(oldest, clock.instant());
    }

    /**
     * How many DISTINCT jobs under a prefix have an execution — one per trigger materialised, never
     * one per firing. A recurring trigger on a short interval re-fires inside any observation window
     * worth measuring, and counting rows would let half the triggers satisfy a criterion written
     * about all of them.
     */
    int countMaterialisedJobs(String jobKeyPrefix) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT job_key) FROM mohs_execution WHERE job_key LIKE ?", Integer.class,
                jobKeyPrefix + "%");
        return count == null ? 0 : count;
    }

    int countReady() {
        return count("SELECT count(*) FROM mohs_ready");
    }

    int countLease() {
        return count("SELECT count(*) FROM mohs_lease");
    }

    int countAttempts() {
        return count("SELECT count(*) FROM mohs_attempt");
    }

    int countTerminal(String state) {
        return count("SELECT count(*) FROM mohs_execution WHERE state = ?", state);
    }

    int countExecutionsOf(String jobKey) {
        return count("SELECT count(*) FROM mohs_execution WHERE job_key = ?", jobKey);
    }

    /**
     * How the engine classified each failed attempt — where {@code NO_HANDLER} and an ownership
     * reclaim appear by name, instead of collapsing into an unexplained FAILED count.
     */
    Map<String, Integer> failureKinds() {
        // By type AND message: the engine uses IllegalStateException for THREE distinct causes —
        // missing handler, cancellation by shutdown, and a dead node's lease. Grouping by type alone
        // collapses all three and empties any assertion about cause attribution.
        //
        // CONCAT, and the same expression repeated in GROUP BY, because this is the one query in the
        // harness that formats rather than counts and so has to hold on every backend a run can
        // select: `||` is logical OR in MySQL, and MySQL's only_full_group_by rejects a selected
        // expression that is not itself grouped.
        return jdbcTemplate.query("""
                SELECT CONCAT(error_type, ': ', LEFT(error, 60)) AS kind, count(*) AS total
                  FROM mohs_attempt
                 WHERE error_type IS NOT NULL
                 GROUP BY CONCAT(error_type, ': ', LEFT(error, 60))
                 ORDER BY count(*) DESC
                """, rs -> {
            Map<String, Integer> kinds = new LinkedHashMap<>();
            while (rs.next()) {
                kinds.put(rs.getString("kind"), rs.getInt("total"));
            }
            return kinds;
        });
    }

    private int count(String sql, Object... args) {
        Integer value = jdbcTemplate.queryForObject(sql, Integer.class, args);
        return value == null ? 0 : value;
    }

    /**
     * Production order ({@code MohsAutoConfiguration}): the engine stops first, and only then does
     * the batcher drain what the last handlers submitted — closing the batcher first would discard
     * completions still in transit.
     */
    @Override
    public void close() {
        nodes.forEach(node -> {
            try {
                node.engine().stop(Duration.ofSeconds(10));
            } catch (IllegalStateException _) {
                // Already stopped — a scenario that kills a node on purpose comes through here
            }
        });
        nodes.forEach(node -> {
            node.batcher().close();
            node.events().close();
            node.runners().close();
        });
        nodes.clear();
    }
}
