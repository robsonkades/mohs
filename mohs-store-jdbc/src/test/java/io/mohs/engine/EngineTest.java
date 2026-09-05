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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.h2.jdbcx.JdbcDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import tools.jackson.databind.json.JsonMapper;

import io.mohs.core.EngineState;
import io.mohs.core.definition.JobDefinition;
import io.mohs.core.definition.PolicySpec;
import io.mohs.core.event.AttemptFailed;
import io.mohs.core.event.Cancelled;
import io.mohs.core.event.ExecutionListener;
import io.mohs.core.event.Failed;
import io.mohs.core.event.RetryScheduled;
import io.mohs.core.event.Succeeded;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.MohsRunner;
import io.mohs.store.jdbc.JdbcBatchStore;
import io.mohs.store.jdbc.JdbcHistoryStore;
import io.mohs.store.jdbc.JdbcJobStore;
import io.mohs.store.jdbc.JdbcLeaseStore;
import io.mohs.store.jdbc.JdbcNodeStore;
import io.mohs.store.jdbc.JdbcRateLimitStore;
import io.mohs.store.jdbc.JdbcTimestamps;
import io.mohs.store.jdbc.JdbcTriggerFirer;
import io.mohs.store.jdbc.JdbcWorkQueue;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class EngineTest {

    record Handler() {
    }

    private static final Instant NOW = Instant.parse("2026-08-14T12:00:00Z");
    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final int BATCH_SIZE = 10;

    private DataSource dataSource;
    private MutableClock clock;
    private JdbcTemplate rawJdbcTemplate;
    private JdbcJobStore jobStore;
    private JdbcHistoryStore historyStore;
    private JdbcWorkQueue workQueue;
    private JdbcLeaseStore leaseStore;
    private JdbcRateLimitStore rateLimitStore;
    private JdbcNodeStore nodeStore;
    private HandlerRegistry handlerRegistry;

    private final ch.qos.logback.classic.Logger engineLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Engine.class);

    @BeforeEach
    void setUp() {
        dataSource = freshH2DataSource();
        clock = new MutableClock(NOW, ZoneId.of("UTC"));
        rawJdbcTemplate = new JdbcTemplate(dataSource);
        jobStore = new JdbcJobStore(dataSource, clock, new H2JdbcDelegate());
        JdbcBatchStore batchStore = new JdbcBatchStore(dataSource, clock, new H2JdbcDelegate());
        historyStore = new JdbcHistoryStore(dataSource, JsonMapper.builder().build(), new H2JdbcDelegate());
        workQueue = new JdbcWorkQueue(dataSource, new H2JdbcDelegate(), batchStore);
        leaseStore = new JdbcLeaseStore(dataSource, new H2JdbcDelegate(), batchStore);
        rateLimitStore = new JdbcRateLimitStore(dataSource, clock, new H2JdbcDelegate());
        nodeStore = new JdbcNodeStore(dataSource, new H2JdbcDelegate());
        handlerRegistry = new HandlerRegistry();
    }

    private static DataSource freshH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:engine-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(h2);
        return h2;
    }

    private Engine newEngine(NodeStore nodeStoreOverride, List<ExecutionListener> listeners) {
        return newEngine(nodeStoreOverride, listeners, defaultRunnerRegistry());
    }

    private Engine newEngine(NodeStore nodeStoreOverride, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry) {
        return newEngine(nodeStoreOverride, listeners, runnerRegistry, new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));
    }

    private Engine newEngine(NodeStore nodeStoreOverride, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(workQueue, leaseStore, historyStore, nodeStoreOverride, listeners, runnerRegistry, settings);
    }

    /**
     * The common assembly: a real dispatcher, trigger firer and tick scheduler over the fixture's ports —
     * the overrides simulate a failure at a single point (queue, ownership, history or node) without
     * touching the rest.
     */
    private Engine assembleEngine(WorkQueue workQueueOverride, LeaseStore leaseStoreOverride, HistoryStore historyStoreOverride,
            NodeStore nodeStoreOverride, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(jobStore, workQueueOverride, leaseStoreOverride, historyStoreOverride, nodeStoreOverride,
                listeners, runnerRegistry, settings);
    }

    private Engine assembleEngine(JobStore jobStoreOverride, WorkQueue workQueueOverride, LeaseStore leaseStoreOverride,
            HistoryStore historyStoreOverride, NodeStore nodeStoreOverride, List<ExecutionListener> listeners,
            RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(jobStoreOverride, workQueueOverride, leaseStoreOverride, historyStoreOverride,
                nodeStoreOverride, new ExecutionWindowRegistry(List.of()), listeners, runnerRegistry, settings);
    }

    private Engine assembleEngine(JobStore jobStoreOverride, WorkQueue workQueueOverride, LeaseStore leaseStoreOverride,
            HistoryStore historyStoreOverride, NodeStore nodeStoreOverride, ExecutionWindowRegistry windowRegistry,
            List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings) {
        return assembleEngine(jobStoreOverride, workQueueOverride, leaseStoreOverride, historyStoreOverride,
                nodeStoreOverride, windowRegistry, listeners, runnerRegistry, settings, clock);
    }

    /**
     * An explicit {@code engineClock}, because the loop's cadence is the one behaviour
     * {@link MutableClock} cannot exercise: the sleep is real, the clock is frozen, and a PUNCTUALITY
     * test needs both on the same scale (see
     * {@code recurringJobFiresOnItsOwnIntervalNotOnTheBackoffPoints}).
     *
     * <p>It swaps the ENGINE's clock, not the fixture's: the {@code batchStore} inside
     * {@code leaseStore}/{@code workQueue} and the {@code rateLimitStore} stay on the frozen
     * {@link MutableClock}. Harmless while neither batches nor rate limits enter the test — anyone
     * copying this overload into a scenario that uses both must pass the same clock to the stores too.
     */
    private Engine assembleEngine(JobStore jobStoreOverride, WorkQueue workQueueOverride, LeaseStore leaseStoreOverride,
            HistoryStore historyStoreOverride, NodeStore nodeStoreOverride, ExecutionWindowRegistry windowRegistry,
            List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings, Clock engineClock) {
        return assembleEngine(jobStoreOverride, workQueueOverride, leaseStoreOverride, historyStoreOverride,
                nodeStoreOverride, windowRegistry, listeners, runnerRegistry, settings, engineClock,
                new SimpleMeterRegistry());
    }

    /** The overload with the registry in the caller's hands — for the tests that read a meter back. */
    private Engine assembleEngine(JobStore jobStoreOverride, WorkQueue workQueueOverride, LeaseStore leaseStoreOverride,
            HistoryStore historyStoreOverride, NodeStore nodeStoreOverride, ExecutionWindowRegistry windowRegistry,
            List<ExecutionListener> listeners, RunnerRegistry runnerRegistry, EngineSettings settings, Clock engineClock,
            MeterRegistry meterRegistry) {
        return assembleEngine(jobStoreOverride, workQueueOverride, leaseStoreOverride, historyStoreOverride, nodeStoreOverride,
                new JdbcTriggerFirer(dataSource, historyStore, workQueue, new H2JdbcDelegate()), windowRegistry, listeners,
                runnerRegistry, settings, engineClock, meterRegistry);
    }

    /** The overload with the trigger firer in the caller's hands — for the tests that slow the firing down. */
    private Engine assembleEngine(JobStore jobStoreOverride, WorkQueue workQueueOverride, LeaseStore leaseStoreOverride,
            HistoryStore historyStoreOverride, NodeStore nodeStoreOverride, TriggerFirer triggerFirer,
            ExecutionWindowRegistry windowRegistry, List<ExecutionListener> listeners, RunnerRegistry runnerRegistry,
            EngineSettings settings, Clock engineClock, MeterRegistry meterRegistry) {
        AsyncTaskExecutor eventExecutor = MohsExecutors.ioBoundExecutor("mohs-events-test", 16);
        EngineMetrics metrics = new EngineMetrics(meterRegistry);
        Dispatcher dispatcher = new Dispatcher(leaseStoreOverride, jobStoreOverride, handlerRegistry, engineClock, List.of(), listeners, eventExecutor, metrics);
        return new Engine(workQueueOverride, dispatcher, historyStoreOverride, leaseStoreOverride, jobStoreOverride, nodeStoreOverride,
                triggerFirer, windowRegistry, rateLimitStore, engineClock, settings, runnerRegistry, metrics);
    }

    private static RunnerRegistry defaultRunnerRegistry() {
        return new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build()));
    }

    /**
     * An engine whose queue and node store record a single trail of {@code tick}/{@code claim:N} — since
     * both run only on the tick's thread, the trail's order IS the tick's structure, and it proves how
     * many claim rounds happened INSIDE each one.
     */
    private Engine newEngineWithTickTrace(List<String> trace, CountingNodeStore counting, List<ExecutionListener> listeners, EngineSettings settings) {
        WorkQueue tracingQueue = new WorkQueue() {
            @Override
            public List<ClaimedWork> claim(int shard, String nodeId, long epoch, int limit, Collection<JobKey> inadmissible, Instant now) {
                List<ClaimedWork> claimed = workQueue.claim(shard, nodeId, epoch, limit, inadmissible, now);
                trace.add("claim:" + claimed.size());
                return claimed;
            }

            @Override
            public boolean hasVisibleWork(Collection<Integer> shards, Instant now) {
                boolean found = workQueue.hasVisibleWork(shards, now);
                trace.add("probe:" + found);
                return found;
            }

            @Override
            public long countVisible(Instant now) {
                return workQueue.countVisible(now);
            }

            @Override
            public void offer(List<ReadyEntry> entries) {
                workQueue.offer(entries);
            }

            @Override
            public int requeue(List<Requeue> orders) {
                return workQueue.requeue(orders);
            }

            @Override
            public boolean cancelQueued(ExecutionId id, Instant now) {
                return workQueue.cancelQueued(id, now);
            }

            @Override
            public boolean rearmForManualRetry(ExecutionId id, Instant now) {
                return workQueue.rearmForManualRetry(id, now);
            }
        };
        NodeStore tracingNodeStore = new NodeStore() {
            @Override
            public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
                trace.add("tick");
                counting.heartbeat(nodeId, state, epoch, at, expiresAt);
            }

            @Override
            public List<StoredNode> findAll() {
                return counting.findAll();
            }

            @Override
            public int deleteHeartbeatsBefore(Instant cutoff) {
                return counting.deleteHeartbeatsBefore(cutoff);
            }
        };
        return assembleEngine(tracingQueue, leaseStore, historyStore, tracingNodeStore, listeners, defaultRunnerRegistry(), settings);
    }

    /**
     * Complete windows of the trail: claims grouped by tick, discarding the window still open at the end.
     *
     * <p>Since the lap arrived, each statement probes ONE shard and the fixture seeds everything into
     * shard 0 — the empty probes ({@code claim:0}) are just the lap circling owned-but-empty shards. The
     * structure being asserted (how many batches per tick, the chaining, the early stop) is that of the
     * claims that found work, so the zeros leave the window.
     */
    private static List<List<Integer>> claimsPerTick(List<String> trace) {
        List<String> snapshot;
        synchronized (trace) {
            snapshot = List.copyOf(trace);
        }
        List<List<Integer>> ticks = new ArrayList<>();
        List<Integer> current = null;
        for (String entry : snapshot) {
            if (entry.equals("tick")) {
                current = new ArrayList<>();
                ticks.add(current);
            } else if (current != null && entry.startsWith("claim:")) {
                int claimed = Integer.parseInt(entry.substring("claim:".length()));
                if (claimed > 0) {
                    current.add(claimed);
                }
            }
        }
        if (!ticks.isEmpty()) {
            ticks.removeLast();
        }
        return ticks;
    }

    /** The RAW probes (empty ones included) of the tick — it pins the SELECT savings the empty-filter above would hide. */
    private static long rawClaimStatementsInTick(List<String> trace, int tickIndex) {
        return entriesInTick(trace, tickIndex, "claim:");
    }

    /** The idle gate's existence probes — the ONE-statement alternative to the whole lap. */
    private static long emptyGateProbesInTick(List<String> trace, int tickIndex) {
        return entriesInTick(trace, tickIndex, "probe:");
    }

    private static long entriesInTick(List<String> trace, int tickIndex, String prefix) {
        List<String> snapshot;
        synchronized (trace) {
            snapshot = List.copyOf(trace);
        }
        int tick = -1;
        long statements = 0;
        for (String entry : snapshot) {
            if (entry.equals("tick")) {
                tick++;
            } else if (tick == tickIndex && entry.startsWith(prefix)) {
                statements++;
            }
        }
        return statements;
    }

    private void seedEnqueuedExecution(String id, String jobKey, Object payload) {
        seedEnqueuedExecution(id, jobKey, payload, NOW.minusSeconds(1), null);
    }

    private void seedEnqueuedExecution(String id, String jobKey, Object payload, Instant scheduledAt) {
        seedEnqueuedExecution(id, jobKey, payload, scheduledAt, null);
    }

    /**
     * {@code retries(0)} declared, not inherited: half this fixture's tests measure the TERMINAL path
     * (timeout, watchdog, shutdown, missing handler), which is only reachable without a retry budget.
     * Whoever needs the path with a retry declares their own, as
     * {@link #aManuallyRearmedExecutionRunsOnceMoreAndFailsTerminally} does.
     */
    private void seedEnqueuedExecution(String id, String jobKey, Object payload, Instant scheduledAt, @Nullable String runner) {
        jobStore.upsert(JobDefinition.of(jobKey, Handler.class, spec -> {
            PolicySpec policySpec = spec.onDemand().retries(0);
            if (runner != null) {
                policySpec.runner(runner);
            }
        }));
        recordAndOffer(id, jobKey, payload, scheduledAt);
    }

    /** The enqueue unit without the upsert — for tests that register the definition themselves. */
    private void recordAndOffer(String id, String jobKey, Object payload, Instant scheduledAt) {
        recordAndOffer(id, jobKey, payload, scheduledAt, 0);
    }

    private void recordAndOffer(String id, String jobKey, Object payload, Instant scheduledAt, int shard) {
        historyStore.record(List.of(new HistoryStore.NewExecution(ExecutionId.of(id), JobKey.of(jobKey), shard, 20,
                scheduledAt, NOW, "test", null, null, payload)));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of(id), JobKey.of(jobKey), shard, 20, 1, scheduledAt)));
    }

    /** The derived read model's state: advisory plus lease plus queue — what the public API sees. */
    private ExecutionState stateOf(String id) {
        return historyStore.find(ExecutionId.of(id), clock.instant()).orElseThrow().state();
    }

    /** RUNNING derivado = posse viva ({@code mohs_lease}); ENQUEUED = entrada na fila. */
    private int leaseCount() {
        Integer count = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_lease", Integer.class);
        return count == null ? 0 : count;
    }

    private int readyCount() {
        Integer count = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_ready", Integer.class);
        return count == null ? 0 : count;
    }

    private int terminalCount(ExecutionState state) {
        Integer count = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_execution WHERE state = ?", Integer.class, state.name());
        return count == null ? 0 : count;
    }

    @Test
    void startClaimsAndDispatchesUntilSuccess() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * The worst failure mode this cycle fixed: the tick's seven steps sat under ONE try/catch, and the
     * heartbeat is the first — when a maintenance step blew up, the node stayed alive and RUNNING to its
     * peers, owning 1/n of the shards, and claimed NOTHING. {@code purgeStaleNodeRows} is the natural
     * candidate: it issues the same DELETE on every node on every tick, a SQL Server deadlock classic.
     *
     * <p>Without this test, the next refactor that regroups the tick under a single try returns to the
     * previous state with nothing to flag it — and the production symptom (a node heartbeating, idle,
     * with no visible error) is among the most expensive to diagnose.
     */
    @Test
    void aFailingMaintenanceStepDoesNotStopTheClaimInTheSameTick() throws Exception {
        jobStore.upsert(JobDefinition.of("poll", Handler.class, spec -> spec.every(Duration.ofSeconds(10))));
        handlerRegistry.register(JobKey.of("poll"), (payload, ctx) -> { });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        NodeStore purgeAlwaysFails = new NodeStore() {
            @Override
            public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
                nodeStore.heartbeat(nodeId, state, epoch, at, expiresAt);
            }

            @Override
            public List<StoredNode> findAll() {
                return nodeStore.findAll();
            }

            @Override
            public int deleteHeartbeatsBefore(Instant cutoff) {
                throw new DataAccessResourceFailureException("deadlock on mohs_nodes — every node deletes these rows");
            }
        };
        clock.advance(Duration.ofSeconds(15)); // the firing armed at upsert (NOW+10s) becomes due
        Engine engine = newEngine(purgeAlwaysFails, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS))
                    .as("failing maintenance must not steal the tick's fire and claim — the claim is why it exists")
                    .isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /** End to end: a due trigger materialises the occurrence within the tick (history plus queue), the SAME tick's claim takes it and the dispatch runs it — and the trigger advances along the series. */
    @Test
    void recurringJobFiresWhenDueAndAdvancesTheTrigger() throws Exception {
        jobStore.upsert(JobDefinition.of("poll", Handler.class, spec -> spec.every(Duration.ofSeconds(10))));
        handlerRegistry.register(JobKey.of("poll"), (payload, ctx) -> {
        });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        // The firing armed at upsert (NOW+10s) becomes due, 5s late — within the threshold, so it fires normally
        clock.advance(Duration.ofSeconds(15));
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        // Exactly one occurrence (the clock frozen at NOW+15s: the next one, NOW+20s, is not due yet)
        String actor = rawJdbcTemplate.queryForObject(
                "SELECT actor FROM mohs_execution WHERE job_key = 'poll'", String.class);
        assertThat(actor).isEqualTo(Execution.SCHEDULER_ACTOR);
        Instant scheduledAt = JdbcTimestamps.fromUtcLocalDateTime(rawJdbcTemplate.queryForObject(
                "SELECT scheduled_at FROM mohs_execution WHERE job_key = 'poll'", LocalDateTime.class));
        assertThat(scheduledAt).isEqualTo(NOW.plusSeconds(10)); // the occurrence's identity, not the insertion's instant
        Instant nextFireAt = JdbcTimestamps.fromUtcLocalDateTime(rawJdbcTemplate.queryForObject(
                "SELECT next_fire_at FROM mohs_job_definitions WHERE job_key = 'poll'", LocalDateTime.class));
        assertThat(nextFireAt).isEqualTo(NOW.plusSeconds(20));
    }

    /**
     * The regression guard: the loop must not sleep past a {@code next_fire_at} it knows about.
     *
     * <p>It reproduces the REPORTED symptom — the demo's {@code PT1S} job firing in pairs of ~0.4s and
     * ~1.6s, never at 1.0s — with the same configuration shape that produced it: a 25ms floor and a 2s
     * ceiling, with the backoff waking at {@code 25, 75, 175, 375, 775, 1575ms}. None of those points is
     * the round second: without the cap, an occurrence due at {@code +1000ms} waits until {@code 1575}
     * and the next comes out almost on time — the pair repeats.
     *
     * <p>Hence the assertion is about the INTERVAL between successive executions, not the total time: the
     * count was always right in the defect (one firing per second), what was wrong was the spacing. And
     * it is ONE-SIDED: in the defect, every ~1.6s interval comes paired with a ~0.4s one, so the floor
     * discriminates on its own — a ceiling would only convert a machine pause into a failure.
     *
     * <p>The floor is NOT immune to a slow machine: the stamp is taken INSIDE the handler, so a dispatch
     * delay inflates one interval and steals the same amount from the next. Measured under an
     * oversubscribed CPU: 0.83s. The 600ms is the margin between the defect (0.39-0.43s) and that
     * jitter. A known residue: a tick more than a second late makes {@code FiringPlanner} materialise the
     * overdue occurrences in the same firing — they come out in one batch and the interval goes to zero.
     *
     * <p>A REAL clock in this test (not the fixture's {@link MutableClock}): punctuality is precisely the
     * relationship between the schedule's time and the sleep's time — freezing either erases the
     * behaviour being measured.
     */
    @Test
    void recurringJobFiresOnItsOwnIntervalNotOnTheBackoffPoints() throws Exception {
        Clock realClock = Clock.systemUTC();
        JdbcJobStore punctualJobStore = new JdbcJobStore(dataSource, realClock, new H2JdbcDelegate());
        punctualJobStore.upsert(JobDefinition.of("punctual", Handler.class, spec -> spec.every(Duration.ofSeconds(1))));
        List<Instant> firedAt = new CopyOnWriteArrayList<>();
        CountDownLatch fired = new CountDownLatch(4);
        handlerRegistry.register(JobKey.of("punctual"), (payload, ctx) -> {
            firedAt.add(realClock.instant());
            fired.countDown();
        });
        Engine engine = assembleEngine(punctualJobStore, workQueue, leaseStore, historyStore, nodeStore,
                new ExecutionWindowRegistry(List.of()), List.of(), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofMillis(25), Duration.ofSeconds(2), BATCH_SIZE, 10, 1,
                        LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD), realClock);

        engine.start();
        try {
            assertThat(fired.await(10, TimeUnit.SECONDS))
                    .as("four occurrences of a PT1S job must fire inside a 10s budget")
                    .isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        List<Duration> gaps = new ArrayList<>();
        for (int i = 1; i < firedAt.size(); i++) {
            gaps.add(Duration.between(firedAt.get(i - 1), firedAt.get(i)));
        }
        assertThat(gaps)
                .as("successive occurrences of a PT1S job — never the 0,4s leg of the backoff-point pair")
                .isNotEmpty()
                .allSatisfy(gap -> assertThat(gap).isGreaterThanOrEqualTo(Duration.ofMillis(600)));
    }

    /**
     * The pairing the sleep cap depends on and that {@link EngineSleepTest} cannot reach: the sleep's
     * horizon must not see a trigger {@code findDueRecurring} would not fire.
     *
     * <p>One side is the Java filter ({@code !paused && !orphaned}), the other is the SQL {@code WHERE} —
     * and this is the only test that submits both to the SAME fixture, including the
     * {@code retired = false} the horizon inherits from {@code findAll} and cannot repeat on its own.
     * Diverging the two sides is a silent failure: the node wakes at the cadence of a trigger nobody
     * fires, and only ticks per second reveals it.
     */
    @Test
    void theHorizonSeesExactlyWhatFindDueRecurringWouldFire() {
        for (String key : List.of("paused", "orphaned", "retired")) {
            jobStore.upsert(JobDefinition.of(key, Handler.class, spec -> spec.every(Duration.ofMinutes(5))));
        }
        jobStore.pause(JobKey.of("paused"));
        jobStore.markOrphaned(JobKey.of("orphaned"));
        jobStore.remove(JobKey.of("retired"));
        // The healthy one is armed LATER, and therefore LATER in time: if any of the three leaked into the
        // horizon, the earliest of them would win the comparison
        clock.advance(Duration.ofMinutes(1));
        jobStore.upsert(JobDefinition.of("healthy", Handler.class, spec -> spec.every(Duration.ofMinutes(5))));
        Instant healthyFire = jobStore.find(JobKey.of("healthy")).orElseThrow().nextFireAt();
        List<StoredJob> snapshot;
        try (var all = jobStore.findAll()) {
            snapshot = all.toList();
        }

        assertThat(jobStore.findDueRecurring(NOW.plusSeconds(600), 100))
                .extracting(job -> job.definition().key().value())
                .containsExactly("healthy");
        assertThat(Engine.earliestArmedFire(snapshot, null, NOW))
                .as("waking for a trigger nobody fires is a silent failure — visible only in ticks per second")
                .isEqualTo(healthyFire);
    }

    @Test
    void heartbeatReflectsCurrentStateAcrossPauseAndResume() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::state).containsExactly(EngineState.RUNNING);

            counting.resetLatch(new CountDownLatch(2));
            engine.pause();
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::state).containsExactly(EngineState.PAUSED);

            counting.resetLatch(new CountDownLatch(2));
            engine.resume();
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::state).containsExactly(EngineState.RUNNING);
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    @Test
    void pauseStopsNewClaimsUntilResumed() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            engine.pause();

            // Ticks are serialised by scheduleWithFixedDelay (the next is only scheduled when the previous
            // returns). Within the tick, state.get() — the claim's gate — comes BEFORE the heartbeat: the
            // 1st fresh heartbeat may belong to a tick already in flight that read RUNNING and is still
            // going to claim. The 2nd heartbeat necessarily belongs to a tick that only started after the
            // in-flight one finished, claim included — only then is it safe to seed (a real flake was
            // observed with the seeding before this marker).
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            seedEnqueuedExecution("exec-1", "welcome-email", "hello");
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.ENQUEUED);

            // No listener here — the final assertion is about persisted state, not an event; waiting a few
            // more real ticks is enough for a no-op handler to be claimed and completed.
            engine.resume();
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * Liveness lives on the NODE: a handler slower than any TTL survives while the node keeps ticking,
     * with no per-execution renewal (the lease does not even carry an expiry of its own). The clock's
     * advance deliberately exceeds {@code lease-ttl}: nothing reclaims the ownership, because the
     * heartbeat runs BEFORE the reaper within the same tick (the ordering that kills self-reaping) and
     * the node's promise stays fresh.
     */
    @Test
    void aHandlerOutlivingItsExecutionLeaseSurvivesWhileTheNodeTicks() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            clock.advance(LEASE_TTL.plusSeconds(5));
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            // The ownership still belongs to this node and no synthetic attempt was written
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
            assertThat(rawJdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class)).isZero();
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * Each tick's claim is bounded by the dispatch headroom ({@code dispatchConcurrency} minus in-flight)
     * — a saturated node stops claiming rather than exceeding the runner's ceiling. The surplus stays in
     * the queue ({@code mohs_ready}), claimable by any node with headroom.
     */
    @Test
    void claimIsBoundedByTheFreeDispatchCapacity() throws Exception {
        for (int i = 1; i <= 5; i++) {
            seedEnqueuedExecution("exec-" + i, "welcome-email", "hello");
        }
        int dispatchConcurrency = 2;
        CountDownLatch handlersStarted = new CountDownLatch(dispatchConcurrency);
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch allSucceeded = new CountDownLatch(5);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlersStarted.countDown();
            releaseHandlers.await(10, TimeUnit.SECONDS);
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                allSucceeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of(listener), defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, dispatchConcurrency, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(handlersStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // At least 2 complete ticks with the node saturated: headroom 0, so no new claim can happen
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(leaseCount()).isEqualTo(dispatchConcurrency);
            assertThat(readyCount()).isEqualTo(3);

            releaseHandlers.countDown();
            // With the headroom back, the following ticks drain the surplus
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(5);
    }

    /**
     * The define-plus-schedule race within one tick (a real bug — about 50% failure in the starter's
     * end-to-end test that defines and schedules in sequence): the tick's definitions snapshot PRECEDES
     * the claim, so a newborn job may be outside the snapshot with its entry already in the queue — and
     * it is NOT a removed job.
     *
     * <p>A snapshot miss cures with a fresh query ({@code jobStore.find}); without it, the execution died
     * a terminal FAILED with the "removed" message. A jobStore blind in findAll makes the miss
     * deterministic.
     */
    @Test
    void aJobBornAfterTheTickSnapshotStillDispatches() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        JobStore snapshotBlind = mock(JobStore.class, delegatesTo(jobStore));
        doAnswer(_ -> java.util.stream.Stream.<StoredJob>empty()).when(snapshotBlind).findAll();
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = assembleEngine(snapshotBlind, workQueue, leaseStore, historyStore, nodeStore, List.of(listener),
                defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("a just-born job dispatches despite the stale snapshot").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * The heal's complement: the fresh query's find is MEMOISED into the snapshot — without that, the
     * Admission of the same tick's later rounds would not see the newborn's leases ({@code leaseCount}
     * = 0) and {@code maxConcurrentExecutions}'s mutex would become a no-op, up to
     * {@code claimRounds x cap} concurrent executions on a single node.
     *
     * <p>{@code batchSize=1} plus {@code claimRounds=2} forces exactly round 2 inside the birth tick (the
     * findAll is blind only on the first call).
     */
    @Test
    void aJobBornAfterTheSnapshotStillHonoursItsConcurrencyCapAcrossClaimRounds() throws Exception {
        jobStore.upsert(JobDefinition.of("single-file", Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(1)));
        recordAndOffer("exec-1", "single-file", "hello", NOW.minusSeconds(2));
        recordAndOffer("exec-2", "single-file", "hello", NOW.minusSeconds(1));
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch bothSucceeded = new CountDownLatch(2);
        handlerRegistry.register(JobKey.of("single-file"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandlers.await(10, TimeUnit.SECONDS);
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                bothSucceeded.countDown();
            }
        };
        JobStore blindOnce = mock(JobStore.class, delegatesTo(jobStore));
        AtomicBoolean firstSnapshot = new AtomicBoolean(true);
        doAnswer(_ -> firstSnapshot.getAndSet(false) ? java.util.stream.Stream.<StoredJob>empty() : jobStore.findAll())
                .when(blindOnce).findAll();
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = assembleEngine(blindOnce, workQueue, leaseStore, historyStore, counting, List.of(listener),
                defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, POLL_INTERVAL, 1, 10, 2, LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            // The blind tick's round 2 did NOT break the mutex: one ownership, one in the queue
            assertThat(leaseCount()).isEqualTo(1);
            assertThat(readyCount()).isEqualTo(1);

            releaseHandlers.countDown();
            assertThat(bothSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt", Integer.class)).containsExactly(1, 1);
    }

    /**
     * The window as a second line of defence: a newborn job (outside the snapshot) with a CLOSED window
     * does not run — {@code admitFor} blocks it and returns it to the queue with the SAME attempt. It is
     * the only guard between the queue and the window when the pre-claim filter does not know the job (or
     * was discarded in {@code MAX_INADMISSIBLE_FILTER}'s degraded mode).
     */
    @Test
    void aJobBornAfterTheSnapshotInsideAClosedWindowIsRequeuedNotDispatched() throws Exception {
        jobStore.upsert(JobDefinition.of("night-batch", Handler.class, spec -> spec.onDemand().window("night")));
        recordAndOffer("exec-1", "night-batch", "hello", NOW.minusSeconds(1));
        AtomicBoolean handlerRan = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("night-batch"), (payload, ctx) -> handlerRan.set(true));
        JobStore alwaysBlind = mock(JobStore.class, delegatesTo(jobStore));
        // ALWAYS blind: every round exercises the heal path — the guard's worst case
        doAnswer(_ -> java.util.stream.Stream.<StoredJob>empty()).when(alwaysBlind).findAll();
        ExecutionWindowRegistry closedWindow = new ExecutionWindowRegistry(
                List.of(new ExecutionWindow("night", List.of(_ -> true))));
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(3));
        Engine engine = assembleEngine(alwaysBlind, workQueue, leaseStore, historyStore, counting, closedWindow,
                List.of(), defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handlerRan.get()).isFalse();
            assertThat(leaseCount()).isZero();
            assertThat(readyCount()).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT attempt FROM mohs_ready", Integer.class)).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_attempt", Integer.class)).isZero();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /**
     * The admission race resolved post-claim: a partial cap (headroom 1, the round brought 2) admits one
     * and returns the other to the queue with the SAME attempt and no synthetic attempt — an admission
     * loss never consumes budget.
     *
     * <p>While the live ownership saturates the cap, the returned one waits in the queue (the guard makes
     * it inadmissible on later rounds); once the cap frees up, it runs normally.
     */
    @Test
    void admissionCapOverflowRequeuesTheLoserWithoutBurningBudget() throws Exception {
        jobStore.upsert(JobDefinition.of("single-file", Handler.class, spec -> spec.onDemand().maxConcurrentExecutions(1)));
        recordAndOffer("exec-1", "single-file", "hello", NOW.minusSeconds(2));
        recordAndOffer("exec-2", "single-file", "hello", NOW.minusSeconds(1));
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch bothSucceeded = new CountDownLatch(2);
        handlerRegistry.register(JobKey.of("single-file"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandlers.await(10, TimeUnit.SECONDS);
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                bothSucceeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of(listener));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            counting.resetLatch(new CountDownLatch(3));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            // one running, one back in the queue — the attempt intact, no attempt recorded
            assertThat(leaseCount()).isEqualTo(1);
            assertThat(readyCount()).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT attempt FROM mohs_ready", Integer.class)).isEqualTo(1);
            assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_attempt", Integer.class)).isZero();

            releaseHandlers.countDown();
            assertThat(bothSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        // Each ran exactly once — the admission loss did not become an attempt
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt", Integer.class)).containsExactly(1, 1);
    }

    /**
     * The failure path of the failure path, end to end: a FAILED execution with attempts 1 and 2 already
     * recorded is manually rearmed ({@code WorkQueue.rearmForManualRetry}: the advisory returns to
     * PENDING and the queue gains the attempt-3 entry, COUNT(attempts)+1), the claim takes it, attempt 3
     * is written WITHOUT a primary-key collision and, with the budget already exhausted, the new failure
     * ends as a terminal FAILED — a manual retry buys exactly one attempt, never a loop.
     */
    @Test
    void aManuallyRearmedExecutionRunsOnceMoreAndFailsTerminally() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = 'exec-1'");
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            throw new RuntimeException("still broken");
        });
        rawJdbcTemplate.update("UPDATE mohs_execution SET state = 'FAILED' WHERE execution_id = 'exec-1'");
        for (int number = 1; number <= 2; number++) {
            rawJdbcTemplate.update(
                    "INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error) VALUES (?, ?, 'node-a', ?, ?, 'FAILED', 'java.lang.RuntimeException', 'boom')",
                    "exec-1", number, JdbcTimestamps.toUtcLocalDateTime(NOW), JdbcTimestamps.toUtcLocalDateTime(NOW));
        }
        CountDownLatch failedTerminally = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Failed failed && failed.attemptsExhausted()) {
                failedTerminally.countDown();
            }
        };
        assertThat(workQueue.rearmForManualRetry(ExecutionId.of("exec-1"), NOW)).isTrue();
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failedTerminally.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        Integer attempts = rawJdbcTemplate.queryForObject("SELECT count(*) FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class);
        assertThat(attempts).isEqualTo(3);
    }

    /**
     * A graceful shutdown writes the last heartbeat as STOPPED — without it, a clean stop and a crash were
     * indistinguishable in the database (a row RUNNING forever).
     *
     * <p>The poll is deliberately unreachable: the first tick runs immediately and is the ONLY one —
     * deterministic against the accepted, documented race of a tick in flight committing after the final
     * write.
     */
    @Test
    void stopWritesAFinalStoppedHeartbeat() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(1));
        Engine engine = newEngine(counting, List.of(), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofMinutes(5), BATCH_SIZE, LEASE_TTL));
        engine.start();
        assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

        engine.stop(Duration.ofSeconds(5));

        assertThat(nodeStore.findAll()).singleElement()
                .extracting(StoredNode::state).isEqualTo(EngineState.STOPPED);
    }

    /** The final heartbeat is best-effort — a database down during shutdown becomes a WARN, never a failed stop. */
    @Test
    void stopCompletesEvenWhenTheFinalHeartbeatWriteFails() throws Exception {
        NodeStore blinkingStore = mock(NodeStore.class, delegatesTo(nodeStore));
        // The purge is the tick's LAST NodeStore call — waiting for it guarantees the tick's thread no
        // longer touches the mock during the stubbing below (Mockito detects the interleaving as
        // UnfinishedStubbing)
        CountDownLatch tickDone = new CountDownLatch(1);
        NodeStore tickCompletionProbe = new NodeStore() {
            @Override
            public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
                blinkingStore.heartbeat(nodeId, state, epoch, at, expiresAt);
            }

            @Override
            public List<StoredNode> findAll() {
                return blinkingStore.findAll();
            }

            @Override
            public int deleteHeartbeatsBefore(Instant cutoff) {
                int purged = blinkingStore.deleteHeartbeatsBefore(cutoff);
                tickDone.countDown();
                return purged;
            }
        };
        Engine engine = newEngine(tickCompletionProbe, List.of(), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofMinutes(5), BATCH_SIZE, LEASE_TTL));
        engine.start();
        assertThat(tickDone.await(5, TimeUnit.SECONDS)).isTrue();
        doThrow(new DataAccessResourceFailureException("database down during shutdown"))
                .when(blinkingStore).heartbeat(any(), any(), anyLong(), any(), any());

        engine.stop(Duration.ofSeconds(5));

        assertThat(engine.state()).isEqualTo(EngineState.STOPPED);
    }

    /** A heartbeat older than 10x lease-ttl is purged as a passenger on the tick — each boot generates a new node_id, and without the purge every dead instance left an orphan row forever. */
    @Test
    void tickPurgesNodeRowsWithStaleHeartbeats() throws Exception {
        Instant staleAt = NOW.minus(LEASE_TTL.multipliedBy(10)).minusSeconds(1);
        nodeStore.heartbeat("dead-node", EngineState.RUNNING, 1, staleAt, staleAt.plus(LEASE_TTL));
        nodeStore.heartbeat("recent-node", EngineState.RUNNING, 1, NOW.minusSeconds(1), NOW.minusSeconds(1).plus(LEASE_TTL));
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nodeStore.findAll()).extracting(StoredNode::nodeId)
                    .doesNotContain("dead-node")
                    .contains("recent-node");
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /**
     * With {@code claimRounds > 1}, ONE tick chains claims while the batch keeps coming back full —
     * bounded by the number of rounds and by the dispatch headroom, which shrinks with each round.
     *
     * <p>The tick/claim trail recorded by the wrappers proves the shape from the inside: [2,2] on the
     * saturating tick, [2] when only enough headroom for one round remains, [] with the node full.
     */
    @Test
    void aFullBatchChainsAnotherClaimRoundWithinTheSameTick() throws Exception {
        for (int i = 1; i <= 6; i++) {
            seedEnqueuedExecution("exec-" + i, "welcome-email", "hello");
        }
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch allSucceeded = new CountDownLatch(6);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> releaseHandlers.await(10, TimeUnit.SECONDS));
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                allSucceeded.countDown();
            }
        };
        List<String> trace = Collections.synchronizedList(new ArrayList<>());
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(4));
        Engine engine = newEngineWithTickTrace(trace, counting, List.of(listener),
                new EngineSettings(POLL_INTERVAL, POLL_INTERVAL, 2, 6, 2, LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            List<List<Integer>> ticks = claimsPerTick(trace);

            assertThat(ticks.get(0)).containsExactly(2, 2); // rounds chained within the same tick, ceiling claimRounds=2
            assertThat(ticks.get(1)).containsExactly(2);    // the dispatch headroom (6-4) bounds the single round
            assertThat(ticks.get(2)).isEmpty();             // a full node: a tick with no claim

            releaseHandlers.countDown();
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(6);
    }

    /** A batch that comes back smaller than requested ends the rounds — the next round would be a SELECT over an already drained queue. */
    @Test
    void aShortBatchEndsTheClaimRoundsEarly() throws Exception {
        for (int i = 1; i <= 3; i++) {
            seedEnqueuedExecution("exec-" + i, "welcome-email", "hello");
        }
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        CountDownLatch allSucceeded = new CountDownLatch(3);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> releaseHandlers.await(10, TimeUnit.SECONDS));
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                allSucceeded.countDown();
            }
        };
        List<String> trace = Collections.synchronizedList(new ArrayList<>());
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngineWithTickTrace(trace, counting, List.of(listener),
                new EngineSettings(POLL_INTERVAL, POLL_INTERVAL, 2, 10, 3, LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            List<List<Integer>> ticks = claimsPerTick(trace);

            assertThat(ticks.get(0)).containsExactly(2, 1); // the 3rd round does not happen despite claimRounds=3
            // Raw saving: lap 1 stops at the 1st probe (a full budget), lap 2 goes all the way round (64)
            // and does not fill up, so there is NO lap 3 — one extra lap of empty probes would exceed this
            // ceiling
            assertThat(rawClaimStatementsInTick(trace, 0)).isLessThanOrEqualTo(1 + Shards.SHARD_COUNT);

            releaseHandlers.countDown();
            assertThat(allSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseHandlers.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(3);
    }

    /**
     * The 64-probe lap was measured at 96% of an IDLE node's query cost. While the previous round came back
     * empty, the tick asks ONCE whether there is visible work in its own shards — contention is a
     * phenomenon of load, and there is nothing to spread when the answer is "nothing". The first tick does
     * not know that yet and does the whole lap.
     */
    @Test
    void anIdleTickProbesOnceInsteadOfLappingEveryShard() throws Exception {
        List<String> trace = Collections.synchronizedList(new ArrayList<>());
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(3));
        Engine engine = newEngineWithTickTrace(trace, counting, List.of(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        assertThat(rawClaimStatementsInTick(trace, 0)).isEqualTo(Shards.SHARD_COUNT);
        assertThat(emptyGateProbesInTick(trace, 0)).isZero();
        assertThat(rawClaimStatementsInTick(trace, 1)).isZero();
        assertThat(emptyGateProbesInTick(trace, 1)).isEqualTo(1);
    }

    /**
     * The idle gate is a saving, never a correctness filter: a probe that finds work returns the SAME tick
     * to the lap — an enqueue does not pay one extra poll for having arrived at a parked engine.
     */
    @Test
    void workOfferedWhileIdleRunsWithoutAnExtraTick() throws Exception {
        CountDownLatch succeeded = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> { });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        List<String> trace = Collections.synchronizedList(new ArrayList<>());
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(3));
        Engine engine = newEngineWithTickTrace(trace, counting, List.of(listener),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            seedEnqueuedExecution("exec-1", "welcome-email", "hello");
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(1);
        // The latch alone would pass even with a tick's delay (this fixture's poll is 20ms): what proves
        // the guarantee is the lap being in the SAME tick
        assertThat(rawClaimStatementsInTick(trace, tickThatProbedTrue(trace)))
                .as("the probe that found work returns the same tick to the lap").isPositive();
    }

    /** The index of the tick in which the idle gate probed and FOUND work — the tick that must contain the lap. */
    private static int tickThatProbedTrue(List<String> trace) {
        List<String> snapshot;
        synchronized (trace) {
            snapshot = List.copyOf(trace);
        }
        int tick = -1;
        for (String entry : snapshot) {
            if (entry.equals("tick")) {
                tick++;
            } else if (entry.equals("probe:true")) {
                return tick;
            }
        }
        throw new AssertionError("the idle gate never probed true — trace: " + snapshot);
    }

    /**
     * The phase's number-one risk: the backoff at its ceiling must NOT space the heartbeat beyond
     * {@code node-lease-ttl/3} — the heartbeat goes out once per tick, so the loop's sleep is bounded by
     * the liveness promise's cadence; otherwise a merely IDLE node would be declared dead by its peers'
     * reaper and have future work claimed for nothing.
     *
     * <p>An absurd ceiling (1h) on purpose: without the cap, the second heartbeat would only come in about
     * an hour and the await would time out.
     */
    @Test
    void idleBackoffNeverStretchesTheHeartbeatPastAThirdOfTheNodeLease() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(4));
        Engine engine = newEngine(counting, List.of(), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofMillis(20), Duration.ofHours(1), BATCH_SIZE, 10, 1,
                        LEASE_TTL, Duration.ofMillis(300), null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            // 4 heartbeats at 100ms or less (300ms/3) fit comfortably; at a 1h ceiling, never
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /** An empty tick doubles the interval up to the ceiling — an idle engine polls less and less, and returns to the floor when there is work (covered by the claim tests, which run at the floor). */
    @Test
    void emptyTicksBackOffTheLoopTowardsTheCeiling() throws Exception {
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(3));
        Engine engine = newEngine(counting, List.of(), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofMillis(20), Duration.ofSeconds(1), BATCH_SIZE, 10, 1,
                        LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            // After 3 empty ticks the interval has already doubled to 160ms or more: 9 more ticks in 500ms
            // are IMPOSSIBLE with backoff (at most 4 fit) — and trivial at the fixed 20ms cadence a
            // regression would restore (25 would fit)
            CountDownLatch nineMore = new CountDownLatch(9);
            counting.resetLatch(nineMore);
            assertThat(nineMore.await(500, TimeUnit.MILLISECONDS)).isFalse();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /** The local wake-up tier: an already-due local enqueue wakes the loop immediately — dispatch latency does not wait for the poll interval. */
    @Test
    void aDueLocalEnqueueWakesTheLoopWithoutWaitingThePoll() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(1));
        // A 2s floor: without the wake, the next claim would only come 2s after the first empty tick — the
        // 1s await below is impossible without the local tier
        Engine engine = newEngine(counting, List.of(listener), defaultRunnerRegistry(),
                new EngineSettings(Duration.ofSeconds(2), Duration.ofSeconds(2), BATCH_SIZE, 10, 1,
                        LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD));

        engine.start();
        try {
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue(); // the first (empty) tick has already passed
            recordAndOffer("exec-1", "welcome-email", "hello", NOW.minusSeconds(1));
            engine.signalWorkScheduled(); // outside a transaction, so an immediate wake
            assertThat(succeeded.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /**
     * With a live RUNNING peer, the partition derived from the ordered node ids gives each node half the
     * shards — this node does NOT claim from the peer's shards, even with the execution due and plenty of
     * headroom. "zzz-peer" sorts after this engine's UUID node_id, so this node is index 0 and owns the
     * EVEN shards.
     */
    @Test
    void shardOwnershipLeavesThePeersShardsUnclaimed() throws Exception {
        nodeStore.heartbeat("zzz-peer", EngineState.RUNNING, 1, NOW, NOW.plusSeconds(3600));
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        recordAndOffer("exec-mine", "welcome-email", "hello", NOW.minusSeconds(1), 0);
        recordAndOffer("exec-peers", "welcome-email", "hello", NOW.minusSeconds(1), 1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(1));
        Engine engine = newEngine(counting, List.of(listener));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
            // Two more complete ticks AFTER the success: the chance the node would have had to claim the
            // other's shard, if it were going to
            CountDownLatch twoMoreTicks = new CountDownLatch(2);
            counting.resetLatch(twoMoreTicks);
            assertThat(twoMoreTicks.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(stateOf("exec-mine")).isEqualTo(ExecutionState.SUCCEEDED);
            assertThat(stateOf("exec-peers")).isEqualTo(ExecutionState.ENQUEUED);
            assertThat(readyCount()).isEqualTo(1);
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /** A non-RUNNING peer (DRAINING/PAUSED) does not claim — keeping it in the assignment would leave 1/n of the queue stalled; with both excluded, their shards come back here and the execution runs. */
    @Test
    void aNonRunningPeerIsExcludedFromShardAssignment() throws Exception {
        nodeStore.heartbeat("zzz-peer", EngineState.DRAINING, 1, NOW, NOW.plusSeconds(3600));
        nodeStore.heartbeat("zzz-peer2", EngineState.PAUSED, 1, NOW, NOW.plusSeconds(3600));
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        recordAndOffer("exec-even", "welcome-email", "hello", NOW.minusSeconds(1), 0);
        recordAndOffer("exec-odd", "welcome-email", "hello", NOW.minusSeconds(1), 1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        CountDownLatch bothSucceeded = new CountDownLatch(2);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                bothSucceeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(bothSucceeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(terminalCount(ExecutionState.SUCCEEDED)).isEqualTo(2);
    }

    /**
     * Lost ownership (an external reclaim) has no active detection — the zombie finishes on its own and its
     * late result is discarded by the {@code (node_id, epoch)} fence: the lease now belongs to the new
     * incarnation, and the zombie's completion neither deletes somebody else's ownership, nor writes an
     * attempt, nor touches the advisory. Without the fence, the zombie's completion would kill the healthy
     * new incarnation.
     */
    @Test
    void aZombieResultAfterAnExternalReclaimIsDiscardedByTheFencedCompletion() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        Engine engine = newEngine(nodeStore, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // A simulated external re-claim: another LIVE node (otherwise this engine's reaper would declare
            // it dead and reclaim mid-test) is already re-executing the same row — the ownership belongs to
            // another incarnation
            nodeStore.heartbeat("other-node", EngineState.RUNNING, 1, NOW, NOW.plusSeconds(3600));
            rawJdbcTemplate.update(
                    "UPDATE mohs_lease SET node_id = 'other-node', epoch = 9 WHERE execution_id = 'exec-1'");
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5)); // drains the handler — the late completion runs and loses the FENCE
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT node_id FROM mohs_lease WHERE execution_id = 'exec-1'", String.class)).isEqualTo("other-node");
        assertThat(rawJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class)).isZero();
    }

    /**
     * End to end: the job's timeout fires as a passenger on the tick, interrupts the blocked handler, and
     * the outcome is passive — following the budget when the handler responds (here retries=0, so FAILED
     * with a timeout cause).
     */
    @Test
    void jobTimeoutInterruptsTheHandlerAndTheOutcomeFollowsTheRetryBudget() throws Exception {
        // retries(0) declared: it is what makes the timeout the OUTCOME rather than one more attempt — the
        // test measures the outcome with the budget exhausted
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class,
                spec -> spec.onDemand().timeout(Duration.ofMillis(50)).retries(0)));
        recordAndOffer("exec-1", "welcome-email", "hello", NOW.minusSeconds(1));
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            try {
                never.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
        });
        CountDownLatch failedPublished = new CountDownLatch(1);
        AtomicReference<Failed> failed = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failed.set(f);
                failedPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failedPublished.await(5, TimeUnit.SECONDS)).as("timeout interrupts and the attempt fails").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(interrupted).isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(failed.get().error().getMessage()).contains("exceeded job timeout");
    }

    /**
     * An expired grace escalates through the cancellation machinery — flag plus interrupt; the attempt fails
     * with a NodeShutdown cause and follows the normal retry (here retries=0, so FAILED). During the grace
     * none of this happens: a drain is not a cancel.
     */
    @Test
    void drainGraceOverflowInterruptsInFlightWorkAndItFailsWithNodeShutdown() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            never.await(10, TimeUnit.SECONDS);
        });
        CountDownLatch failedPublished = new CountDownLatch(1);
        AtomicReference<Failed> failed = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failed.set(f);
                failedPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
        engine.stop(Duration.ofMillis(50));

        assertThat(failedPublished.await(5, TimeUnit.SECONDS)).as("escalation interrupts and the attempt fails").isTrue();
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(failed.get().error().getMessage()).contains("node shutdown");
    }

    /**
     * End to end: a manual cancel recorded in the database (what {@code POST /executions/{id}/cancel} does
     * from another process — the flag now living on the OWNERSHIP, {@code mohs_lease.cancel_requested}) is
     * observed by the tick within one poll interval — a pure flag, with no interrupt (cancellation is
     * cooperative by contract); the handler observes it through JobContext, exits, and the outcome is
     * CANCELLED with a Cancelled event.
     */
    @Test
    void manualCancelRequestedInTheDatabaseCancelsTheRunningExecution() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            while (!ctx.cancellationRequested()) {
                Thread.onSpinWait();
            }
            throw new IllegalStateException("stopping: cancellation observed");
        });
        CountDownLatch cancelledPublished = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Cancelled) {
                cancelledPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(leaseStore.requestCancellation(ExecutionId.of("exec-1"))).isTrue();
            assertThat(cancelledPublished.await(5, TimeUnit.SECONDS)).as("tick observes the flag and the handler exits").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.CANCELLED);
    }

    /**
     * The successor to the per-execution expiry the split retired: a lease belonging to THIS node with no
     * in-memory incarnation (work lost between claim and dispatch — a failed payload query, a rejecting
     * executor) would be invisible to the reaper (the node is alive) and to the watchdog (it never entered
     * the map). The reconciliation pass returns it to the queue in two rounds, with the SAME attempt — and
     * it completes without burning budget.
     */
    @Test
    void aStrayLeaseOnAHealthyNodeIsRequeuedAndCompletes() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = 'exec-1'");
        CountDownLatch succeeded = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));
        // the ownership belongs to THIS engine (real nodeId/epoch), but no dispatch knows about it
        rawJdbcTemplate.update("""
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested)
                VALUES ('exec-1', 'welcome-email', ?, 1, 1, 20, ?, FALSE)
                """, engine.nodeId(), JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        // It ran ONCE, attempt 1 — the reconciliation returned it, it did not punish it
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class))
                .containsExactly(1);
    }

    /**
     * The grace: a lease with no incarnation but RECENTLY claimed is not a reconcile candidate — at high
     * throughput it is almost always a completion in transit in the batcher, and requeueing it is the bug
     * measured in the bench (phantom requeues, deadlocks with the flush). Only after
     * {@code max(2s, 4xpoll)} plus two rounds does it become a genuine orphan and return to the queue.
     */
    @Test
    void aFreshStrayLeaseWaitsTheClaimedAtGraceBeforeRequeue() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = 'exec-1'");
        CountDownLatch succeeded = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
        });
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(3));
        Engine engine = newEngine(counting, List.of(listener));
        // Ownership held by this engine WITHOUT an incarnation, claimed_at = now — inside the grace
        rawJdbcTemplate.update("""
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested)
                VALUES ('exec-1', 'welcome-email', ?, 1, 1, 20, ?, FALSE)
                """, engine.nodeId(), JdbcTimestamps.toUtcLocalDateTime(NOW));

        engine.start();
        try {
            // Several ticks with the clock stopped: inside the grace, NOTHING is requeued
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(readyCount()).isZero();
            assertThat(leaseCount()).isEqualTo(1);

            // Beyond the grace (2s at the test's poll): two rounds later, requeue, claim, complete
            clock.advance(Duration.ofSeconds(3));
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).as("stray requeued after the grace and completed").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(rawJdbcTemplate.queryForList("SELECT number FROM mohs_attempt WHERE execution_id = 'exec-1'", Integer.class))
                .containsExactly(1);
    }

    /** A mass death one row wider than a reclaim chunk, with retry budget: the sweep needs two transactions to clear it. */
    private void seedOrphanedLeasesSpanningTwoReclaimChunks() {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1)));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> { });
        for (int i = 1; i <= Engine.RECLAIM_CHUNK + 1; i++) {
            seedOrphanedLease("exec-" + i, "welcome-email", false);
        }
    }

    /** A lease owned by a node ABSENT from mohs_nodes (dead by definition) — the raw material of the reaper tests. */
    private void seedOrphanedLease(String id, String jobKey, boolean cancelRequested) {
        recordAndOffer(id, jobKey, "hello", NOW.minusSeconds(60));
        rawJdbcTemplate.update("DELETE FROM mohs_ready WHERE execution_id = ?", id);
        rawJdbcTemplate.update("""
                INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at, cancel_requested)
                VALUES (?, ?, 'dead-node', 1, 1, 20, ?, ?)
                """, id, jobKey, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(60)), cancelRequested);
    }

    /** The reaper's outcome publishes the same events as a dispatch — the node-death alert (Javadoc de Failed) passa a disparar de verdade. */
    @Test
    void reclaimOfADeadNodesExecutionPublishesRetryEvents() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1)));
        seedOrphanedLease("exec-1", "welcome-email", false);
        CountDownLatch retryScheduled = new CountDownLatch(1);
        CountDownLatch attemptFailed = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof RetryScheduled) {
                retryScheduled.countDown();
            }
            if (event instanceof AttemptFailed) {
                attemptFailed.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(attemptFailed.await(5, TimeUnit.SECONDS)).as("AttemptFailed published for the reclaim").isTrue();
            assertThat(retryScheduled.await(5, TimeUnit.SECONDS)).as("RetryScheduled published for the reclaim").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        // The final state is not asserted: after the reclaim, the retry may be re-claimed within the same
        // test (the jitter can be about 0) — the contract under test is the events
    }

    /**
     * A mass death is reclaimed in chunks of {@code RECLAIM_CHUNK}, each its own transaction under the
     * tick's deadline: a chunk that fails loses only itself — the chunks after it still run in the same
     * sweep, and what it lost is found again next tick. The sizes the store sees pin the order: the
     * first chunk (50) throws, the second (1) is attempted anyway, the first comes back on the next tick.
     */
    @Test
    void aFailingReclaimChunkDoesNotStopTheChunksAfterIt() {
        seedOrphanedLeasesSpanningTwoReclaimChunks();
        List<Integer> reclaimSizes = Collections.synchronizedList(new ArrayList<>());
        LeaseStore firstChunkFails = mock(LeaseStore.class, delegatesTo(leaseStore));
        doAnswer(invocation -> {
            List<LeaseStore.CompletionResult> results = invocation.getArgument(0);
            reclaimSizes.add(results.size());
            if (reclaimSizes.size() == 1) {
                throw new RuntimeException("simulated deadline on the first chunk");
            }
            return leaseStore.reclaim(results, invocation.getArgument(1));
        }).when(firstChunkFails).reclaim(any(), any());
        Engine engine = assembleEngine(workQueue, firstChunkFails, historyStore, nodeStore, List.of(), defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(rawJdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM mohs_lease WHERE node_id = 'dead-node'", Integer.class)).isZero());
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        assertThat(reclaimSizes.subList(0, 2)).as("the second chunk ran in the same sweep the first one failed in")
                .containsExactly(Engine.RECLAIM_CHUNK, 1);
        assertThat(reclaimSizes.get(2)).as("the failed chunk came back on the next tick").isEqualTo(Engine.RECLAIM_CHUNK);
    }

    /** On the crash-recovery path: reclaiming a dead node's execution with a pending cancel publishes Cancelled — neither a retry nor a Failed; the operator's order survives the node's death. */
    @Test
    void reclaimOfACancelRequestedExecutionPublishesCancelled() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand().retries(1)));
        seedOrphanedLease("exec-1", "welcome-email", true);
        CountDownLatch cancelledPublished = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Cancelled) {
                cancelledPublished.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(cancelledPublished.await(5, TimeUnit.SECONDS)).as("Cancelled published for the reclaim").isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.CANCELLED);
    }

    /**
     * A zombie (ownership lost to an external reclaim) stays in the in-flight map until completion — and so
     * the shutdown's escalation still interrupts it (for a job with no timeout, the only chance of stopping
     * it before the JVM dies). Its late result is discarded by the fence, like every zombie's.
     */
    @Test
    void aZombieAfterAnExternalReclaimStillReceivesTheShutdownInterrupt() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            try {
                never.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        });
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // An external reclaim: the ownership dropped, but the entry stays in the map until completion
            rawJdbcTemplate.update("DELETE FROM mohs_lease WHERE execution_id = 'exec-1'");
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofMillis(50));
        }

        assertThat(interrupted.await(5, TimeUnit.SECONDS)).as("escalation still reaches the dropped zombie").isTrue();
    }

    /** A drain is not a cancel: in-flight work keeps executing in PAUSED — the NODE's liveness promise has to follow the WORK, not the control loop's mode; without this, a pause or drain longer than the node's lease becomes a double execution of what the drain itself is waiting for. */
    @Test
    void theNodeLeaseKeepsBeingPromisedWhilePaused() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            engine.pause();
            Instant initialExpiry = nodeLeaseExpiryOfTheOnlyNode();
            clock.advance(Duration.ofSeconds(5));
            counting.resetLatch(new CountDownLatch(2));
            assertThat(counting.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(nodeLeaseExpiryOfTheOnlyNode()).isAfter(initialExpiry);
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
            engine.resume();
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    private Instant nodeLeaseExpiryOfTheOnlyNode() {
        List<StoredNode> nodes = nodeStore.findAll();
        assertThat(nodes).hasSize(1);
        Instant expiresAt = nodes.get(0).expiresAt();
        assertThat(expiresAt).isNotNull();
        return expiresAt;
    }

    /** A backoff ceiling below the floor would invert the ramp — rejected at construction, naming both properties. */
    @Test
    void maxPollIntervalMustBeAtLeastThePollInterval() {
        assertThatThrownBy(() -> new EngineSettings(Duration.ofSeconds(1), Duration.ofMillis(500), BATCH_SIZE, 10, 1,
                LEASE_TTL, LEASE_TTL, null, EngineSettings.DEFAULT_MISFIRE_THRESHOLD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mohs.engine.max-poll-interval")
                .hasMessageContaining("mohs.engine.poll-interval");
    }

    /** A bound at or below the NODE's lease would release ownership before the node could even be considered dead — rejected at construction, naming both properties. */
    @Test
    void watchdogTimeoutMustExceedNodeLeaseTtl() {
        assertThatThrownBy(() -> new EngineSettings(POLL_INTERVAL, BATCH_SIZE, Duration.ofSeconds(30), Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mohs.engine.watchdog-timeout")
                .hasMessageContaining("mohs.engine.node-lease-ttl");
    }

    /**
     * The Watchdog Bound: once the bound passes (real monotonic time, ~200ms here), the node RELEASES the
     * ownership explicitly — a synthetic attempt consumes the budget (retries = 0, so a terminal FAILED)
     * with neither a reaper nor a clock advance involved; the zombie handler keeps running and its late
     * result is discarded by the fence.
     */
    @Test
    void watchdogBoundReleasesOwnershipAndFailsTheExecution() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        CountDownLatch failedPublished = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Failed) {
                failedPublished.countDown();
            }
        };
        CountDownLatch boundWarned = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> watcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("watchdog-timeout")) {
                    boundWarned.countDown();
                }
            }
        };
        watcher.start();
        engineLogger.addAppender(watcher);
        CountingNodeStore counting = new CountingNodeStore(nodeStore, new CountDownLatch(2));
        Engine engine = newEngine(counting, List.of(listener), defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, Duration.ofMillis(50), Duration.ofMillis(200)));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(boundWarned.await(5, TimeUnit.SECONDS)).as("watchdog bound warned within timeout").isTrue();
            assertThat(failedPublished.await(5, TimeUnit.SECONDS)).as("ownership released as a terminal failure").isTrue();

            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        } finally {
            releaseHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
            engineLogger.detachAppender(watcher);
        }
        // o zumbi terminou no stop e o resultado tardio perdeu o fence
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
    }

    @Test
    void drainWaitsForInFlightDispatchWithoutCancellingIt() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(5, TimeUnit.SECONDS);
        });
        Engine engine = newEngine(nodeStore, List.of());

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);

            CompletableFuture<Void> drainCall = CompletableFuture.runAsync(() -> engine.drain(Duration.ofSeconds(5)));
            assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING); // drain ≠ cancel — still running
            releaseHandler.countDown();
            drainCall.get(5, TimeUnit.SECONDS);
        } finally {
            engine.stop(Duration.ZERO);
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * The window between {@code submitDispatch}'s {@code runAsync} and {@code inFlight.add}: within it the
     * execution has a lease in the database and is absent from the set the drain observes, so finding
     * {@code inFlight} empty proves the dispatch has not been REGISTERED yet, not that it finished.
     *
     * <p>If {@code stop} returned there, the final heartbeat would write the node's lease already expired
     * with the handler running, and a peer's reaper would reclaim live work — the good result is discarded
     * by the fence and, with no retry budget, becomes a terminal FAILED.
     *
     * <p>What makes the window deterministic is {@link #registryTrappingTheTickAfterSubmit} — without it
     * the race is a matter of microseconds and does not reproduce. Without {@code stop}'s second wait, the
     * {@code stopCall} below completes immediately and the timeout assertion fails.
     */
    @Test
    void stopWaitsForADispatchSubmittedButNotYetRegistered() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        CountDownLatch releaseSubmit = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> {
            handlerStarted.countDown();
            releaseHandler.await(10, TimeUnit.SECONDS);
        });
        Engine engine = newEngine(nodeStore, List.of(), registryTrappingTheTickAfterSubmit(releaseSubmit));

        engine.start();
        try {
            assertThat(handlerStarted.await(5, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Void> stopCall = CompletableFuture.runAsync(() -> engine.stop(Duration.ofSeconds(10)));
            // Waiting for STOPPED is what makes the scenario deterministic: the state only changes after the
            // drain's wait, and that only passes through an EMPTY `inFlight` — it is exactly that misleading
            // emptiness the test needs to produce. Releasing the submit before this would let the drain see
            // the future and wait on its own: the test would pass even without the second wait.
            //
            // The ceiling has to stay BELOW nodeLeaseTtl/4 (7.5s with the fixture's 30s LEASE_TTL), which is
            // the loop join's budget: exceeding it would make the post-join wait read `inFlight` still empty
            // and the stop return — red because of the environment, not because of a defect
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> engine.state() == EngineState.STOPPED);
            releaseSubmit.countDown(); // only now does the tick register the future and the loop end

            assertThatThrownBy(() -> stopCall.get(1, TimeUnit.SECONDS))
                    .as("stop returned while the handler was still running — the final heartbeat would hand live work to a peer's reaper")
                    .isInstanceOf(TimeoutException.class);

            releaseHandler.countDown();
            stopCall.get(10, TimeUnit.SECONDS);
        } finally {
            // Release both ends: without this, an assertion failing earlier would leave the tick's thread
            // stuck in the submit and the handler hanging
            releaseSubmit.countDown();
            releaseHandler.countDown();
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.SUCCEEDED);
    }

    /**
     * A registry whose executor traps the tick's thread INSIDE {@code execute}, after having already
     * submitted the task to the real executor: that is how the tick freezes in the window between
     * {@code submitDispatch}'s {@code runAsync} and {@code inFlight.add}. It enters through the
     * package-private {@code RunnerRegistry(List, Function)} seam — the only way to swap a runner's
     * executor without touching the engine.
     *
     * <p>A no-op shutdown, like {@code assembleEngine}'s event executor: nobody closes the registry in the
     * test (the {@code Engine} does not own it) and the real executor uses virtual threads, with no pool to
     * leak between tests.
     */
    private static RunnerRegistry registryTrappingTheTickAfterSubmit(CountDownLatch releaseSubmit) {
        AsyncTaskExecutor realExecutor = MohsExecutors.ioBoundExecutor("mohs-runner-gated", BATCH_SIZE);
        AsyncTaskExecutor trappingExecutor = task -> {
            realExecutor.execute(task);
            try {
                // The tick's thread stops HERE: already submitted, not yet registered. Expiring is a FAILURE,
                // not an escape hatch: a trap that releases itself would register the future and make the
                // test pass without the second wait — green for the wrong reason. The exception exits through
                // submitDispatch's "runner executor rejected" guard, the future is never registered, and the
                // test goes red.
                if (!releaseSubmit.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("submit trap expired — the test never released it");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        return new RunnerRegistry(
                List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build()),
                runner -> new RunnerRegistry.LiveRunner(runner, new RunnerRegistry.CountingExecutor(trappingExecutor), () -> { }));
    }

    @Test
    void unreadablePayloadFailsTheExecutionWithoutHangingTheTick() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        rawJdbcTemplate.update("""
                INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, payload_type)
                VALUES ('exec-1', 'welcome-email', 'PENDING', ?, ?, 'test', '{}', 'com.example.DoesNotExist')
                """, JdbcTimestamps.toUtcLocalDateTime(NOW.minusSeconds(1)), JdbcTimestamps.toUtcLocalDateTime(NOW));
        workQueue.offer(List.of(new WorkQueue.ReadyEntry(ExecutionId.of("exec-1"), JobKey.of("welcome-email"), 0, 20, 1, NOW.minusSeconds(1))));
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Failed> failedEvent = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failedEvent.set(f);
                failed.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(handlerCalled.get()).isFalse();
        Execution found = historyStore.find(ExecutionId.of("exec-1"), NOW).orElseThrow();
        assertThat(found.attempts().get(0).error()).contains("payload could not be read");
        // An unreadable payload also publishes Failed, the same path as any other terminal failure.
        assertThat(failedEvent.get()).isNotNull();
        assertThat(failedEvent.get().error()).hasMessageContaining("payload could not be read");
        // Terminal by nature, not by budget: exhausted=false on a pre-dispatch failure
        assertThat(failedEvent.get().attemptsExhausted()).isFalse();
    }

    @Test
    void executionDispatchesOnItsJobsNamedRunner() throws Exception {
        RunnerRegistry runnerRegistry = new RunnerRegistry(List.of(
                MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(BATCH_SIZE).build(),
                MohsRunner.io("s3").maxConcurrent(4).build()));
        seedEnqueuedExecution("exec-1", "s3-upload", "hello", NOW.minusSeconds(1), "s3");
        AtomicReference<String> dispatchThreadName = new AtomicReference<>();
        handlerRegistry.register(JobKey.of("s3-upload"), (payload, ctx) -> dispatchThreadName.set(Thread.currentThread().getName()));
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener), runnerRegistry);

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(dispatchThreadName.get()).startsWith("mohs-runner-s3-");
    }

    /** The same pattern as unreadablePayloadFailsTheExecutionWithoutHangingTheTick — it fails only the execution, not the tick nor the node. */
    @Test
    void unknownRunnerFailsTheExecutionWithoutHangingTheTick() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello", NOW.minusSeconds(1), "ghost-runner");
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Failed> failedEvent = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failedEvent.set(f);
                failed.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(handlerCalled.get()).isFalse();
        assertThat(failedEvent.get()).isNotNull();
        assertThat(failedEvent.get().error()).hasMessageContaining("runner could not be resolved");
    }

    /**
     * Over the new port: a TRANSIENT failure of the batched payload query is infrastructure, never a verdict
     * on the executions — the already claimed batch keeps its ownership standing until a reaper returns it
     * if this node dies; the hiccup never becomes an immediate TERMINAL failure.
     */
    @Test
    void transientPayloadQueryErrorLeavesTheBatchLeasedForTheReaper() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        HistoryStore flakyHistoryStore = mock(HistoryStore.class, delegatesTo(historyStore));
        doThrow(new RuntimeException("simulated transient database error"))
                .when(flakyHistoryStore).findPayloads(any());
        CountDownLatch leftForReaper = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> warnWatcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("could not load the payloads")) {
                    leftForReaper.countDown();
                }
            }
        };
        warnWatcher.start();
        engineLogger.addAppender(warnWatcher);
        Engine engine = assembleEngine(workQueue, leaseStore, flakyHistoryStore, nodeStore, List.of(),
                defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(leftForReaper.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
            engineLogger.detachAppender(warnWatcher);
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
        assertThat(handlerCalled.get()).isFalse();
    }

    /**
     * A {@code Mohs.remove} between claim and dispatch: the definition is genuinely gone (outside the tick's
     * definitions snapshot) — a terminal failure with a diagnostic of its own, not "runner could not be
     * resolved" (which would point the operator at the wrong problem).
     */
    @Test
    void removedDefinitionFailsTheExecutionWithItsOwnError() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello");
        rawJdbcTemplate.update("DELETE FROM mohs_job_definitions WHERE job_key = 'welcome-email'");
        AtomicBoolean handlerCalled = new AtomicBoolean();
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> handlerCalled.set(true));
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Failed> failedEvent = new AtomicReference<>();
        ExecutionListener listener = event -> {
            if (event instanceof Failed f) {
                failedEvent.set(f);
                failed.countDown();
            }
        };
        Engine engine = newEngine(nodeStore, List.of(listener));

        engine.start();
        try {
            assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.FAILED);
        assertThat(handlerCalled.get()).isFalse();
        assertThat(failedEvent.get().error()).hasMessageContaining("was removed after this execution was claimed");
    }

    /**
     * The terminal-failure write (failBeforeDispatch) runs inside tick()'s for loop: if it throws itself (the
     * database, a saturated event executor), the rest of the batch still needs dispatching — without the
     * guard, the exception aborted the loop and exec-2, already owned in the database, was orphaned until a
     * reaper.
     */
    @Test
    void tickContinuesWhenRecordingATerminalFailureThrows() throws Exception {
        // exec-1: a job with no registered definition — the failBeforeDispatch path
        recordAndOffer("exec-1", "ghost-job", "hello", NOW.minusSeconds(2));
        seedEnqueuedExecution("exec-2", "welcome-email", "hello", NOW.minusSeconds(1));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> { });
        LeaseStore failingForExec1 = mock(LeaseStore.class, delegatesTo(leaseStore));
        doAnswer(invocation -> {
            List<LeaseStore.CompletionResult> results = invocation.getArgument(0);
            if (results.stream().anyMatch(result -> result.executionId().equals(ExecutionId.of("exec-1")))) {
                throw new RuntimeException("simulated database error completing exec-1");
            }
            return leaseStore.complete(results, invocation.getArgument(1));
        }).when(failingForExec1).complete(any(), any());
        CountDownLatch succeeded = new CountDownLatch(1);
        ExecutionListener listener = event -> {
            if (event instanceof Succeeded) {
                succeeded.countDown();
            }
        };
        Engine engine = assembleEngine(workQueue, failingForExec1, historyStore, nodeStore, List.of(listener),
                defaultRunnerRegistry(), new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL));

        engine.start();
        try {
            assertThat(succeeded.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
        assertThat(stateOf("exec-2")).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(stateOf("exec-1")).isEqualTo(ExecutionState.RUNNING);
    }

    /**
     * Engine.submitDispatch: a saturated {@code dispatchExecutor} rejects the 2nd and 3rd submission of the
     * same claim batch; before the fix, the synchronous exception from {@code CompletableFuture.runAsync}
     * aborted {@code tick()}'s {@code for} as soon as the 1st rejection happened, leaving the batch's
     * remaining executions without even attempting {@code submitDispatch}.
     *
     * <p>{@code exec-1} holds the executor's only slot (concurrency 1); {@code exec-2} and {@code exec-3} are
     * claimed in the same batch and must be rejected individually — both appear in the log, not just the
     * first, which is only possible if the loop continued after {@code exec-2}'s rejection.
     */
    @Test
    void submitDispatchContinuesAfterDispatchExecutorRejectsOne() throws Exception {
        seedEnqueuedExecution("exec-1", "welcome-email", "hello", NOW.minusSeconds(3));
        seedEnqueuedExecution("exec-2", "welcome-email", "hello", NOW.minusSeconds(2));
        seedEnqueuedExecution("exec-3", "welcome-email", "hello", NOW.minusSeconds(1));
        CountDownLatch releaseFirstHandler = new CountDownLatch(1);
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> releaseFirstHandler.await(5, TimeUnit.SECONDS));

        // A CountDownLatch, not ListAppender.list: an Appender's append() runs on the tick scheduler's thread
        // while this assertion runs on the test's — counting a latch gives the safe publication (JCIP) that
        // reading a plain list concurrently does not (ListAppender.list is a raw ArrayList).
        CountDownLatch exec2Rejected = new CountDownLatch(1);
        CountDownLatch exec3Rejected = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> rejectionWatcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                String message = event.getFormattedMessage();
                if (message.contains("exec-2")) {
                    exec2Rejected.countDown();
                }
                if (message.contains("exec-3")) {
                    exec3Rejected.countDown();
                }
            }
        };
        rejectionWatcher.start();
        engineLogger.addAppender(rejectionWatcher);

        // Concurrency 1 on purpose — exec-1 exhausts the only slot, forcing exec-2 and exec-3 to be rejected.
        RunnerRegistry oneSlotRunnerRegistry = new RunnerRegistry(List.of(MohsRunner.io(RunnerRegistry.DEFAULT_RUNNER).maxConcurrent(1).build()));
        Engine engine = newEngine(nodeStore, List.of(), oneSlotRunnerRegistry);

        engine.start();
        try {
            // Both have to fire — only possible if tick()'s loop continued after exec-2's rejection and got as
            // far as attempting exec-3 too.
            assertThat(exec2Rejected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(exec3Rejected.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseFirstHandler.countDown();
            engine.stop(Duration.ofSeconds(5));
            engineLogger.detachAppender(rejectionWatcher);
        }
    }

    /** Numbers the ticks by their heartbeat, so a collaborator can record which tick it ran in. */
    private static final class TickCountingNodeStore implements NodeStore {
        private final NodeStore delegate;
        private final AtomicInteger ticks = new AtomicInteger();

        TickCountingNodeStore(NodeStore delegate) {
            this.delegate = delegate;
        }

        int ticks() {
            return ticks.get();
        }

        @Override
        public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
            ticks.incrementAndGet();
            delegate.heartbeat(nodeId, state, epoch, at, expiresAt);
        }

        @Override
        public List<StoredNode> findAll() {
            return delegate.findAll();
        }

        @Override
        public int deleteHeartbeatsBefore(Instant cutoff) {
            return delegate.deleteHeartbeatsBefore(cutoff);
        }
    }

    /**
     * One reincarnation per observed death. A heartbeat that keeps failing (a network partition)
     * leaves the last persisted promise in the past on every tick; the epoch must bump — and the
     * WARN must fire — once, when the expiry is first noticed, not once per tick until the database
     * is reachable again.
     */
    @Test
    void aLostHeartbeatBumpsTheEpochOncePerObservedExpiryNotOncePerTick() throws Exception {
        AtomicBoolean partitioned = new AtomicBoolean();
        CountDownLatch firstHeartbeat = new CountDownLatch(1);
        CountDownLatch failedHeartbeats = new CountDownLatch(5);
        CountDownLatch recovered = new CountDownLatch(1);
        List<Long> epochsOffered = new CopyOnWriteArrayList<>();
        NodeStore flaky = new NodeStore() {
            @Override
            public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
                epochsOffered.add(epoch);
                if (partitioned.get()) {
                    failedHeartbeats.countDown();
                    throw new IllegalStateException("simulated network partition: the heartbeat never reaches the database");
                }
                nodeStore.heartbeat(nodeId, state, epoch, at, expiresAt);
                firstHeartbeat.countDown();
                if (failedHeartbeats.getCount() == 0) {
                    recovered.countDown();
                }
            }

            @Override
            public List<StoredNode> findAll() {
                return nodeStore.findAll();
            }

            @Override
            public int deleteHeartbeatsBefore(Instant cutoff) {
                return nodeStore.deleteHeartbeatsBefore(cutoff);
            }
        };
        AtomicInteger bumpsLogged = new AtomicInteger();
        AppenderBase<ILoggingEvent> bumpWatcher = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("epoch bumped")) {
                    bumpsLogged.incrementAndGet();
                }
            }
        };
        bumpWatcher.start();
        engineLogger.addAppender(bumpWatcher);
        Engine engine = newEngine(flaky, List.of());

        engine.start();
        try {
            assertThat(firstHeartbeat.await(5, TimeUnit.SECONDS)).isTrue();
            // The clock moves first: a tick that slips in between the two lines then observes the
            // expiry with the database still reachable and bumps once all the same — the assertions
            // below hold in either interleaving
            clock.advance(LEASE_TTL.plusSeconds(1));
            partitioned.set(true);
            assertThat(failedHeartbeats.await(10, TimeUnit.SECONDS)).isTrue();
            partitioned.set(false);
            assertThat(recovered.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            engine.stop(Duration.ofSeconds(5));
            engineLogger.detachAppender(bumpWatcher);
        }

        assertThat(bumpsLogged).hasValue(1);
        // One transition, 1 → 2, never back: however many ticks ran before the clock moved, and
        // however many heartbeats failed, every offer after the bump carried the reincarnated epoch
        assertThat(epochsOffered).isSorted().containsOnly(1L, 2L);
        assertThat(epochsOffered.getFirst()).isEqualTo(1L);
        assertThat(epochsOffered.getLast()).isEqualTo(2L);
    }

    /** A decorator purely to give the test a deterministic way to wait for N real ticks, without Thread.sleep. */
    private static final class CountingNodeStore implements NodeStore {
        private final NodeStore delegate;
        private final AtomicReference<CountDownLatch> latch;

        CountingNodeStore(NodeStore delegate, CountDownLatch initial) {
            this.delegate = delegate;
            this.latch = new AtomicReference<>(initial);
        }

        void resetLatch(CountDownLatch newLatch) {
            latch.set(newLatch);
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.get().await(timeout, unit);
        }

        @Override
        public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
            delegate.heartbeat(nodeId, state, epoch, at, expiresAt);
            latch.get().countDown();
        }

        @Override
        public List<StoredNode> findAll() {
            return delegate.findAll();
        }

        @Override
        public int deleteHeartbeatsBefore(Instant cutoff) {
            return delegate.deleteHeartbeatsBefore(cutoff);
        }
    }

    /**
     * The gauge is fed by the TICK, never by the scrape: it exists from boot (a series that only
     * appears once there is a backlog cannot be alerted on), and the value it holds is the one the
     * loop sampled — the first tick counts three entries BEFORE claiming them, which is exactly why
     * the number survives the dispatch that follows.
     */
    @Test
    void theBacklogGaugeIsSampledByTheTickAndExistsFromBoot() throws Exception {
        jobStore.upsert(JobDefinition.of("welcome-email", Handler.class, spec -> spec.onDemand()));
        handlerRegistry.register(JobKey.of("welcome-email"), (payload, ctx) -> { });
        recordAndOffer("exec-1", "welcome-email", "a", NOW.minusSeconds(1));
        recordAndOffer("exec-2", "welcome-email", "b", NOW.minusSeconds(1));
        recordAndOffer("exec-3", "welcome-email", "c", NOW.minusSeconds(1));
        MeterRegistry registry = new SimpleMeterRegistry();
        Engine engine = assembleEngine(jobStore, workQueue, leaseStore, historyStore, nodeStore,
                new ExecutionWindowRegistry(List.of()), List.of(), defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL), clock, registry);

        assertThat(registry.get("mohs.queue.depth").gauge().value())
                .as("the series exists before the first tick — an alert cannot tell a missing series from zero")
                .isZero();

        engine.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(registry.get("mohs.queue.depth").gauge().value()).isEqualTo(3.0));
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /**
     * The firing sweep has the claim laps' budget ({@code node-lease-ttl/4}, monotonic): each CAS is
     * bounded by the tick's statement ceiling, but up to {@code FIRE_LIMIT} of them in a row would still
     * outlast the node's lease when a host transaction holds a handful of definition rows. What does
     * not fire in one tick fires in the next — and nothing is lost.
     *
     * <p>A real wait inside the firer, like the punctuality test's real clock: the budget IS elapsed
     * time, and freezing it would erase the behaviour being measured. With a 120 ms node lease the
     * budget is 30 ms, and a firer that takes 40 ms fires exactly one trigger per tick.
     */
    @Test
    void theFiringSweepStopsAtTheTicksBudgetAndResumesNextTick() throws Exception {
        for (int i = 1; i <= 4; i++) {
            jobStore.upsert(JobDefinition.of("poll-" + i, Handler.class, spec -> spec.every(Duration.ofSeconds(10))));
            handlerRegistry.register(JobKey.of("poll-" + i), (payload, ctx) -> { });
        }
        clock.advance(Duration.ofSeconds(15));
        TickCountingNodeStore tickCounting = new TickCountingNodeStore(nodeStore);
        TriggerFirer real = new JdbcTriggerFirer(dataSource, historyStore, workQueue, new H2JdbcDelegate());
        Map<Integer, Integer> firesPerTick = new ConcurrentHashMap<>();
        TriggerFirer slow = (key, observed, next, occurrences, payload, now) -> {
            try {
                Thread.sleep(40); // a slow CAS, deliberately — the budget is wall-clock
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while simulating a slow CAS", e);
            }
            firesPerTick.merge(tickCounting.ticks(), 1, Integer::sum);
            return real.fire(key, observed, next, occurrences, payload, now);
        };
        Engine engine = assembleEngine(jobStore, workQueue, leaseStore, historyStore, tickCounting, slow,
                new ExecutionWindowRegistry(List.of()), List.of(), defaultRunnerRegistry(),
                settingsWithNodeLeaseTtl(Duration.ofMillis(120)), clock, new SimpleMeterRegistry());

        engine.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(firesPerTick.values().stream().mapToInt(Integer::intValue).sum()).isEqualTo(4));
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        assertThat(firesPerTick.values()).as("one trigger per tick: the budget is spent after the first").allMatch(count -> count == 1);
        assertThat(firesPerTick).hasSize(4);
    }

    /**
     * The reaper's sweep has the claim laps' budget too: ten reclaim chunks each waiting out a 3 s
     * deadline would outlast the node's lease. With a 120 ms node lease the budget is 30 ms, and a
     * reclaim that takes 40 ms leaves the second chunk for the next tick — a real wait, like the
     * firing-sweep test's, because the budget IS elapsed time.
     */
    @Test
    void theReclaimSweepStopsAtTheTicksBudgetAndResumesNextTick() {
        seedOrphanedLeasesSpanningTwoReclaimChunks();
        TickCountingNodeStore tickCounting = new TickCountingNodeStore(nodeStore);
        Map<Integer, Integer> reclaimsPerTick = new ConcurrentHashMap<>();
        LeaseStore slowReclaim = mock(LeaseStore.class, delegatesTo(leaseStore));
        doAnswer(invocation -> {
            try {
                Thread.sleep(40); // a slow reclaim, deliberately — the budget is wall-clock
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while simulating a slow reclaim", e);
            }
            reclaimsPerTick.merge(tickCounting.ticks(), 1, Integer::sum);
            return leaseStore.reclaim(invocation.getArgument(0), invocation.getArgument(1));
        }).when(slowReclaim).reclaim(any(), any());
        Engine engine = assembleEngine(jobStore, workQueue, slowReclaim, historyStore, tickCounting,
                new ExecutionWindowRegistry(List.of()), List.of(), defaultRunnerRegistry(),
                settingsWithNodeLeaseTtl(Duration.ofMillis(120)), clock, new SimpleMeterRegistry());

        engine.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(rawJdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM mohs_lease WHERE node_id = 'dead-node'", Integer.class)).isZero());
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        assertThat(reclaimsPerTick.values()).as("one chunk per tick: the budget is spent after the first")
                .allMatch(count -> count == 1);
        assertThat(reclaimsPerTick).hasSize(2);
    }

    /**
     * A death outside the maintenance steps — the heartbeat here, but equally the definitions read,
     * the firing or the claim — used to reach only the log: the alert on {@code mohs.tick.failed}
     * never saw a node whose every tick died, which is the case that matters most.
     */
    @Test
    void aTickThatDiesOutsideAMaintenanceStepIsCountedUnderItsOwnLabel() {
        NodeStore heartbeatBroken = new NodeStore() {
            @Override
            public void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt) {
                throw new RuntimeException("simulated database error on the heartbeat");
            }

            @Override
            public List<StoredNode> findAll() {
                return nodeStore.findAll();
            }

            @Override
            public int deleteHeartbeatsBefore(Instant cutoff) {
                return 0;
            }
        };
        MeterRegistry registry = new SimpleMeterRegistry();
        Engine engine = assembleEngine(jobStore, workQueue, leaseStore, historyStore, heartbeatBroken,
                new ExecutionWindowRegistry(List.of()), List.of(), defaultRunnerRegistry(),
                new EngineSettings(POLL_INTERVAL, BATCH_SIZE, LEASE_TTL), clock, registry);

        engine.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(registry.get("mohs.tick.failed").tag("step", "tick").counter().count())
                            .isGreaterThanOrEqualTo(1.0));
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /**
     * The deduplication window is the row's lifetime, so enforcing the window IS pruning: a key older
     * than {@code idempotency-retention} stops deduplicating because its row is gone. Without this the
     * window was the installation's lifetime — the method existed, was indexed, and nothing called it.
     */
    @Test
    void anIdempotencyKeyOlderThanTheRetentionIsPrunedOnTheTick() throws Exception {
        insertIdempotencyKey("stale", NOW.minus(Duration.ofDays(2)));
        insertIdempotencyKey("fresh", NOW.minus(Duration.ofHours(1)));
        Engine engine = newEngine(nodeStore, List.of(), defaultRunnerRegistry(),
                settingsWithIdempotencyRetention(Duration.ofDays(1)));

        engine.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(rawJdbcTemplate.queryForList("SELECT idempotency_key FROM mohs_idempotency", String.class))
                            .containsExactly("fresh"));
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    /** Zero is the opt-out, not "prune everything": the operator who sets it keeps every key, and the growth that comes with it. */
    @Test
    void aZeroRetentionKeepsEveryIdempotencyKey() throws Exception {
        insertIdempotencyKey("ancient", NOW.minus(Duration.ofDays(3650)));
        Engine engine = newEngine(nodeStore, List.of(), defaultRunnerRegistry(),
                settingsWithIdempotencyRetention(Duration.ZERO));

        engine.start();
        try {
            // A heartbeat row proves a tick completed — the point at which the prune would have run
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(rawJdbcTemplate.queryForObject("SELECT COUNT(*) FROM mohs_nodes", Integer.class)).isPositive());
            assertThat(rawJdbcTemplate.queryForList("SELECT idempotency_key FROM mohs_idempotency", String.class))
                    .containsExactly("ancient");
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    private EngineSettings settingsWithIdempotencyRetention(Duration retention) {
        return new EngineSettings(POLL_INTERVAL, POLL_INTERVAL, BATCH_SIZE, BATCH_SIZE, 1, LEASE_TTL, LEASE_TTL, null,
                EngineSettings.DEFAULT_MISFIRE_THRESHOLD, retention);
    }

    private void insertIdempotencyKey(String key, Instant createdAt) {
        rawJdbcTemplate.update("""
                INSERT INTO mohs_idempotency (job_key, idempotency_key, execution_id, created_at)
                VALUES ('welcome-email', ?, ?, ?)
                """, key, "exec-" + key, JdbcTimestamps.toUtcLocalDateTime(createdAt));
    }

    /**
     * The sweep's loop decision, pinned with a scripted store: passes CHAIN while any table comes
     * back full and STOP on the first pass that came back under the batch everywhere. The budget
     * path stays untested on purpose — forcing it would need an injected monotonic clock for no
     * decision the drained path does not already exercise; and retention left at {@code ZERO} never
     * constructs the cadence, which the constructor makes unrepresentable rather than testable.
     */
    @Test
    void historySweepChainsPassesUntilEveryTableComesBackUnderTheBatch() {
        Queue<HistoryStore.PrunedHistory> script = new ConcurrentLinkedQueue<>(List.of(
                new HistoryStore.PrunedHistory(1000, 1000, 1000),
                new HistoryStore.PrunedHistory(1000, 4, 0),
                new HistoryStore.PrunedHistory(3, 2, 1)));
        List<Instant> cutoffs = new CopyOnWriteArrayList<>();
        HistoryStore scripted = mock(HistoryStore.class, delegatesTo(historyStore));
        doAnswer(invocation -> {
            cutoffs.add(invocation.getArgument(0));
            HistoryStore.PrunedHistory next = script.poll();
            return next != null ? next : new HistoryStore.PrunedHistory(0, 0, 0);
        }).when(scripted).pruneHistoryBefore(any(), anyInt());
        Engine engine = assembleEngine(workQueue, leaseStore, scripted, nodeStore, List.of(),
                defaultRunnerRegistry(), settingsWithHistoryRetention(Duration.ofDays(30)));

        engine.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> cutoffs.size() >= 3);
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }

        // Exactly three: the third pass ended the slot — a fourth call only comes on the next
        // hourly cadence, which this test never reaches.
        assertThat(cutoffs).hasSize(3);
        assertThat(script).isEmpty();
        assertThat(cutoffs).allSatisfy(cutoff -> assertThat(cutoff).isEqualTo(NOW.minus(Duration.ofDays(30))));
    }

    private EngineSettings settingsWithNodeLeaseTtl(Duration nodeLeaseTtl) {
        return new EngineSettings(POLL_INTERVAL, POLL_INTERVAL, BATCH_SIZE, BATCH_SIZE, 1, LEASE_TTL, nodeLeaseTtl, null,
                EngineSettings.DEFAULT_MISFIRE_THRESHOLD, EngineSettings.DEFAULT_IDEMPOTENCY_RETENTION, Duration.ZERO);
    }

    private EngineSettings settingsWithHistoryRetention(Duration retention) {
        return new EngineSettings(POLL_INTERVAL, POLL_INTERVAL, BATCH_SIZE, BATCH_SIZE, 1, LEASE_TTL, LEASE_TTL, null,
                EngineSettings.DEFAULT_MISFIRE_THRESHOLD, EngineSettings.DEFAULT_IDEMPOTENCY_RETENTION, retention);
    }
}
