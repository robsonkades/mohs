package io.mohs.store.jdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.core.resource.ExecutionWindow;
import io.mohs.core.resource.RateLimit;
import io.mohs.engine.EngineSettings;
import io.mohs.engine.SubmissionStore;
import io.mohs.test.InputFixtures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in integrated progress/load experiment. Offers follow deadlines independent of completions. */
@EnabledIfSystemProperty(named = "mohs.coordinator.measure", matches = "true")
class CoordinatorLoadScenario {
    private static final Path OUTPUT = Path.of(System.getProperty("mohs.coordinator.output", "target/coordinator-load"));
    private static final int CLOSED = 257, FUTURE = 4096, RETAINED = 4096, OPEN = 8;
    private static final int TOKENS = 20;
    private final ConcurrentHashMap<ExecutionId, Trace> traces = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> running = new ConcurrentHashMap<>();
    private final AtomicInteger maximumPerJob = new AtomicInteger();
    private final AtomicInteger unexpected = new AtomicInteger();

    private static final class Trace {
        final ExecutionId id;
        final long intended;
        volatile long submit, accepted, started, finished;
        final AtomicInteger invocations = new AtomicInteger();
        Trace(ExecutionId id, long intended) { this.id = id; this.intended = intended; }
    }

    @Test
    void progressesPastClosedPagesAndDrainsAnOpenArrivalBurst() throws Exception {
        Files.createDirectories(OUTPUT);
        var backend = ScenarioBackend.current();
        var dialect = backend.delegate();
        var config = new HikariConfig();
        config.setDataSource(backend.freshSchema());
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(16);
        config.setConnectionTimeout(3000);
        try (var pool = new HikariDataSource(config)) {
            var measured = new PerformanceDataSource(pool);
            var raw = new JdbcTemplate(pool);
            var submissions = new JdbcSubmissionStore(measured, dialect, Duration.ZERO);
            try (var cluster = new ScenarioCluster(measured, Clock.systemUTC(), dialect)) {
                var closed = ExecutionWindow.named("closed").exclude(_ -> true).build();
                cluster.defineJob("a-future", options -> options.withRetries(0));
                cluster.defineJob("a-retained", options -> options.withRetries(0));
                for (int i = 0; i < CLOSED; i++) cluster.defineJob(closedKey(i), options -> options.withWindow("closed").withRetries(0));
                for (int i = 0; i < OPEN; i++) cluster.defineJob(openKey(i), options -> options.withRateLimit("shared")
                        .withMaxConcurrentExecutions(2).withRetries(0));
                cluster.rateLimits().upsert(new RateLimit("shared", TOKENS, Duration.ofSeconds(1)));
                var settings = new EngineSettings(Duration.ofMillis(25), Duration.ofMillis(100), 32, 16, 2,
                        Duration.ofSeconds(30), null, Duration.ofSeconds(60), Duration.ZERO, Duration.ZERO);
                for (int i = 0; i < 2; i++) cluster.addNode(settings, List.of(), List.of(closed));
                for (int i = 0; i < CLOSED; i++) cluster.registerEverywhere(closedKey(i), (_, _) -> unexpected.incrementAndGet());
                cluster.registerEverywhere("a-future", (_, _) -> unexpected.incrementAndGet());
                cluster.registerEverywhere("a-retained", (_, _) -> unexpected.incrementAndGet());
                for (int i = 0; i < OPEN; i++) {
                    String key = openKey(i);
                    cluster.registerEverywhere(key, (_, context) -> {
                        Trace trace = traces.get(context.executionId());
                        if (trace == null) { unexpected.incrementAndGet(); return; }
                        trace.invocations.incrementAndGet();
                        trace.started = System.nanoTime();
                        var active = running.computeIfAbsent(key, _ -> new AtomicInteger());
                        maximumPerJob.accumulateAndGet(active.incrementAndGet(), Math::max);
                        try { Thread.sleep(Duration.ofMillis(20)); }
                        finally { active.decrementAndGet(); trace.finished = System.nanoTime(); }
                    });
                }
                Instant fixtureTime = Instant.now().minusSeconds(10);
                for (int i = 0; i < CLOSED; i++) submissions.accept(List.of(birth(closedKey(i), fixtureTime)), null, () -> {});
                for (int offset = 0; offset < FUTURE; offset += 128) {
                    var group = new ArrayList<SubmissionStore.NewExecution>();
                    for (int i = 0; i < 128; i++) group.add(birth("a-future", fixtureTime.plusSeconds(3600)));
                    submissions.accept(group, null, () -> {});
                }
                Object time = dialect.splitTimestamp(fixtureTime);
                for (int offset = 0; offset < RETAINED; offset += 128) {
                    var rows = new ArrayList<Object[]>();
                    for (int i = 0; i < 128; i++) rows.add(new Object[]{UUIDv7.randomUUIDString(), time, time, time});
                    raw.batchUpdate("INSERT INTO mohs_execution (execution_id,job_key,state,scheduled_at,created_at,finished_at,actor,payload,input_contract) "
                            + "VALUES (?, 'a-retained', 'SUCCEEDED', ?, ?, ?, 'scenario', '{}', 'a-retained')", rows);
                }
                WorkLayoutPerformanceScenario.analyze(pool, backend, WorkLayoutPerformanceScenario.tables(pool));
                var parameters = new MapSqlParameterSource("shard", 0).addValue("afterKey", "")
                        .addValue("now", dialect.splitTimestamp(Instant.now())).addValue("limit", 128L, Types.BIGINT);
                Files.createDirectories(OUTPUT.resolve("plans"));
                BindingQueryPlanScenario.capture(pool, backend, OUTPUT.resolve("plans"), "candidate-prefix", dialect.candidateJobKeys(), parameters);
                try (var connection = pool.getConnection()) {
                    var metadata = connection.getMetaData();
                    Files.writeString(OUTPUT.resolve("environment.txt"), "database=" + metadata.getDatabaseProductVersion()
                            + "\ndriver=" + metadata.getDriverVersion() + "\njava=" + Runtime.version()
                            + "\nbackend=" + backend + "\nprocessors=" + Runtime.getRuntime().availableProcessors()
                            + "\npool=16\nnodes=2 engines in one JVM\nshards=1 deliberately hot\nclosed_jobs=" + CLOSED
                            + "\nfuture_work=" + FUTURE + "\nretained_history=" + RETAINED
                            + "\nopen_jobs=8, max-grants/job=2, shared bucket=20 per second, handler=20ms\n"
                            + "Warmup excluded. Arrival deadlines do not wait for completion. Four producer threads, queue 128; rejection fails the experiment.\n"
                            + "Start latency includes producer delay, acceptance and scheduler backlog. Terminal counts sampled every 100ms.\n"
                            + "JDBC counts include producers and coordinator, not sampling queries. Lock waits sampled every 100ms and can miss short waits.\n");
                }
                Files.writeString(OUTPUT.resolve("measurements.csv"), "phase,offered,accepted,terminal,elapsed_ms,terminal_s,start_p50_ms,start_p95_ms,start_p99_ms,accept_p95_ms,max_producer_lag_ms,max_pending,jdbc_exec,borrows,borrow_ms,cpu_ms,wait_samples,max_waiters,max_handlers_per_job\n");
                cluster.startAll();
                runPhase("warmup", 24, 20, submissions, pool, measured, backend, 0);
                runPhase("steady", 50, 10, submissions, pool, measured, backend, 24);
                runPhase("burst", 300, 200, submissions, pool, measured, backend, 74);
                assertThat(unexpected).hasValue(0);
                assertThat(maximumPerJob.get()).isLessThanOrEqualTo(2);
                assertThat(raw.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state='READY'", Long.class)).isEqualTo(CLOSED + FUTURE);
                assertThat(raw.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state='READY' AND attempt<>1", Long.class)).isZero();
                assertThat(raw.queryForObject("SELECT COUNT(*) FROM mohs_nodes WHERE revoked=?", Long.class, true)).isZero();
            }
        }
    }

    private void runPhase(String phase, int count, int arrivalsPerSecond, JdbcSubmissionStore submissions,
            HikariDataSource pool, PerformanceDataSource measured, ScenarioBackend backend, int prior) throws Exception {
        var before = measured.snapshot();
        long cpuBefore = WorkLayoutPerformanceScenario.CPU.getProcessCpuTime();
        long began = System.nanoTime();
        var cohort = new ArrayList<Trace>();
        var tasks = new ArrayList<Future<?>>();
        var producers = new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(128), new ThreadPoolExecutor.AbortPolicy());
        var raw = new JdbcTemplate(pool);
        var maxPending = new AtomicLong();
        var samplingFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var poller = Executors.newSingleThreadScheduledExecutor();
        poller.scheduleWithFixedDelay(() -> {
            try {
                maxPending.accumulateAndGet(raw.queryForObject(
                        "SELECT COUNT(*) FROM mohs_execution WHERE job_key LIKE 'z-open-%' AND state='PENDING'", Long.class), Math::max);
            } catch (Throwable failure) { samplingFailure.compareAndSet(null, failure); }
        }, 0, 100, TimeUnit.MILLISECONDS);
        try (var waits = new WorkLayoutPerformanceScenario.LockSampler(pool, backend)) {
            for (int i = 0; i < count; i++) {
                long due = began + i * 1_000_000_000L / arrivalsPerSecond;
                while (System.nanoTime() < due) LockSupport.parkNanos(Math.min(1_000_000, due - System.nanoTime()));
                var execution = birth(openKey(i % OPEN), Instant.now());
                var trace = new Trace(execution.executionId(), due);
                cohort.add(trace);
                traces.put(trace.id, trace);
                tasks.add(producers.submit(() -> {
                    trace.submit = System.nanoTime();
                    submissions.accept(List.of(execution), null, () -> {});
                    trace.accepted = System.nanoTime();
                }));
            }
            for (var task : tasks) task.get(30, TimeUnit.SECONDS);
            boolean drained = ScenarioCluster.awaitUntil(Duration.ofSeconds(90), () -> raw.queryForObject(
                    "SELECT COUNT(*) FROM mohs_execution WHERE job_key LIKE 'z-open-%' AND state='SUCCEEDED'", Long.class) == prior + count);
            long ended = System.nanoTime();
            var after = measured.snapshot();
            var waitResult = waits.snapshot();
            assertThat(drained).as("all accepted work must drain past blocked pages").isTrue();
            assertThat(cohort).allSatisfy(t -> { assertThat(t.accepted).isPositive(); assertThat(t.invocations).hasValue(1); });
            long[] starts = cohort.stream().mapToLong(t -> t.started - t.intended).sorted().toArray();
            long[] accepts = cohort.stream().mapToLong(t -> t.accepted - t.submit).sorted().toArray();
            long producerLag = cohort.stream().mapToLong(t -> t.submit - t.intended).max().orElseThrow();
            Files.writeString(OUTPUT.resolve(phase + "-traces.csv"), "execution_id,intended_ns,submit_ns,accepted_ns,started_ns,finished_ns,invocations\n"
                    + cohort.stream().map(t -> t.id + "," + (t.intended-began) + "," + (t.submit-began) + "," + (t.accepted-began) + "," + (t.started-began) + "," + (t.finished-began) + "," + t.invocations + "\n").collect(java.util.stream.Collectors.joining()));
            if (!phase.equals("warmup")) {
                String line = String.format(Locale.ROOT, "%s,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%.3f,%.3f,%d,%d,%d%n",
                        phase,count,count,count,(ended-began)/1e6,count*1e9/(ended-began),quantile(starts,.5),quantile(starts,.95),quantile(starts,.99),quantile(accepts,.95),producerLag/1e6,maxPending.get(),
                        after.executions()-before.executions(),after.borrows()-before.borrows(),(after.borrowNanos()-before.borrowNanos())/1e6,
                        (WorkLayoutPerformanceScenario.CPU.getProcessCpuTime()-cpuBefore)/1e6,waitResult.samples(),waitResult.max(),maximumPerJob.get());
                Files.writeString(OUTPUT.resolve("measurements.csv"), line, java.nio.file.StandardOpenOption.APPEND);
                Files.writeString(OUTPUT.resolve(phase + "-sql.txt"), after.statements().entrySet().stream()
                        .map(e -> (e.getValue()-before.statements().getOrDefault(e.getKey(),0L)) + "\t" + e.getKey() + "\n").collect(java.util.stream.Collectors.joining()));
            }
        } finally {
            producers.shutdownNow();
            poller.shutdownNow();
            assertThat(producers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(poller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(samplingFailure.get()).as("pending-work samples must not silently stop").isNull();
        }
    }

    private static double quantile(long[] sorted, double q) { return sorted[Math.max(0, (int)Math.ceil(sorted.length*q)-1)]/1e6; }
    private static String closedKey(int n) { return String.format(Locale.ROOT, "b-closed-%04d", n); }
    private static String openKey(int n) { return "z-open-" + n; }
    private static SubmissionStore.NewExecution birth(String key, Instant when) {
        var job = JobKey.of(key);
        return new SubmissionStore.NewExecution(ExecutionId.of(UUIDv7.randomUUIDString()), job, 0, 20, when, Instant.now(),
                "scenario", null, null, InputFixtures.encode(job, "x".repeat(1024)), SubmissionStore.Origin.MANUAL, null);
    }
}
