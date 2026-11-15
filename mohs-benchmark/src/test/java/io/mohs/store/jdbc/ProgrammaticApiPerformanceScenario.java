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

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.autoconfigure.MohsAutoConfiguration;
import io.mohs.core.Mohs;
import io.mohs.core.definition.JobRegistration;
import io.mohs.core.definition.OnDemandJob;
import io.mohs.core.job.JobKey;
import io.mohs.core.job.JobRef;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.engine.JobStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Explicitly selected I/O experiment, never a default Surefire test.
 * Fixed-work closed-loop cells; CSV keeps every warmup and measured block.
 * The instrumented pool is fixed at 16. No server or production pool tuning is implied.
 */
class ProgrammaticApiPerformanceScenario {
    static final JobRef<Input> JOB = JobRef.of("perf-action", Input.class);
    static final JobRef<Input> LEGACY = JobRef.of("perf-legacy", Input.class);
    static final int POPULATION = Integer.getInteger("mohs.perf.population", 10_000);
    static final int SAMPLES = Integer.getInteger("mohs.perf.samples", 600);
    static final int BLOCKS = Integer.getInteger("mohs.perf.blocks", 3);
    static final String LABEL = System.getProperty("mohs.perf.label", "run");
    static final Path OUTPUT = Path.of(System.getProperty("mohs.perf.output", "target/performance")).resolve(LABEL);
    static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    static final com.sun.management.OperatingSystemMXBean PROCESS =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public record Input(String company, int revision, String details) {}
    static class Observations {
        final AtomicInteger attempts = new AtomicInteger();
        final LongAdder bytes = new LongAdder();
        void accept(Input input) { bytes.add(input.details().length()); attempts.incrementAndGet(); }
    }
    static class Legacy {
        private final Observations observations;
        Legacy(Observations observations) { this.observations = observations; }
        @OnDemandJob("perf-legacy") public void execute(Input input) { observations.accept(input); }
    }
    @Configuration(proxyBeanMethods = false)
    static class Jobs {
        @Bean Observations observations() { return new Observations(); }
        @Bean JobRegistration<Input> action(Observations observations) {
            return JobRegistration.of(JOB, (input, context) -> observations.accept(input));
        }
        @Bean Legacy legacy(Observations observations) { return new Legacy(observations); }
    }

    @Test
    void measureFixedWorkAndPopulatedStartup() throws Exception {
        Files.createDirectories(OUTPUT);
        Files.writeString(OUTPUT.resolve("measurements.csv"),
                "cell,block,samples,callers,payload_bytes,elapsed_ms,ops_s,p50_ms,p95_ms,p99_ms,jdbc_exec_op,borrows_op,borrow_us_op,caller_alloc_bytes_op,process_cpu_ms,gc_count,gc_ms\n");
        ScenarioBackend backend = ScenarioBackend.current();
        HikariConfig config = new HikariConfig();
        config.setDataSource(backend.freshSchema());
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(16);
        config.setConnectionTimeout(3_000);
        config.setPoolName("mohs-perf");
        try (HikariDataSource pool = new HikariDataSource(config)) {
            String indexes = BindingQueryPlanScenario.indexes(new JdbcTemplate(pool), backend);
            Files.writeString(OUTPUT.resolve("binding-indexes.txt"), indexes);
            if (indexes.contains("idx_mohs_binding_executable_probe")) {
                throw new IllegalStateException("Run query-plan and API scenarios in separate Maven invocations: "
                        + "freshSchema resets data, not indexes");
            }
            // Force every connection to exist before measurement.
            var connections = new ArrayList<java.sql.Connection>();
            for (int i = 0; i < 16; i++) connections.add(pool.getConnection());
            for (var connection : connections) connection.close();
            PerformanceDataSource dataSource = new PerformanceDataSource(pool);
            try (var connection = pool.getConnection()) {
                var metadata = connection.getMetaData();
                Files.writeString(OUTPUT.resolve("environment.txt"),
                        "label=" + LABEL + "\nbackend=" + backend + "\nserver=" + metadata.getDatabaseProductVersion()
                        + "\ndriver=" + metadata.getDriverName() + " " + metadata.getDriverVersion()
                        + "\njava=" + Runtime.version() + "\nprocessors=" + Runtime.getRuntime().availableProcessors()
                        + "\nmaxHeap=" + Runtime.getRuntime().maxMemory() + "\npool=16\npopulation=" + POPULATION
                        + "\nsamples=" + SAMPLES + "\nblocks=" + BLOCKS
                        + "\nflags=" + ManagementFactory.getRuntimeMXBean().getInputArguments() + "\n");
            }
            runner(dataSource, backend).run(context -> {
                assertThat(context).hasNotFailed();
                Mohs mohs = context.getBean(Mohs.class);
                mohs.scheduleRecurring("perf-template", JOB, new Input("seed", 0, "x".repeat(1024)),
                        new IntervalSpec(Duration.ofDays(1), false));
                seedFromTemplate(dataSource, POPULATION);
            });
            for (int block = 0; block <= BLOCKS; block++) {
                measure(dataSource, "startup", block, 1, 1, 1024, _ ->
                        runner(dataSource, backend).run(context -> assertThat(context).hasNotFailed()));
            }
            runner(dataSource, backend).run(context -> {
                assertThat(context).hasNotFailed();
                Mohs mohs = context.getBean(Mohs.class);
                JobStore store = context.getBean(JobStore.class);
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                for (int size : new int[] {1024, 65_536}) {
                    String details = "x".repeat(size);
                    for (int callers : new int[] {1, 8}) {
                        for (int block = 0; block <= BLOCKS; block++) {
                            // The first-release API has one direct submission path.
                            {
                                clearWork(jdbc);
                                int count = block == 0 ? Math.min(SAMPLES, 300) : SAMPLES;
                                measure(dataSource, "submit-direct", block, count, callers, size, i -> {
                                    Input input = new Input(i % 2 == 0 ? "A" : "B", i, details);
                                    mohs.enqueue(JOB, input);
                                });
                                assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_execution", Integer.class)).isEqualTo(count);
                            }
                        }
                    }
                }
                clearWork(jdbc);
                for (int block = 0; block <= BLOCKS; block++) {
                    // Every call addresses a different existing recurrence sharing the same executable.
                    measure(dataSource, "recurring-update", block, Math.min(SAMPLES, POPULATION), 8, 1024, i ->
                            mohs.scheduleRecurring(scheduleKey(i), JOB, new Input("A", i, "x".repeat(1024)),
                                    new IntervalSpec(Duration.ofDays(1), false)));
                    int removed = Math.min(POPULATION, 60);
                    jdbc.update("UPDATE mohs_job_definitions SET retired = ?", false);
                    measure(dataSource, "remove-schedule", block, removed, 1, 1024,
                            i -> mohs.remove(JobKey.of(scheduleKey(POPULATION - 1 - i))));
                    // Restore outside measurement, so every block removes active schedules.
                    jdbc.update("UPDATE mohs_job_definitions SET retired = ?", false);
                }
                // Read projection cell before starting background workers, to isolate its SQL counts.
                mohs.scheduleRecurring("perf-large", JOB, new Input("large", 0, "x".repeat(65_536)),
                        new IntervalSpec(Duration.ofDays(1), false));
                for (int block = 0; block <= BLOCKS; block++) {
                    measure(dataSource, "executable-resolution", block, SAMPLES, 1, 65_536, _ ->
                            assertThat(io.mohs.engine.ProgrammaticDispatchProbe.resolve(JobKey.of("perf-large"),
                                    store, context.getBean(io.mohs.engine.HandlerRegistry.class))).isNotNull());
                }
                // Diagnostic drain cells include engine start/resume and polling observer cost;
                // retain those limits rather than presenting their ratio as a controlled speedup.
                for (String path : new String[] {"legacy", "bound"}) {
                    clearWork(jdbc);
                    JobRef<Input> job = path.equals("legacy") ? LEGACY : JOB;
                    for (int i = 0; i < SAMPLES; i++) mohs.enqueue(job, new Input("drain", i, "x".repeat(1024)));
                    Observations observations = context.getBean(Observations.class);
                    int before = observations.attempts.get();
                    long bytesBefore = observations.bytes.sum();
                    PerformanceDataSource.Snapshot sqlBefore = dataSource.snapshot();
                    long started = System.nanoTime();
                    if (path.equals("legacy")) mohs.lifecycle().start();
                    else mohs.lifecycle().resume();
                    await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_execution WHERE state = 'SUCCEEDED'", Integer.class))
                                    .isEqualTo(SAMPLES));
                    double elapsed = (System.nanoTime() - started) / 1e9;
                    mohs.lifecycle().pause();
                    assertThat(observations.attempts.get() - before).isEqualTo(SAMPLES);
                    assertThat(observations.bytes.sum() - bytesBefore).isEqualTo((long) SAMPLES * 1024);
                    write("drain.txt", path + " samples=" + SAMPLES + " elapsed_s=" + elapsed + " throughput=" + SAMPLES / elapsed
                            + " jdbc_calls=" + (dataSource.snapshot().executions() - sqlBefore.executions()) + "\n");
                }
                captureSqlSummary(dataSource);
            });
        }
    }

    static ApplicationContextRunner runner(DataSource dataSource, ScenarioBackend backend) {
        String dialect = switch (backend) { case POSTGRES -> "postgresql"; case MYSQL -> "mysql"; case SQLSERVER -> "sqlserver"; };
        return new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(MohsAutoConfiguration.class))
                .withUserConfiguration(Jobs.class).withBean(DataSource.class, () -> dataSource)
                .withPropertyValues("mohs.jdbc.dialect=" + dialect, "mohs.lifecycle.start-mode=manual",
                        "mohs.engine.poll-interval=10ms", "mohs.engine.max-poll-interval=10ms");
    }

    /** Copies a named recurrence using the current baseline columns; setup is outside measurement. */
    static void seedFromTemplate(DataSource dataSource, int count) throws Exception {
        List<String> columns = List.of("job_key", "name", "schedule_type", "schedule_revision",
                "cron_expression", "cron_zone", "interval_micros", "interval_after_finish", "runner", "window_name",
                "rate_limit", "misfire", "start_paused", "max_concurrent_executions", "retries", "timeout_micros",
                "retry_policy", "source", "paused", "retired", "next_fire_at", "created_at", "updated_at",
                "executable_key", "recurring_payload", "input_contract");
        String projection = String.join(", ", columns.stream().map(c -> c.equals("job_key") ? "?" : c).toList());
        String sql = "INSERT INTO mohs_job_definitions (" + String.join(", ", columns) + ") SELECT " + projection
                + " FROM mohs_job_definitions WHERE job_key = 'perf-template'";
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var definitions = connection.prepareStatement(sql)) {
                for (int i = 0; i < count; i++) {
                    definitions.setString(1, scheduleKey(i));
                    definitions.addBatch();
                    if ((i + 1) % 250 == 0 || i == count - 1) definitions.executeBatch();
                }
                connection.commit();
            } catch (Exception failure) { connection.rollback(); throw failure; }
        }
    }

    static String scheduleKey(int i) { return "perf-schedule-" + String.format(java.util.Locale.ROOT, "%06d", i); }

    static void clearWork(JdbcTemplate jdbc) {
        for (String table : List.of("mohs_work", "mohs_idempotency", "mohs_execution", "mohs_batches")) {
            jdbc.update("DELETE FROM " + table);
        }
    }

    static void measure(PerformanceDataSource dataSource, String cell, int block, int count, int callers, int bytes,
            IntConsumer operation) throws Exception {
        long[] latencies = new long[count];
        LongAdder allocations = new LongAdder();
        CountDownLatch ready = new CountDownLatch(callers), go = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(callers)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < callers; worker++) {
                int offset = worker;
                futures.add(workers.submit(() -> {
                    ready.countDown();
                    try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                    long allocated = THREADS.getCurrentThreadAllocatedBytes();
                    for (int i = offset; i < count; i += callers) {
                        long begin = System.nanoTime();
                        operation.accept(i);
                        latencies[i] = System.nanoTime() - begin;
                    }
                    allocations.add(THREADS.getCurrentThreadAllocatedBytes() - allocated);
                }));
            }
            ready.await();
            var before = dataSource.snapshot();
            long cpu = PROCESS.getProcessCpuTime(), gcCount = gcCount(), gcTime = gcTime();
            long start = System.nanoTime();
            go.countDown();
            for (Future<?> future : futures) future.get();
            long elapsed = System.nanoTime() - start;
            var after = dataSource.snapshot();
            Arrays.sort(latencies);
            String line = String.format(java.util.Locale.ROOT,
                    "%s,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.1f,%.3f,%d,%d%n",
                    cell, block, count, callers, bytes, elapsed / 1e6, count * 1e9 / elapsed,
                    quantile(latencies, .50) / 1e6, quantile(latencies, .95) / 1e6, quantile(latencies, .99) / 1e6,
                    (after.executions() - before.executions()) / (double) count,
                    (after.borrows() - before.borrows()) / (double) count,
                    (after.borrowNanos() - before.borrowNanos()) / (count * 1e3),
                    allocations.sum() / (double) count, (PROCESS.getProcessCpuTime() - cpu) / 1e6,
                    gcCount() - gcCount, gcTime() - gcTime);
            write("measurements.csv", line);
            System.out.print("PERF " + line);
        }
    }

    static long quantile(long[] values, double q) { return values[Math.max(0, (int) Math.ceil(values.length * q) - 1)]; }
    static long gcCount() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(b -> Math.max(0, b.getCollectionCount())).sum(); }
    static long gcTime() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(b -> Math.max(0, b.getCollectionTime())).sum(); }
    static void write(String file, String text) {
        try { Files.writeString(OUTPUT.resolve(file), text, StandardOpenOption.CREATE, StandardOpenOption.APPEND); }
        catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
    }
    static void captureSqlSummary(PerformanceDataSource dataSource) {
        dataSource.snapshot().statements().forEach((sql, count) -> write("statements.txt", count + "\t" + sql + "\n"));
    }
}
