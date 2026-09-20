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

import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;
import io.github.robsonkades.uuidv7.UUIDv7;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.Priority;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.JobBinding;
import io.mohs.engine.JobDefinitions;
import io.mohs.engine.TriggerFirer;

import static io.mohs.store.jdbc.ProgrammaticApiPerformanceScenario.*;
import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in comparison of definition reads, recurring writes and firing with retained input. */
class RecurringStoragePerformanceScenario {
    @Test
    void measureRecurringStorageAndFiring() throws Exception {
        Files.createDirectories(OUTPUT);
        Files.writeString(OUTPUT.resolve("measurements.csv"),
                "cell,block,samples,callers,payload_bytes,elapsed_ms,ops_s,p50_ms,p95_ms,p99_ms,jdbc_exec_op,borrows_op,borrow_us_op,caller_alloc_bytes_op,process_cpu_ms,gc_count,gc_ms\n");
        var backend = ScenarioBackend.current();
        var delegate = backend.delegate();
        var config = new HikariConfig();
        config.setDataSource(backend.freshSchema());
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(16);
        config.setConnectionTimeout(3_000);
        Instant now = Instant.parse("2026-09-08T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        try (var pool = new HikariDataSource(config)) {
            var dataSource = new PerformanceDataSource(pool);
            var jdbc = new JdbcTemplate(dataSource);
            var store = new JdbcJobStore(dataSource, clock, delegate);
            var history = new JdbcHistoryStore(dataSource, delegate);
            var batches = new JdbcBatchStore(dataSource, delegate);
            var queue = new JdbcWorkQueue(dataSource, delegate);
            var firer = new JdbcSubmissionStore(dataSource, delegate, java.time.Duration.ofDays(7));
            JobKey owner = JobKey.of("perf-action");
            store.upsertBound(JobDefinitions.definition(owner, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false),
                    new JobBinding(owner, owner, null));
            var schedule = new IntervalSpec(Duration.ofDays(1), false);
            store.upsertBound(JobDefinitions.definition(JobKey.of("perf-template"), JobOptions.defaults(), schedule, Misfire.IGNORE, false),
                    new JobBinding(JobKey.of("perf-template"), owner, io.mohs.test.InputFixtures.encode(owner, new Input("A", 2026, "x".repeat(1024)))));
            seedFromTemplate(dataSource, POPULATION);
            try (var connection = pool.getConnection()) {
                Files.writeString(OUTPUT.resolve("environment.txt"), "backend=" + backend + "\nserver="
                        + connection.getMetaData().getDatabaseProductVersion() + "\ndriver=" + connection.getMetaData().getDriverVersion()
                        + "\njava=" + Runtime.version() + "\nprocessors=" + Runtime.getRuntime().availableProcessors()
                        + "\npool=16\npopulation=" + POPULATION + "\nsamples=" + SAMPLES + "\nblocks=" + BLOCKS + "\n");
            }
            int count = Math.min(SAMPLES, POPULATION);
            for (int size : new int[] {1024, 65_536}) {
                var input = io.mohs.test.InputFixtures.encode(owner, new Input("A", 2026, "x".repeat(size)));
                var definitions = java.util.stream.IntStream.range(0, count).mapToObj(i ->
                        JobDefinitions.definition(JobKey.of(scheduleKey(i)), JobOptions.defaults(), schedule, Misfire.IGNORE, false)).toList();
                for (var definition : definitions) store.upsertBound(definition, new JobBinding(definition.key(), owner, input));
                BindingQueryPlanScenario.analyze(jdbc, backend);
                Files.writeString(OUTPUT.resolve("storage-" + size + ".txt"), BindingQueryPlanScenario.sizes(jdbc, backend));
                for (int block = 0; block <= BLOCKS; block++) {
                    clearWork(jdbc);
                    jdbc.update("UPDATE mohs_job_definitions SET next_fire_at = ? WHERE schedule_type = ?",
                            JdbcTimestamps.toUtcLocalDateTime(now), "INTERVAL");
                    measure(dataSource, "due-metadata", block, Math.min(count, 60), 1, size,
                            _ -> assertThat(store.findDueRecurring(now, Math.min(count, 100)).jobs()).hasSize(Math.min(count, 100)));
                    String prefix = size + "-" + block;
                    counters(jdbc, backend, prefix + "-before");
                    measure(dataSource, "bound-upsert", block, count, 8, size, i -> {
                        var definition = definitions.get(i);
                        store.upsertBound(definition, new JobBinding(definition.key(), owner, input));
                    });
                    counters(jdbc, backend, prefix + "-after-upsert");
                    measure(dataSource, "trigger-fire", block, count, 8, size, i -> {
                        JobKey key = definitions.get(i).key();
                        var occurrence = Execution.enqueued(ExecutionId.of(UUIDv7.randomUUIDString()), key, now,
                                Execution.SCHEDULER_ACTOR, Priority.NORMAL);
                        assertThat(firer.fire(key, store.find(key).orElseThrow().scheduleRevision(), now, now.plus(Duration.ofDays(1)), List.of(occurrence), now)).isTrue();
                    });
                    counters(jdbc, backend, prefix + "-after-fire");
                    assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY'", Integer.class)).isEqualTo(count);
                    assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_execution", Integer.class)).isEqualTo(count);
                    assertThat(jdbc.queryForObject("SELECT payload FROM mohs_execution WHERE job_key = ?", String.class,
                            definitions.getFirst().key().value())).isEqualTo(input.json());
                }
            }
        }
    }

    /** Cumulative diagnostic snapshots; background work is included and counters are never reset. */
    private static void counters(JdbcTemplate jdbc, ScenarioBackend backend, String name) throws Exception {
        String values = switch (backend) {
            case POSTGRES -> jdbc.queryForList("SELECT pg_current_wal_insert_lsn() AS wal_position").toString();
            case MYSQL -> jdbc.queryForList("""
                    SHOW GLOBAL STATUS WHERE Variable_name IN
                        ('Innodb_os_log_written', 'Innodb_row_lock_time', 'Innodb_row_lock_waits', 'Bytes_sent', 'Bytes_received')
                    """).toString();
            case SQLSERVER -> jdbc.queryForList("""
                    SELECT SUM(v.num_of_bytes_written) AS log_bytes_written, SUM(v.io_stall_write_ms) AS log_write_stall_ms
                    FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) v JOIN sys.database_files f ON v.file_id = f.file_id
                    WHERE f.type = 1
                    """).toString() + "\n" + jdbc.queryForList("""
                    SELECT wait_type, waiting_tasks_count, wait_time_ms FROM sys.dm_os_wait_stats
                    WHERE wait_type LIKE 'LCK_M_%' AND waiting_tasks_count > 0
                    """).toString();
        };
        Files.writeString(OUTPUT.resolve(name + ".txt"), values);
    }
}
