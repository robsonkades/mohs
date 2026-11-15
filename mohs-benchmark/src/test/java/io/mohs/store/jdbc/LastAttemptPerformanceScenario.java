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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.LeaseStore;
import io.mohs.engine.JobDefinitions;
import io.mohs.engine.WorkQueue;

import static io.mohs.store.jdbc.ProgrammaticApiPerformanceScenario.*;
import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in latest-result completion/read/cleanup workload. Historical alternative sources are frozen with their results. */
class LastAttemptPerformanceScenario {
    private static final Instant NOW = Instant.parse("2026-09-08T12:00:00Z");
    private static final JobKey JOB = JobKey.of("attempt-performance");

    @Test
    void compareCompletionReadsAndCleanup() throws Exception {
        Files.createDirectories(OUTPUT);
        Files.writeString(OUTPUT.resolve("measurements.csv"),
                "cell,block,samples,callers,payload_bytes,elapsed_ms,ops_s,p50_ms,p95_ms,p99_ms,jdbc_exec_op,borrows_op,borrow_us_op,caller_alloc_bytes_op,process_cpu_ms,gc_count,gc_ms\n");
        var backend = ScenarioBackend.current();
        var delegate = backend.delegate();
        var config = new HikariConfig();
        config.setDataSource(backend.freshSchema());
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(16);
        try (var pool = new HikariDataSource(config)) {
            var measured = new PerformanceDataSource(pool);
            var jdbc = new JdbcTemplate(measured);
            Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
            var jobs = new JdbcJobStore(measured, clock, delegate);
            var batches = new JdbcBatchStore(measured, delegate);
            var queue = new JdbcWorkQueue(measured, delegate);
            var leases = new JdbcLeaseStore(measured, delegate);
            var history = new JdbcHistoryStore(measured, delegate);
            jobs.upsert(JobDefinitions.definition(JOB, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false));
            new JdbcNodeStore(measured, delegate).register("01991337-0000-7000-8000-00000000000f", io.mohs.core.EngineState.RUNNING,
                    java.time.Duration.ofHours(1), new io.mohs.engine.ExecutableCatalog(java.util.Map.of(JOB, JOB.value())));
            try (var connection = pool.getConnection()) {
                Files.writeString(OUTPUT.resolve("environment.txt"), "backend=" + backend + "\nlayout=work-plus-latest-result"
                        + "\nserver=" + connection.getMetaData().getDatabaseProductVersion()
                        + "\ndriver=" + connection.getMetaData().getDriverVersion()
                        + "\njava=" + Runtime.version() + "\nprocessors=" + Runtime.getRuntime().availableProcessors()
                        + "\npool=16\npopulation=" + POPULATION + "\nsamples=" + SAMPLES + "\nblocks=" + BLOCKS
                        + "\nflags=" + java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments() + "\n");
            }
            var fixtures = new TransactionTemplate(new DataSourceTransactionManager(measured));
            // Fixture construction is outside measurement and should not pay one durable commit per seed row.
            fixtures.executeWithoutResult(_ -> seedHistory(jdbc, backend));
            analyze(jdbc, backend);
            Files.writeString(OUTPUT.resolve("storage-initial.txt"), sizes(jdbc, backend));
            for (int bytes : new int[] {1024, 65536}) {
                for (int block = 0; block <= BLOCKS; block++) {
                    String prefix = bytes + "-" + block;
                    List<String> ids = new ArrayList<>();
                    var births = new ArrayList<Object[]>();
                    for (int i = 0; i < SAMPLES; i++) {
                        String id = io.github.robsonkades.uuidv7.UUIDv7.randomUUIDString();
                        ids.add(id);
                        births.add(new Object[] {id, JOB.value(), delegate.splitTimestamp(NOW), delegate.splitTimestamp(NOW),
                                "\"" + "x".repeat(bytes) + "\""});
                    }
                    fixtures.executeWithoutResult(_ -> jdbc.batchUpdate("""
                            INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, actor, payload, input_contract)
                            VALUES (?, ?, 'PENDING', ?, ?, 'benchmark', ?, 'attempt-performance')
                            """, births));
                    for (int number = 1; number <= 3; number++) {
                        jdbc.update("DELETE FROM mohs_work WHERE work_state = 'READY'");
                        var owners = ids.stream().map(id -> new Object[] {id, JOB.value(), delegate.splitTimestamp(NOW)}).toList();
                        int attemptNumber = number;
                        String claimToken = io.github.robsonkades.uuidv7.UUIDv7.randomUUIDString();
                        fixtures.executeWithoutResult(_ -> jdbc.batchUpdate("INSERT INTO mohs_work (work_state, execution_id, job_key, node_id, claim_token, attempt, priority, claimed_at) VALUES ('LEASED', ?, ?, '01991337-0000-7000-8000-00000000000f', '" + claimToken + "', " + attemptNumber + ", 20, ?)", owners));
                        var results = java.util.stream.IntStream.range(0, SAMPLES).mapToObj(i -> {
                            boolean terminal = attemptNumber == 3;
                            ExecutionState outcome = terminal && i % 2 == 0 ? ExecutionState.SUCCEEDED : ExecutionState.FAILED;
                            var id = ExecutionId.of(ids.get(i));
                            return new LeaseStore.CompletionResult(id, JOB, "01991337-0000-7000-8000-00000000000f", claimToken, attemptNumber,
                                    NOW.minusSeconds(1), NOW, outcome, outcome == ExecutionState.FAILED ? "example.Failure" : null,
                                    outcome == ExecutionState.FAILED ? "e".repeat(bytes) : null, terminal ? null : NOW);
                        }).toList();
                        counters(jdbc, backend, prefix + "-attempt-" + number + "-before");
                        measure(measured, number == 3 ? "terminal-complete" : "retry-complete-" + number, block, SAMPLES, 8, bytes,
                                i -> assertThat(leases.complete(List.of(results.get(i))).get(results.get(i).executionId()).owned()).isTrue());
                        counters(jdbc, backend, prefix + "-attempt-" + number + "-after");
                        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'LEASED'", Integer.class)).isZero();
                    }
                    measure(measured, "execution-detail", block, SAMPLES, 1, bytes,
                            i -> assertThat(history.find(ExecutionId.of(ids.get(i)), NOW)).isPresent());
                    measure(measured, "execution-overview", block, 60, 1, bytes,
                            _ -> assertThat(history.countTerminalOutcomesSince(NOW.minusSeconds(60))).containsKey(ExecutionState.SUCCEEDED));
                    measure(measured, "manual-retry", block, SAMPLES / 2, 8, bytes,
                            i -> assertThat(queue.rearmForManualRetry(ExecutionId.of(ids.get(i * 2 + 1)), NOW)).isTrue());
                    assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM mohs_work WHERE work_state = 'READY' AND  attempt = 4", Integer.class)).isEqualTo(SAMPLES / 2);
                    jdbc.update("DELETE FROM mohs_work WHERE work_state = 'READY'");
                    // Cleanup fixture: close the rearmed rows outside measurement and age only this cohort.
                    fixtures.executeWithoutResult(_ -> jdbc.batchUpdate("UPDATE mohs_execution SET state = 'SUCCEEDED', finished_at = ? WHERE execution_id = ?",
                            ids.stream().map(id -> new Object[] {delegate.splitTimestamp(NOW), id}).toList()));
                    Files.writeString(OUTPUT.resolve("storage-" + prefix + ".txt"), sizes(jdbc, backend));
                }
            }
            analyze(jdbc, backend);
            var planDir = OUTPUT.resolve("plans");
            Files.createDirectories(planDir);
            BindingQueryPlanScenario.capture(pool, backend, planDir, "outcomes", delegate.countTerminalOutcomesSince(),
                    new MapSqlParameterSource("since", delegate.splitTimestamp(NOW.minusSeconds(60))));
            String id = jdbc.queryForObject("SELECT MAX(execution_id) FROM mohs_execution", String.class);
            BindingQueryPlanScenario.capture(pool, backend, planDir, "detail", delegate.findExecutionById(),
                    new MapSqlParameterSource("executionId", id));
            BindingQueryPlanScenario.capture(pool, backend, planDir, "payload", delegate.findPayloads(),
                    new MapSqlParameterSource("ids", id)); // One-item IN: a scalar bind also works in SQL Server's plan helper.
            // UUIDv7 birth time is real (the scheduler clock is fixed); a later cutoff includes these rows.
            Instant cutoff = Instant.parse("2030-01-01T00:00:00Z");
            for (int block = 0; block <= BLOCKS; block++) {
                measure(measured, "history-prune", block, 3, 1, 0,
                        _ -> assertThat(history.pruneHistoryBefore(cutoff, 100).executions()).isEqualTo(100));
            }
            Files.writeString(OUTPUT.resolve("storage-after-prune.txt"), sizes(jdbc, backend));
        }
    }

    private static void seedHistory(JdbcTemplate jdbc, ScenarioBackend backend) {
        var delegate = backend.delegate();
        Object time = delegate.splitTimestamp(NOW.minusSeconds(3600));
        for (int offset = 0; offset < POPULATION; offset += 250) {
            var executions = new ArrayList<Object[]>();
            var attempts = new ArrayList<Object[]>();
            for (int i = offset; i < Math.min(POPULATION, offset + 250); i++) {
                String id = io.github.robsonkades.uuidv7.UUIDv7.randomUUIDString();
                executions.add(new Object[] {id, JOB.value(), time, time, time, "\"" + "x".repeat(1024) + "\""});
                attempts.add(new Object[] {time, time, id});
            }
            jdbc.batchUpdate("""
                    INSERT INTO mohs_execution (execution_id, job_key, state, scheduled_at, created_at, finished_at, actor, payload, input_contract)
                    VALUES (?, ?, 'SUCCEEDED', ?, ?, ?, 'benchmark', ?, 'attempt-performance')
                    """, executions);
            jdbc.batchUpdate("""
                    UPDATE mohs_execution SET last_attempt_number = 3, last_attempt_node_id = '01991337-0001-7000-8000-00000000008e',
                    last_attempt_started_at = ?, last_attempt_finished_at = ?, last_attempt_outcome = 'SUCCEEDED'
                    WHERE execution_id = ?
                    """, attempts);

        }
    }

    private static void analyze(JdbcTemplate jdbc, ScenarioBackend backend) {
        for (String table : List.of("mohs_execution")) {
            switch (backend) {
                case POSTGRES -> jdbc.execute("ANALYZE " + table);
                case MYSQL -> jdbc.execute("ANALYZE TABLE " + table);
                case SQLSERVER -> jdbc.execute("UPDATE STATISTICS " + table + " WITH FULLSCAN");
            }
        }
    }

    private static String sizes(JdbcTemplate jdbc, ScenarioBackend backend) {
        return switch (backend) {
            case POSTGRES -> jdbc.queryForList("""
                    SELECT relname, pg_total_relation_size(oid) AS bytes FROM pg_class
                    WHERE relname IN ('mohs_execution') AND relkind = 'r' ORDER BY relname
                    """).toString();
            case MYSQL -> jdbc.queryForList("""
                    SELECT table_name, data_length, index_length FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name IN ('mohs_execution')
                    """).toString();
            case SQLSERVER -> jdbc.queryForList("""
                    SELECT OBJECT_NAME(object_id) AS table_name, SUM(reserved_page_count) * 8192 AS bytes
                    FROM sys.dm_db_partition_stats WHERE object_id IN (OBJECT_ID('mohs_execution'))
                    GROUP BY object_id
                    """).toString();
        };
    }

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
                    """).toString();
        };
        Files.writeString(OUTPUT.resolve(name + ".txt"), values);
    }
}
