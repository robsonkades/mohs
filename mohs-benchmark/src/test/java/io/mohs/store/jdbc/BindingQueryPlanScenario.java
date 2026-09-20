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
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterUtils;
import tools.jackson.databind.json.JsonMapper;
import io.mohs.core.definition.JobOptions;
import io.mohs.core.job.JobKey;
import io.mohs.core.schedule.IntervalSpec;
import io.mohs.core.schedule.Misfire;
import io.mohs.core.schedule.OnDemandSpec;
import io.mohs.engine.JobBinding;
import io.mohs.engine.JobDefinitions;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual plans and bounded JDBC timings on an isolated populated database; explicitly opt-in. */
class BindingQueryPlanScenario {
    @Test
    void captureInstalledBindingPlans() throws Exception {
        var backend = ScenarioBackend.current();
        var delegate = backend.delegate();
        Path output = ProgrammaticApiPerformanceScenario.OUTPUT.resolve("plans");
        Files.createDirectories(output);
        HikariConfig config = new HikariConfig();
        config.setDataSource(backend.freshSchema());
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(3_000);
        try (var pool = new HikariDataSource(config)) {
            var clock = Clock.systemUTC();
            var store = new JdbcJobStore(pool, clock, delegate);
            var history = new JdbcHistoryStore(pool, delegate);
            for (String key : List.of("perf-action", "perf-sparse", "perf-empty")) {
                var job = JobKey.of(key);
                store.upsertBound(JobDefinitions.definition(job, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false),
                        new JobBinding(job, job, null));
            }
            var input = io.mohs.test.InputFixtures.encode(JobKey.of("perf-action"), new ProgrammaticApiPerformanceScenario.Input("seed", 0, "x".repeat(1024)));
            var template = JobKey.of("perf-template");
            store.upsertBound(JobDefinitions.definition(template, JobOptions.defaults(),
                    new IntervalSpec(Duration.ofDays(1), false), Misfire.IGNORE, false),
                    new JobBinding(template, JobKey.of("perf-action"), input));
            ProgrammaticApiPerformanceScenario.seedFromTemplate(pool, ProgrammaticApiPerformanceScenario.POPULATION);
            var jdbc = new JdbcTemplate(pool);
            String bindingTable = "mohs_job_definitions";
            for (int i = 0; i < 10; i++) {
                jdbc.update("UPDATE " + bindingTable + " SET executable_key = ? WHERE job_key = ?",
                        "perf-sparse", ProgrammaticApiPerformanceScenario.scheduleKey(i));
            }
            analyze(jdbc, backend);
            Files.writeString(output.resolve("indexes.txt"), indexes(jdbc, backend));
            Files.writeString(output.resolve("storage.txt"), sizes(jdbc, backend));
            try (Connection connection = pool.getConnection()) {
                Files.writeString(output.resolve("environment.txt"), connection.getMetaData().getDatabaseProductVersion()
                        + "\n" + connection.getMetaData().getDriverName() + " " + connection.getMetaData().getDriverVersion());
            }
            var named = new NamedParameterJdbcTemplate(pool);
            for (String key : List.of("perf-empty", "perf-action", "perf-sparse")) {
                var params = new MapSqlParameterSource("jobKey", key).addValue("retired", false);
                assertThat(store.hasActiveDependents(JobKey.of(key))).isEqualTo(!key.equals("perf-empty"));
                capture(pool, backend, output, "dependents-" + key, delegate.hasActiveJobDependents(), params);
                timeQuery(named, output, "dependents-" + key, delegate.hasActiveJobDependents(), params);
            }
            var params = new MapSqlParameterSource("jobKey", "perf-template").addValue("retired", false)
                    .addValue("paused", false).addValue("orphaned", false).addValue("limit", 100)
                    .addValue("now", JdbcTimestamps.toUtcLocalDateTime(clock.instant().plus(Duration.ofDays(2))));
            capture(pool, backend, output, "executable-key", delegate.findExecutableKey(), params);
            capture(pool, backend, output, "definition", delegate.findJobByKey(), params);
            capture(pool, backend, output, "due", delegate.findDueRecurringJobs(), params);
            if (delegate.lockBindingOwner().stripLeading().startsWith("SELECT")) {
                capture(pool, backend, output, "owner-lock", delegate.lockBindingOwner(), params);
            }
            timeQuery(named, output, "definition", delegate.findJobByKey(), params);
            timeQuery(named, output, "due", delegate.findDueRecurringJobs(), params);
            // Materialize keys first: MySQL rejects selecting the target table in an UPDATE subquery.
            var retiredKeys = jdbc.queryForList("SELECT job_key FROM " + bindingTable
                    + " WHERE executable_key = ? AND job_key <> ?", String.class, "perf-sparse", "perf-sparse");
            for (String key : retiredKeys) jdbc.update("UPDATE mohs_job_definitions SET retired = ? WHERE job_key = ?", true, key);
            analyze(jdbc, backend);
            var retired = new MapSqlParameterSource("jobKey", "perf-sparse").addValue("retired", false);
            assertThat(store.hasActiveDependents(JobKey.of("perf-sparse"))).isFalse();
            capture(pool, backend, output, "dependents-retired", delegate.hasActiveJobDependents(), retired);
        }
    }

    static void analyze(JdbcTemplate jdbc, ScenarioBackend backend) {
        for (String table : List.of("mohs_job_definitions")) {
            switch (backend) {
                case POSTGRES -> jdbc.execute("VACUUM (ANALYZE) " + table);
                case MYSQL -> jdbc.execute("ANALYZE TABLE " + table);
                case SQLSERVER -> jdbc.execute("UPDATE STATISTICS " + table + " WITH FULLSCAN");
            }
        }
    }

    static void capture(HikariDataSource pool, ScenarioBackend backend, Path output, String name,
            String query, MapSqlParameterSource params) throws Exception {
        Files.writeString(output.resolve(name + ".sql"), query + "\n-- parameters: " + params.getValues() + "\n");
        if (backend == ScenarioBackend.POSTGRES || backend == ScenarioBackend.MYSQL) {
            String prefix = backend == ScenarioBackend.POSTGRES
                    ? "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) " : "EXPLAIN ANALYZE ";
            var lines = new NamedParameterJdbcTemplate(pool).query(prefix + query, params, (rs, _) -> rs.getString(1));
            Files.writeString(output.resolve(name + (backend == ScenarioBackend.POSTGRES ? ".json" : ".txt")), String.join("\n", lines));
        } else {
            // Keep settings and prepared query on the same connection; consume both data and actual-plan results.
            try (Connection connection = pool.getConnection(); Statement settings = connection.createStatement()) {
                settings.execute("SET STATISTICS XML ON");
                settings.execute("SET STATISTICS IO ON");
                var parsed = NamedParameterUtils.parseSqlStatement(query);
                String sql = NamedParameterUtils.substituteNamedParameters(parsed, params);
                Object[] values = NamedParameterUtils.buildValueArray(parsed, params, null);
                StringBuilder plans = new StringBuilder(), messages = new StringBuilder();
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < values.length; i++) {
                        // NamedParameterUtils preserves explicitly declared types as SqlParameterValue.
                        // Use the same binder as JDBC templates, including BIGINT limits and timestamps.
                        org.springframework.jdbc.core.StatementCreatorUtils.setParameterValue(statement, i + 1,
                                org.springframework.jdbc.core.SqlTypeValue.TYPE_UNKNOWN, values[i]);
                    }
                    boolean result = statement.execute();
                    while (true) {
                        if (result) {
                            try (ResultSet rows = statement.getResultSet()) {
                                boolean plan = rows.getMetaData().getColumnCount() == 1;
                                while (rows.next()) {
                                    if (plan) {
                                        String value = rows.getString(1);
                                        if (value != null && value.contains("<ShowPlanXML")) plans.append(value).append("\n");
                                    }
                                }
                            }
                        } else if (statement.getUpdateCount() == -1) break;
                        result = statement.getMoreResults();
                    }
                    for (var warning = statement.getWarnings(); warning != null; warning = warning.getNextWarning()) {
                        messages.append(warning.getMessage()).append("\n");
                    }
                } finally {
                    settings.execute("SET STATISTICS XML OFF");
                    settings.execute("SET STATISTICS IO OFF");
                }
                assertThat(plans).as("SQL Server must return an actual plan").isNotEmpty();
                Files.writeString(output.resolve(name + ".sqlplan"), plans);
                Files.writeString(output.resolve(name + "-io.txt"), messages);
            }
        }
    }

    static void timeQuery(NamedParameterJdbcTemplate jdbc, Path output, String name,
            String query, MapSqlParameterSource params) throws Exception {
        long[] times = new long[60];
        long characters = 0, rowsRead = 0;
        for (int i = -10; i < times.length; i++) {
            long start = System.nanoTime();
            var rows = jdbc.queryForList(query, params); // Same full materialization as the original application operation.
            long chars = 0;
            for (Map<String, Object> row : rows) for (Object value : row.values()) if (value instanceof String s) chars += s.length();
            if (i >= 0) { times[i] = System.nanoTime() - start; characters += chars; rowsRead += rows.size(); }
        }
        java.util.Arrays.sort(times);
        Files.writeString(output.resolve("query-times.csv"), String.format(java.util.Locale.ROOT,
                "%s,%d,%.3f,%.3f,%.3f,%.1f,%.1f%n", name, times.length,
                ProgrammaticApiPerformanceScenario.quantile(times, .5) / 1e6,
                ProgrammaticApiPerformanceScenario.quantile(times, .95) / 1e6,
                ProgrammaticApiPerformanceScenario.quantile(times, .99) / 1e6,
                rowsRead / (double) times.length, characters / (double) times.length),
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
    }

    static String indexes(JdbcTemplate jdbc, ScenarioBackend backend) {
        String table = "mohs_job_definitions";
        return switch (backend) {
            case POSTGRES -> jdbc.queryForList("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = ?", table).toString();
            case MYSQL -> jdbc.queryForList("SHOW INDEX FROM " + table).toString();
            case SQLSERVER -> jdbc.queryForList("""
                    SELECT i.name, c.name AS column_name, ic.key_ordinal
                    FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE i.object_id = OBJECT_ID(?) ORDER BY i.name, ic.key_ordinal
                    """, table).toString();
        };
    }

    static String sizes(JdbcTemplate jdbc, ScenarioBackend backend) {
        return switch (backend) {
            case POSTGRES -> jdbc.queryForList("""
                    SELECT relname, pg_total_relation_size(oid) AS bytes
                    FROM pg_class WHERE relname IN ('mohs_job_definitions')
                    """).toString();
            case MYSQL -> jdbc.queryForList("""
                    SELECT table_name, data_length, index_length FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name IN ('mohs_job_definitions')
                    """).toString();
            case SQLSERVER -> jdbc.queryForList("""
                    SELECT OBJECT_NAME(p.object_id) AS table_name, SUM(p.reserved_page_count) * 8192 AS bytes
                    FROM sys.dm_db_partition_stats p
                    WHERE OBJECT_NAME(p.object_id) IN ('mohs_job_definitions')
                    GROUP BY p.object_id
                    """).toString();
        };
    }
}
