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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
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
import io.mohs.engine.HistoryStore;
import io.mohs.engine.JobBinding;
import io.mohs.engine.JobPayload;
import io.mohs.engine.JobDefinitions;
import io.mohs.store.jdbc.JdbcJobBindingTest.Backend;
import io.mohs.store.jdbc.delegate.H2JdbcDelegate;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;
import io.mohs.test.MutableClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcInlineBindingTest {
    private static final Instant NOW = Instant.parse("2026-09-08T12:00:00Z");
    public record Input(String company, int year) {}

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void installerHasEightTablesAndRejectsIncompleteInputsAndMissingExecutables(Backend backend) throws Exception {
        assertTables(backend.dataSource(), 8);
        var jdbc = new JdbcTemplate(backend.dataSource());
        var store = store(backend);
        JobKey key = JobKey.of("inline-owner");
        store.upsertBound(JobDefinitions.definition(key, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false),
                new JobBinding(key, key, null));
        assertThatThrownBy(() -> jdbc.update("UPDATE mohs_job_definitions SET recurring_payload = ? WHERE job_key = ?", "{}", key.value()))
                .isInstanceOf(DataAccessException.class).hasMessageMatching("(?is).*ck_mohs_job_input.*")
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE mohs_job_definitions SET input_contract = NULL WHERE job_key = ?", key.value()))
                .isInstanceOf(DataAccessException.class).hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(() -> jdbc.update("""
                UPDATE mohs_job_definitions SET executable_key = NULL, recurring_payload = ?, input_contract = ?
                WHERE job_key = ?
                """, "{}", Input.class.getName(), key.value())).isInstanceOf(DataAccessException.class)
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(() -> jdbc.update("""
                UPDATE mohs_job_definitions SET executable_key = ?, recurring_payload = ?, input_contract = ?
                WHERE job_key = ?
                """, "missing", "{}", Input.class.getName(), key.value())).isInstanceOf(DataIntegrityViolationException.class);
        assertThat(store.findBinding(key)).contains(new JobBinding(key, key, null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.mohs.store.jdbc.JdbcJobBindingTest#backends")
    void metadataQueriesExcludeRecurringInputAndHostRollbackRestoresDefinitionAndInput(Backend backend) {
        var store = store(backend);
        JobKey owner = JobKey.of("inline-owner");
        store.upsertBound(JobDefinitions.definition(owner, JobOptions.defaults(), new OnDemandSpec(), Misfire.IGNORE, false),
                new JobBinding(owner, owner, null));
        JobKey key = JobKey.of("inline-monthly");
        var definition = JobDefinitions.definition(key, JobOptions.defaults(), new IntervalSpec(Duration.ofHours(1), false), Misfire.IGNORE, false);
        var input = new JobPayload("{\"company\":\"A\",\"year\":2026}", owner.value());
        var before = store.upsertBound(definition, new JobBinding(key, owner, input));
        var transaction = new TransactionTemplate(new DataSourceTransactionManager(backend.dataSource()));
        transaction.executeWithoutResult(status -> {
            store.upsertBound(JobDefinitions.definition(key, JobOptions.defaults().withName("changed"),
                            new IntervalSpec(Duration.ofHours(2), false), Misfire.FIRE_NOW, false),
                    new JobBinding(key, owner, new JobPayload("{\"company\":\"B\",\"year\":2027}", owner.value())));
            status.setRollbackOnly();
        });
        assertThat(store.find(key)).contains(before);
        assertThat(store.findBinding(key)).contains(new JobBinding(key, owner, input));
        var params = new MapSqlParameterSource("jobKey", key.value()).addValue("retired", false)
                .addValue("paused", false).addValue("orphaned", false).addValue("source", "ANNOTATION")
                .addValue("now", JdbcTimestamps.toUtcLocalDateTime(NOW.plusSeconds(7200))).addValue("limit", 100);
        var jdbc = new NamedParameterJdbcTemplate(backend.dataSource());
        for (String sql : List.of(backend.delegate().findJobByKey(), backend.delegate().findAllJobs(),
                backend.delegate().findDueRecurringJobs())) {
            List<String> columns = jdbc.query(sql, params, rs -> {
                var names = new ArrayList<String>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) names.add(rs.getMetaData().getColumnLabel(i).toLowerCase(java.util.Locale.ROOT));
                return names;
            });
            assertThat(columns).contains("job_key", "executable_key", "input_contract").doesNotContain("recurring_payload");
        }
    }

    private static JdbcJobStore store(Backend backend) {
        return new JdbcJobStore(backend.dataSource(), MutableClock.startingAt(NOW), backend.delegate());
    }

    private static void assertTables(DataSource dataSource, int expected) throws Exception {
        try (var connection = dataSource.getConnection(); var tables = connection.getMetaData()
                .getTables(connection.getCatalog(), null, "%", new String[] {"TABLE"})) {
            var names = new ArrayList<String>();
            while (tables.next()) {
                String name = tables.getString("TABLE_NAME").toLowerCase(java.util.Locale.ROOT);
                if (name.startsWith("mohs_")) names.add(name);
            }
            assertThat(names).hasSize(expected).doesNotContain("mohs_job_bindings");
        }
    }
}
