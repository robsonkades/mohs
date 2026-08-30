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

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Tier 1 validation criterion: a migration applied to an EXISTING database — the shared container
 * already has the schema applied by {@code schema-postgresql.sql} (the pre-Flyway path), and the
 * adoption writes {@code mohs_schema_history} with the idempotent V1 passing over it without touching
 * anything.
 */
class MohsFlywayPostgresTest {

    /**
     * V5 is the project's ONLY migration that MOVES ROWS, and its copy path was only ever exercised with
     * empty tables: the other tests either start from the already flat schema (where V5 is a no-op) or do
     * not go through Flyway at all.
     *
     * <p>The structural guardian does not cover this class of defect — a pair swapped between columns of
     * the SAME type ({@code job_key}/{@code actor}, {@code correlation_id}/{@code idempotency_key},
     * {@code error_type}/{@code error}) leaves the structure identical and scrambles the customer's
     * database in silence. Hence the ordered concatenation: it catches the swap a schema comparison
     * cannot see.
     *
     * <p>It is also the only place where the {@code RENAME CONSTRAINT} loop and the "it was partitioned"
     * branch run over data.
     */
    @Test
    void v5CarriesEveryHistoryColumnAcrossTheDepartitioning() {
        DataSource dataSource = PostgresTestSupport.freshEmptyDatabase("mohs_v5_copy");
        Flyway.configure()
                .dataSource(dataSource)
                .table(MohsFlyway.HISTORY_TABLE)
                .locations(new PostgresJdbcDialect().migrationLocation())
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .target(MigrationVersion.fromVersion("3")) // stops IN the partitioned era
                .load()
                .migrate();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.update("""
                INSERT INTO mohs_execution (execution_id, job_key, shard, priority, state, scheduled_at, created_at,
                                            finished_at, actor, correlation_id, idempotency_key, payload, payload_type)
                VALUES ('exec-1', 'job-a', 7, 5, 'SUCCEEDED', now(), now(), now(), 'alice', 'corr-1', 'idem-1', '{}', 'java.lang.Object')
                """);
        jdbc.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
                VALUES ('exec-1', 3, 'node-a', now(), now(), 'FAILED', 'java.io.IOException', 'boom')
                """);

        new MohsFlyway(dataSource, new PostgresJdbcDialect()).migrate();

        assertThat(jdbc.queryForObject("""
                SELECT job_key || '|' || actor || '|' || correlation_id || '|' || idempotency_key
                       || '|' || shard || '|' || priority
                  FROM mohs_execution WHERE execution_id = 'exec-1'
                """, String.class))
                .as("every column has to land in ITS column — a swap between columns of the same type is invisible to the structural guardian")
                .isEqualTo("job-a|alice|corr-1|idem-1|7|5");
        assertThat(jdbc.queryForObject("""
                SELECT node_id || '|' || number || '|' || outcome || '|' || error_type || '|' || error
                  FROM mohs_attempt WHERE execution_id = 'exec-1'
                """, String.class)).isEqualTo("node-a|3|FAILED|java.io.IOException|boom");
        assertThat(jdbc.queryForList(
                "SELECT 1 FROM pg_partitioned_table WHERE partrelid = 'mohs_execution'::regclass"))
                .as("the conversion has to have actually happened — otherwise the test above would pass with V5 doing nothing")
                .isEmpty();
    }

    @Test
    void adoptsThePreFlywaySchemaOnPostgres() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("DROP TABLE IF EXISTS " + MohsFlyway.HISTORY_TABLE);

        new MohsFlyway(dataSource, new PostgresJdbcDialect()).migrate();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM \"" + MohsFlyway.HISTORY_TABLE + "\" WHERE \"success\"", Integer.class))
                .isGreaterThanOrEqualTo(1);
        // The pre-existing tables are still there and usable (and V4 dropped the single-table era)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_execution", Integer.class)).isNotNull();
        assertThat(jdbc.queryForList("SELECT 1 FROM information_schema.tables WHERE table_name = 'mohs_executions'")).isEmpty();
    }

    /**
     * The guardian of the two copies of the truth IN the dialect where they diverge most:
     * {@code TIMESTAMPTZ}, storage options and the de-partitioning (V5 recreates both history tables)
     * exist only on Postgres — {@code MohsFlywayTest}'s H2 guardian cannot express them.
     *
     * <p>{@code pg_indexes.indexdef} carries the complete form — a typo in {@code schema-postgresql.sql},
     * or a V-script whose guards eat the difference, fails HERE rather than becoming a silent no-op.
     */
    @Test
    void flywayChainMatchesTheSchemaFileStructurally() {
        DataSource fromSchemaFile = PostgresTestSupport.freshEmptyDatabase("mohs_struct_schema");
        new ResourceDatabasePopulator(new ClassPathResource("schema-postgresql.sql")).execute(fromSchemaFile);
        DataSource fromFlyway = PostgresTestSupport.freshEmptyDatabase("mohs_struct_flyway");
        new MohsFlyway(fromFlyway, new PostgresJdbcDialect()).migrate();

        assertThat(mohsStructure(fromFlyway)).isEqualTo(mohsStructure(fromSchemaFile));
    }

    /** Columns plus the complete {@code indexdef} (including the partial predicate) of the {@code mohs_*} tables, excluding Flyway's history. */
    private static List<String> mohsStructure(DataSource dataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<String> structure = new ArrayList<>(jdbc.query("""
                SELECT table_name || '.' || column_name || ':' || data_type || ':' || is_nullable
                FROM information_schema.columns
                WHERE table_name LIKE 'mohs\\_%' AND table_name <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        structure.addAll(jdbc.query("""
                SELECT indexdef FROM pg_indexes
                WHERE tablename LIKE 'mohs\\_%' AND tablename <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        return structure;
    }
}
