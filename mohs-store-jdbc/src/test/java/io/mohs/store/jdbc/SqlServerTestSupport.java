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

import javax.sql.DataSource;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

/**
 * A shared SQL Server container — the same design as {@link PostgresTestSupport}: a singleton container,
 * with the schema applied only once when the container starts. Isolation through {@code DELETE} (T-SQL
 * forbids {@code TRUNCATE} on a table referenced by a foreign key, even when the child table is empty —
 * it is not merely a bypassable check as in MySQL).
 *
 * <p>The schema lives in a dedicated {@code mohs} database with {@code READ_COMMITTED_SNAPSHOT ON} —
 * the dialect's boot requirement — so every SQL Server test runs the configuration a supported
 * deployment actually has, not the container's locking default. {@code master}, which cannot enable
 * the setting, stays reachable through {@link #dataSourceFor(String)} as the permanent negative case.
 */
final class SqlServerTestSupport {

    // The tag is duplicated in .github/workflows/maven.yml, which pre-pulls it while Maven compiles.
    private static final MSSQLServerContainer CONTAINER = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2025-latest")
            .acceptLicense();

    static {
        CONTAINER.start();
        JdbcTemplate master = new JdbcTemplate(dataSourceFor("master"));
        master.execute("CREATE DATABASE mohs");
        // No other session exists yet, so the exclusive access the switch demands is free here.
        master.execute("ALTER DATABASE mohs SET READ_COMMITTED_SNAPSHOT ON");
        new ResourceDatabasePopulator(new ClassPathResource("schema-sqlserver.sql")).execute(dataSource());
    }

    private SqlServerTestSupport() {
    }

    private static DataSource dataSource() {
        return dataSourceFor("mohs");
    }

    static DataSource dataSourceFor(String databaseName) {
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(CONTAINER.getJdbcUrl());
        dataSource.setUser(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        dataSource.setDatabaseName(databaseName);
        return dataSource;
    }

    /**
     * A NEW, empty database in the same container — for the structural guardian (the installer versus the
     * delta chain), which needs two schemas built from scratch by different paths.
     */
    static DataSource freshEmptyDatabase(String name) {
        JdbcTemplate master = new JdbcTemplate(dataSourceFor("master"));
        master.execute("IF DB_ID('" + name + "') IS NOT NULL DROP DATABASE " + name);
        master.execute("CREATE DATABASE " + name);
        return dataSourceFor(name);
    }

    /** Clears every table — the schema was applied once, when the container started. */
    static DataSource freshSchema() {
        DataSource dataSource = dataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("DELETE FROM mohs_job_definitions");
        jdbcTemplate.execute("DELETE FROM mohs_batches");
        jdbcTemplate.execute("DELETE FROM mohs_rate_limits");
        jdbcTemplate.execute("DELETE FROM mohs_nodes");
        jdbcTemplate.execute("DELETE FROM mohs_ready");
        jdbcTemplate.execute("DELETE FROM mohs_lease");
        jdbcTemplate.execute("DELETE FROM mohs_attempt");
        jdbcTemplate.execute("DELETE FROM mohs_execution");
        jdbcTemplate.execute("DELETE FROM mohs_idempotency");
        return dataSource;
    }
}
