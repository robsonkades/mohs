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

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * A Postgres container shared between the test classes that validate against a real database — a
 * singleton container: it starts once per test JVM and is never stopped explicitly (Testcontainers' Ryuk
 * handles cleanup at the JVM's end), avoiding the cost of a new container per class.
 *
 * <p>Isolation between tests does not come from a new database per test (as {@code freshH2DataSource()}
 * does through a unique in-memory URL) — it comes from {@code TRUNCATE ... CASCADE} before each test, in
 * the same shared schema.
 */
final class PostgresTestSupport {

    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:16-alpine");

    static {
        CONTAINER.start();
        DataSource dataSource = dataSource();
        // The schema is applied once, here — not in freshSchema(), which runs before each test against the
        // same container: reapplying schema-postgresql.sql is idempotent (CREATE TABLE IF NOT EXISTS), but
        // it is unnecessary, repeated work.
        new ResourceDatabasePopulator(new ClassPathResource("schema-postgresql.sql")).execute(dataSource);
    }

    private PostgresTestSupport() {
    }

    private static DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(CONTAINER.getJdbcUrl());
        dataSource.setUser(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        return dataSource;
    }

    /** Clears every table — the schema was applied once, when the container started. */
    static DataSource freshSchema() {
        DataSource dataSource = dataSource();
        new JdbcTemplate(dataSource).execute(
                "TRUNCATE TABLE mohs_job_definitions, mohs_batches, mohs_rate_limits, mohs_nodes, mohs_ready, mohs_lease, mohs_execution, mohs_attempt, mohs_idempotency CASCADE");
        return dataSource;
    }

    /**
     * A NEW, empty database in the same container — for the structural guardian (schema file versus the
     * Flyway chain), which needs two schemas built from scratch by different paths, something the shared
     * database (its schema applied in the static block) does not offer.
     */
    static DataSource freshEmptyDatabase(String name) {
        new JdbcTemplate(dataSource()).execute("DROP DATABASE IF EXISTS " + name + "; CREATE DATABASE " + name);
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerNames(new String[] { CONTAINER.getHost() });
        dataSource.setPortNumbers(new int[] { CONTAINER.getMappedPort(5432) });
        dataSource.setDatabaseName(name);
        dataSource.setUser(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        return dataSource;
    }
}
