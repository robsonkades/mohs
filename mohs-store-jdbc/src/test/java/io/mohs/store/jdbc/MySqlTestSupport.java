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

import com.mysql.cj.jdbc.MysqlDataSource;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.mysql.MySQLContainer;

/**
 * A shared MySQL container — the same design as {@link PostgresTestSupport}: a singleton container, with
 * the schema applied only once when the container starts (MySQL has no {@code CREATE INDEX IF NOT
 * EXISTS} — reapplying the script per test would break from the second test onwards, the index already
 * existing).
 *
 * <p>Isolation through {@code DELETE} rather than {@code TRUNCATE} (MySQL requires disabling
 * {@code FOREIGN_KEY_CHECKS} to truncate a table referenced by a foreign key — a {@code DELETE}
 * respecting the foreign-key order is simpler and does not depend on session state).
 */
final class MySqlTestSupport {

    // The tag is duplicated in .github/workflows/maven.yml, which pre-pulls it while Maven compiles.
    private static final MySQLContainer CONTAINER = new MySQLContainer("mysql:8.0");

    static {
        // A server default that is utf8mb4 but NOT the collation DEFAULT CHARACTER SET utf8mb4
        // resolves to (0900_ai_ci): a table that forgets the clause inherits this one, and the
        // round-trip's collation-equality test sees it — on the image's own default it could not
        CONTAINER.withCommand("mysqld", "--character-set-server=utf8mb4", "--collation-server=utf8mb4_general_ci");
        CONTAINER.start();
        new ResourceDatabasePopulator(new ClassPathResource("schema-mysql.sql")).execute(dataSource());
    }

    private MySqlTestSupport() {
    }

    private static DataSource dataSource() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(CONTAINER.getJdbcUrl());
        dataSource.setUser(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        return dataSource;
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
