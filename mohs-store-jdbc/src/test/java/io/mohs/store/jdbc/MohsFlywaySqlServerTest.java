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

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.store.jdbc.dialect.SqlServerJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Flyway on Tier 2 SQL Server — adopting a pre-Flyway installation: V1
 * ({@code IF OBJECT_ID ... CREATE}) passes as a transactional no-op over the existing schema and the
 * history is born. The same safety net as {@code MohsFlywayMySqlTest} — the dialect where it caught the
 * defect.
 */
class MohsFlywaySqlServerTest {

    @Test
    void adoptsThePreFlywaySchemaOnSqlServer() {
        DataSource dataSource = SqlServerTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("IF OBJECT_ID('" + MohsFlyway.HISTORY_TABLE + "', 'U') IS NOT NULL DROP TABLE "
                + MohsFlyway.HISTORY_TABLE);
        MohsFlyway flyway = new MohsFlyway(dataSource, new SqlServerJdbcDialect());

        flyway.migrate();
        flyway.migrate();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM " + MohsFlyway.HISTORY_TABLE + " WHERE success = 1", Integer.class))
                .isGreaterThanOrEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_execution", Integer.class)).isNotNull();
    }
}
