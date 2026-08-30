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

import io.mohs.store.jdbc.dialect.MySqlJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Flyway on Tier 2 MySQL — the scenario the bench caught broken: MySQL's {@code CREATE INDEX} had no
 * guard ({@code IF NOT EXISTS} does not exist for an index), so adopting a pre-Flyway installation
 * failed with 1061 in the MIDDLE of the script (DDL commits implicitly), left {@code success=false} in
 * the history and made every subsequent boot die in validation — a permanent loop. A V1 guarded by
 * {@code information_schema} plus dynamic SQL is the fix; the adoption here is the regression guard.
 */
class MohsFlywayMySqlTest {

    @Test
    void adoptsThePreFlywaySchemaOnMySql() {
        DataSource dataSource = MySqlTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("DROP TABLE IF EXISTS " + MohsFlyway.HISTORY_TABLE);
        MohsFlyway flyway = new MohsFlyway(dataSource, new MySqlJdbcDialect());

        flyway.migrate();
        // Re-execution: a no-op through the history — if V1 had failed midway, this second call would be
        // the boot loop of the finding
        flyway.migrate();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM " + MohsFlyway.HISTORY_TABLE + " WHERE success = 1", Integer.class))
                .isGreaterThanOrEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_execution", Integer.class)).isNotNull();
    }
}
