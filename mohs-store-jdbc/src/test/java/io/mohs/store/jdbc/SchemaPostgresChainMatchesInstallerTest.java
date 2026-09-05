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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/**
 * The schema ships as two copies of the same truth: {@code schema-postgresql.sql} for a fresh install
 * and the {@code V*.sql} chain for an upgrade. Nothing at runtime reconciles them any more — the
 * library executes no DDL, so a delta that does not keep up with the installer is not a boot failure
 * on our side but a wrong schema on someone else's database, discovered by whoever was upgrading.
 *
 * <p>This test is what keeps the two honest. It builds one database from each path, in the same
 * container, and compares the structure the {@code mohs_*} tables end up with.
 */
class SchemaPostgresChainMatchesInstallerTest {

    @Test
    void theDeltaChainProducesTheSameStructureAsTheInstaller() throws IOException {
        DataSource fromInstaller = PostgresTestSupport.freshEmptyDatabase("mohs_struct_installer");
        new ResourceDatabasePopulator(new ClassPathResource("schema-postgresql.sql")).execute(fromInstaller);

        DataSource fromChain = PostgresTestSupport.freshEmptyDatabase("mohs_struct_chain");
        applyChain(fromChain);

        assertThat(mohsStructure(fromChain))
                .as("a V*.sql that does not keep up with schema-postgresql.sql breaks whoever is upgrading, silently")
                .isEqualTo(mohsStructure(fromInstaller));
    }

    /**
     * One statement per file, deliberately: the Postgres deltas carry {@code DO $$ ... END $$;} blocks
     * whose inner {@code ;} shreds any splitter — which is exactly why the documentation sends the
     * operator to psql rather than to Boot's script initializer. The driver's simple query protocol
     * understands dollar quoting, so the whole file goes over as it was written.
     */
    private static void applyChain(DataSource dataSource) throws IOException {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        for (Resource delta : MigrationChain.deltasOf("postgresql")) {
            jdbc.execute(delta.getContentAsString(StandardCharsets.UTF_8));
        }
    }

    /**
     * Columns — type with its length, precision and scale, nullability, default and collation — plus
     * the complete {@code indexdef}, the partial predicate included, of the {@code mohs_*} tables.
     */
    private static List<String> mohsStructure(DataSource dataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<String> structure = new ArrayList<>(jdbc.query("""
                SELECT table_name || '.' || column_name || ':' || data_type
                       || '(' || COALESCE(character_maximum_length::text, '') || ','
                               || COALESCE(numeric_precision::text, '') || ','
                               || COALESCE(numeric_scale::text, '') || ')'
                       || ':' || is_nullable
                       || ':' || COALESCE(column_default, '<none>')
                       || ':' || COALESCE(collation_name, '-')
                FROM information_schema.columns
                WHERE table_name LIKE 'mohs\\_%'
                ORDER BY 1
                """, (rs, _) -> rs.getString(1)));
        structure.addAll(jdbc.query("""
                SELECT indexdef FROM pg_indexes
                WHERE tablename LIKE 'mohs\\_%'
                ORDER BY 1
                """, (rs, _) -> rs.getString(1)));
        return structure;
    }

}
