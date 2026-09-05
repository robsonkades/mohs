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
 * The MySQL counterpart of {@link SchemaPostgresChainMatchesInstallerTest}: {@code schema-mysql.sql}
 * for a fresh install and the {@code V*.sql} chain for an upgrade are two copies of one truth, and
 * nothing at runtime reconciles them. Both paths are built in the same container and compared —
 * columns with their type, nullability, default and collation; indexes with their columns and
 * direction; and each table's collation, which is what the utf8mb4 repair delta exists for.
 */
@Tag("docker")
class SchemaMySqlChainMatchesInstallerTest {

    @Test
    void theDeltaChainProducesTheSameStructureAsTheInstaller() throws IOException {
        DataSource fromInstaller = MySqlTestSupport.freshEmptyDatabase("mohs_struct_installer");
        new ResourceDatabasePopulator(new ClassPathResource("schema-mysql.sql")).execute(fromInstaller);

        DataSource fromChain = MySqlTestSupport.freshEmptyDatabase("mohs_struct_chain");
        applyChain(fromChain);

        assertThat(mohsStructure(fromChain))
                .as("a V*.sql that does not keep up with schema-mysql.sql breaks whoever is upgrading, silently")
                .isEqualTo(mohsStructure(fromInstaller));
    }

    /**
     * Split on {@code ;} like the installer is: the MySQL deltas are plain statements — the guarded
     * ones go through {@code SET @sql / PREPARE / EXECUTE}, one statement each, with no delimiter
     * change — so the same script initializer the documentation recommends applies them as written.
     */
    private static void applyChain(DataSource dataSource) throws IOException {
        for (Resource delta : MigrationChain.deltasOf("mysql")) {
            new ResourceDatabasePopulator(delta).execute(dataSource);
        }
    }

    /** Each table's collation, then its columns, then its indexes — all of the {@code mohs_*} tables. */
    private static List<String> mohsStructure(DataSource dataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<String> structure = new ArrayList<>(jdbc.query("""
                SELECT CONCAT(table_name, ':', table_collation)
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name LIKE 'mohs\\_%'
                ORDER BY 1
                """, (rs, _) -> rs.getString(1)));
        structure.addAll(jdbc.query("""
                SELECT CONCAT(table_name, '.', column_name, ':', column_type, ':', is_nullable, ':',
                              IFNULL(column_default, '<none>'), ':', IFNULL(collation_name, '-'), ':', IFNULL(extra, ''))
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name LIKE 'mohs\\_%'
                ORDER BY 1
                """, (rs, _) -> rs.getString(1)));
        structure.addAll(jdbc.query("""
                SELECT CONCAT(table_name, '.', index_name, ':', non_unique, ':',
                              GROUP_CONCAT(CONCAT(column_name, IF(sub_part IS NULL, '', CONCAT('(', sub_part, ')')),
                                                  IF(collation = 'D', ' DESC', ''))
                                           ORDER BY seq_in_index SEPARATOR ','))
                FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name LIKE 'mohs\\_%'
                GROUP BY table_name, index_name, non_unique
                ORDER BY 1
                """, (rs, _) -> rs.getString(1)));
        return structure;
    }
}
