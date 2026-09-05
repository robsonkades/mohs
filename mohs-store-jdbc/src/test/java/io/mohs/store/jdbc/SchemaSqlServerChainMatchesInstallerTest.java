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
 * The SQL Server counterpart of {@link SchemaPostgresChainMatchesInstallerTest}. The dialect has the
 * one delta that rebuilds a primary key ({@code V8}, the clustered-key change of
 * {@code mohs_idempotency}), which is exactly the kind of drift a structural comparison catches
 * and a round-trip does not: the index's kind (clustered or not), its uniqueness, its columns and
 * their direction, and any filter — compared alongside every column's type, length, precision,
 * scale, nullability and default.
 */
@Tag("docker")
class SchemaSqlServerChainMatchesInstallerTest {

    @Test
    void theDeltaChainProducesTheSameStructureAsTheInstaller() throws IOException {
        DataSource fromInstaller = SqlServerTestSupport.freshEmptyDatabase("mohs_struct_installer");
        new ResourceDatabasePopulator(new ClassPathResource("schema-sqlserver.sql")).execute(fromInstaller);

        DataSource fromChain = SqlServerTestSupport.freshEmptyDatabase("mohs_struct_chain");
        applyChain(fromChain);

        assertThat(mohsStructure(fromChain))
                .as("a V*.sql that does not keep up with schema-sqlserver.sql breaks whoever is upgrading, silently")
                .isEqualTo(mohsStructure(fromInstaller));
    }

    /**
     * One batch per file, deliberately: the T-SQL deltas carry {@code IF ... BEGIN ... END} blocks
     * whose inner {@code ;} shreds any splitter, and none of them uses {@code GO} — so a file is one
     * batch, the way {@code sqlcmd} would send it between two {@code GO}s.
     */
    private static void applyChain(DataSource dataSource) throws IOException {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        for (Resource delta : MigrationChain.deltasOf("sqlserver")) {
            jdbc.execute(delta.getContentAsString(StandardCharsets.UTF_8));
        }
    }

    /**
     * The catalog views answer in the server collation and the literals in the database's, so every
     * name is coerced to the latter before the concatenation — otherwise SQL Server refuses the mix.
     * A primary key or a UNIQUE declared inline gets a generated name ({@code PK__mohs_job__...},
     * {@code UQ__mohs_job__...}) that differs per database, so exactly those generated names compare
     * by role; a named constraint and every other index keep their name, which is contract. Included
     * columns are tagged and ordered after the key, so a key column demoted to INCLUDE is a change.
     */
    private static List<String> mohsStructure(DataSource dataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<String> structure = new ArrayList<>(jdbc.query("""
                SELECT t.name COLLATE DATABASE_DEFAULT + '.' + c.name COLLATE DATABASE_DEFAULT
                       + ':' + ty.name COLLATE DATABASE_DEFAULT
                       + '(' + CAST(c.max_length AS varchar(10)) + ',' + CAST(c.precision AS varchar(10)) + ',' + CAST(c.scale AS varchar(10)) + ')'
                       + ':' + CAST(c.is_nullable AS varchar(1))
                       + ':' + ISNULL(OBJECT_DEFINITION(c.default_object_id), '<none>')
                       + ':' + ISNULL(c.collation_name COLLATE DATABASE_DEFAULT, '-')
                FROM sys.columns c
                JOIN sys.tables t ON t.object_id = c.object_id
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE t.name LIKE 'mohs[_]%'
                ORDER BY 1
                """, (rs, _) -> rs.getString(1)));
        structure.addAll(jdbc.query("""
                SELECT t.name COLLATE DATABASE_DEFAULT + '.'
                       + CASE WHEN i.name LIKE 'PK[_][_]%' THEN 'PRIMARY KEY'
                              WHEN i.name LIKE 'UQ[_][_]%' THEN 'UNIQUE'
                              ELSE i.name COLLATE DATABASE_DEFAULT END
                       + ':' + i.type_desc COLLATE DATABASE_DEFAULT
                       + ':' + CAST(i.is_unique AS varchar(1))
                       + ':' + ISNULL(i.filter_definition COLLATE DATABASE_DEFAULT, '')
                       + ':' + STRING_AGG(c.name COLLATE DATABASE_DEFAULT
                                          + CASE WHEN ic.is_included_column = 1 THEN ' INCLUDE'
                                                 WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE '' END, ',')
                                   WITHIN GROUP (ORDER BY ic.is_included_column, ic.key_ordinal, ic.column_id)
                FROM sys.indexes i
                JOIN sys.tables t ON t.object_id = i.object_id
                JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE t.name LIKE 'mohs[_]%' AND i.type > 0
                GROUP BY t.name, i.name, i.type_desc, i.is_unique, i.filter_definition
                ORDER BY 1
                """, (rs, _) -> rs.getString(1)));
        return structure;
    }
}
