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
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.store.jdbc.dialect.H2JdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three scenarios that must be proven: a new database (migrating from scratch), an EXISTING
 * installation's database (tables created by hand from the {@code schema-*.sql} files, with no history —
 * the idempotent V1 adopts it without breaking) and a re-execution (a no-op through the history). The
 * equivalent Postgres case lives in {@code MohsFlywayPostgresTest} (Testcontainers).
 */
class MohsFlywayTest {

    private static DataSource freshH2() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:mohs-flyway-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        return h2;
    }

    /** A quoted identifier: Flyway creates the history table case-sensitively in lower case, and H2 upper-cases an unquoted identifier. */
    private static int historyRows(DataSource dataSource) {
        Integer rows = new JdbcTemplate(dataSource).queryForObject(
                "SELECT count(*) FROM \"" + MohsFlyway.HISTORY_TABLE + "\" WHERE \"success\" = TRUE", Integer.class);
        return rows == null ? 0 : rows;
    }

    @Test
    void migratesAFreshDatabaseAndRecordsTheHistory() {
        DataSource dataSource = freshH2();

        new MohsFlyway(dataSource, new H2JdbcDialect()).migrate();

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_execution", Integer.class)).isZero();
        assertThat(historyRows(dataSource)).isGreaterThanOrEqualTo(1);
    }

    /** The adoption: a pre-Flyway installation (its schema applied by hand) gains the history without V1 breaking anything that already exists. */
    @Test
    void adoptsAnExistingInstallationWhoseSchemaWasCreatedByHand() {
        DataSource dataSource = freshH2();
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(dataSource);
        new JdbcTemplate(dataSource).update(
                "INSERT INTO mohs_batches (id, name, total, succeeded, failed, created_at) VALUES ('b1', 'nightly', 1, 0, 0, CURRENT_TIMESTAMP)");

        new MohsFlyway(dataSource, new H2JdbcDialect()).migrate();

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        // The pre-existing data survives the adoption — V1 is a no-op over it
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_batches", Integer.class)).isEqualTo(1);
        assertThat(historyRows(dataSource)).isGreaterThanOrEqualTo(1);
    }

    /**
     * The guardian of two copies of the same truth, in the form V2 demanded: {@code schema-h2.sql} in one
     * database and the Flyway chain (V1 plus V2) in another must produce the SAME structure — the columns
     * and indexes of the {@code mohs_*} tables.
     *
     * <p>Without it, a divergent migration would become a silent no-op during adoption (the guards eat the
     * difference) and production would have a schema the tests' fast path never saw. H2 is the fast
     * guardian; Postgres has its own ({@code MohsFlywayPostgresTest}) because V2's partial index predicate
     * on owner does not exist in H2. MySQL and SQL Server are left to the adoption tests (the chain applied
     * OVER the schema file, failing on conflict) — SQL Server covered by proxy through the PG guardian's
     * filter shape.
     */
    @Test
    void flywayChainMatchesTheSchemaFileStructurally() {
        DataSource fromSchemaFile = freshH2();
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(fromSchemaFile);
        DataSource fromFlyway = freshH2();
        new MohsFlyway(fromFlyway, new H2JdbcDialect()).migrate();

        assertThat(mohsStructure(fromFlyway)).isEqualTo(mohsStructure(fromSchemaFile));
    }

    /** Columns plus indexes of the mohs_* tables (excluding Flyway's history), in a shape comparable by equality. */
    private static List<String> mohsStructure(DataSource dataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<String> structure = new ArrayList<>(jdbc.query("""
                SELECT LOWER(table_name) || '.' || LOWER(column_name) || ':' || data_type || ':' || is_nullable
                FROM information_schema.columns
                WHERE LOWER(table_name) LIKE 'mohs\\_%' AND LOWER(table_name) <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        // Primary-key and UNIQUE constraint index names are auto-generated by H2 (a positional suffix — it
        // changed when V4 started creating and dropping extra objects in the chain), so they are
        // normalised; what is compared is "the primary key / uniqueness exists on these columns"
        structure.addAll(jdbc.query("""
                SELECT LOWER(i.table_name) || '.'
                       || CASE WHEN LOWER(i.index_name) LIKE 'primary\\_key%' THEN 'primary_key'
                               WHEN LOWER(i.index_name) LIKE 'constraint%' THEN 'unique_constraint'
                               ELSE LOWER(i.index_name) END
                       || '(' || LOWER(ic.column_name) || '@' || ic.ordinal_position || ')'
                FROM information_schema.indexes i
                JOIN information_schema.index_columns ic
                  ON ic.index_name = i.index_name AND ic.table_name = i.table_name
                WHERE LOWER(i.table_name) LIKE 'mohs\\_%' AND LOWER(i.table_name) <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        return structure;
    }

    @Test
    void aSecondMigrateIsANoOp() {
        DataSource dataSource = freshH2();
        MohsFlyway flyway = new MohsFlyway(dataSource, new H2JdbcDialect());
        flyway.migrate();
        int after = historyRows(dataSource);

        flyway.migrate();

        assertThat(historyRows(dataSource)).isEqualTo(after);
    }
}
