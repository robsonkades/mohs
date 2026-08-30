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

import java.util.Objects;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * Mohs's schema migrations, through the library's OWN Flyway — a separate instance and history table
 * ({@value #HISTORY_TABLE}) from the host's on purpose: Mohs is an embedded library sharing the database
 * with the host application, which may have its own Flyway with its own
 * {@code flyway_schema_history}; hijacking (or colliding with) the host's migration chain would be the
 * classic embedded-library defect.
 *
 * <p>Each dialect's {@code V1__mohs_baseline} is the earlier hand-written schema VERBATIM and, like it,
 * is <b>idempotent</b> ({@code IF NOT EXISTS}/{@code IF OBJECT_ID}): an existing installation (tables
 * created by hand from the {@code schema-*.sql} files, with no history) adopts Flyway by running V1 as a
 * no-op and gaining the history.
 *
 * <p>{@code baselineOnMigrate} with {@code baselineVersion = 0} is a deliberate combination: Flyway
 * requires a baseline to migrate a non-empty schema with no history — and "non-empty" here is the RULE,
 * not the exception, because the schema is shared with the host — but a baseline at 0 skips no migration
 * at all (the trap would be the default of 1, which would mark V1 as applied in a database where only
 * the host's tables exist and would never create Mohs's). V2 and later are ordinary deltas, applied once,
 * through the history.
 */
public final class MohsFlyway {

    private static final Logger log = LoggerFactory.getLogger(MohsFlyway.class);

    /** Never {@code flyway_schema_history} — see the class Javadoc. */
    public static final String HISTORY_TABLE = "mohs_schema_history";

    private final DataSource dataSource;
    private final JdbcDialect dialect;

    public MohsFlyway(DataSource dataSource, JdbcDialect dialect) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    public void migrate() {
        var result = Flyway.configure()
                .dataSource(dataSource)
                .table(HISTORY_TABLE)
                .locations(dialect.migrationLocation())
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate();
        if (result.migrationsExecuted > 0) {
            log.info("mohs schema migrated: {} migration(s) applied up to version {}",
                    result.migrationsExecuted, result.targetSchemaVersion);
        }
    }
}
