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

import java.util.Locale;
import java.util.function.Supplier;

import javax.sql.DataSource;

import io.mohs.store.jdbc.delegate.JdbcDelegate;
import io.mohs.store.jdbc.delegate.MySqlJdbcDelegate;
import io.mohs.store.jdbc.delegate.PostgresJdbcDelegate;
import io.mohs.store.jdbc.delegate.SqlServerJdbcDelegate;

/**
 * Which database a scenario runs against — the seam that was missing while the whole bench was
 * wired to {@code PostgresJdbcDelegate} plus {@code PostgresTestSupport} by hand.
 *
 * <p>The gap it closes is specific: concurrent claim, ownership reclaim and batch closing under
 * contention were measured on the reference dialect ONLY, and those three are exactly where the
 * dialects differ — {@code FOR UPDATE SKIP LOCKED} on PostgreSQL and MySQL against
 * {@code READPAST}/{@code UPDLOCK} on SQL Server is not the same lock manager wearing two names.
 *
 * <p>Selected per run rather than per class:
 *
 * <pre>{@code
 * ./mvnw -pl mohs-benchmark test -Dtest=NodeChurnScenario -Dmohs.scenario.backend=sqlserver
 * }</pre>
 *
 * <p>A run targets ONE backend on purpose. Three containers of three different engines in a single
 * JVM would measure the machine's contention, not the database's — and the number this bench exists
 * to produce is per dialect anyway.
 */
enum ScenarioBackend {

    POSTGRES(PostgresTestSupport::freshSchema, PostgresJdbcDelegate::new),
    SQLSERVER(SqlServerTestSupport::freshSchema, SqlServerJdbcDelegate::new),
    MYSQL(MySqlTestSupport::freshSchema, MySqlJdbcDelegate::new);

    /** The property a run overrides; absent means the reference dialect, so an unqualified run keeps measuring what it always measured. */
    static final String PROPERTY = "mohs.scenario.backend";

    private final Supplier<DataSource> freshSchema;
    private final Supplier<JdbcDelegate> delegate;

    ScenarioBackend(Supplier<DataSource> freshSchema, Supplier<JdbcDelegate> delegate) {
        this.freshSchema = freshSchema;
        this.delegate = delegate;
    }

    static ScenarioBackend current() {
        String selected = System.getProperty(PROPERTY, POSTGRES.name());
        try {
            return valueOf(selected.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("-D" + PROPERTY + "=" + selected
                    + " is not a scenario backend — use postgres, sqlserver or mysql", e);
        }
    }

    /** A schema of its own per call, like every scenario already assumed: a bench that inherits rows measures the previous run. */
    DataSource freshSchema() {
        return freshSchema.get();
    }

    JdbcDelegate delegate() {
        return delegate.get();
    }
}
