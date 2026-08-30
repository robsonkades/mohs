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
/**
 * The few real SQL dialect divergences between the supported databases — modelled on Hibernate's
 * {@code LimitHandler}/{@code LockingStrategy} shape (small interfaces, one concern each), without
 * taking Hibernate as a dependency.
 *
 * <p>{@link io.mohs.store.jdbc.dialect.JdbcDialect} isolates the dialect-sensitive queries from
 * {@code io.mohs.store.jdbc} (the {@code mohs_ready} claim: the position of {@code LIMIT}/{@code TOP},
 * {@code SKIP LOCKED} versus a table hint, Postgres's single statement; and the split tables'
 * {@code TIMESTAMPTZ}/{@code LocalDateTime} temporal crossing) — each supported database (H2,
 * PostgreSQL, MySQL, SQL Server) has an implementation of its own, even where the SQL is identical
 * today, so as not to couple independent databases to a present-day coincidence of syntax.
 *
 * <p>Which {@link io.mohs.store.jdbc.dialect.JdbcDialect} to use is an explicit choice, never
 * auto-detection — whoever assembles {@link io.mohs.store.jdbc.JdbcWorkQueue} decides.
 */
@NullMarked
package io.mohs.store.jdbc.dialect;

import org.jspecify.annotations.NullMarked;
