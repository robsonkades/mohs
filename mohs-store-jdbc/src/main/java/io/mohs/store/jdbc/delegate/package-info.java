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
 * Every statement {@code io.mohs.store.jdbc} issues, one implementation per supported database.
 *
 * <p>{@link io.mohs.store.jdbc.delegate.JdbcDelegate} declares ONE method per statement, and each of the
 * four delegates (H2, PostgreSQL, MySQL, SQL Server) answers all of them with its own complete text —
 * no fragment is concatenated at runtime, so what a reader sees in one of these classes is literally
 * what that server receives. The shape is Quartz's ({@code StdJDBCDelegate}/{@code MSSQLDelegate}),
 * not Hibernate's fragment handlers.
 *
 * <p>The text that is identical between delegates is duplicated on purpose: SQL that matches today is
 * a coincidence of syntax rather than a shared contract, and a DBA should be able to open one file and
 * read what their own server runs. The price — a correction applied four times — is stated in the
 * decision record and guarded, as far as it can be, by {@code JdbcDelegateStatementDriftTest}.
 *
 * <p>The split tables' {@code TIMESTAMPTZ}/{@code LocalDateTime} temporal crossing lives here for the
 * same reason: it is a fact about the database, not about the store.
 *
 * <p>Which {@link io.mohs.store.jdbc.delegate.JdbcDelegate} to use is an explicit choice, never
 * auto-detection — the property selects among the four built-ins, and a {@code @Bean} supplies any
 * other database (a public SPI).
 */
@NullMarked
package io.mohs.store.jdbc.delegate;

import org.jspecify.annotations.NullMarked;
