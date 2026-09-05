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
 * Internal JDBC persistence for jobs, the queue, ownership, history, batches and rate limits — a Data
 * Mapper (PoEAA) over {@code io.mohs.engine}'s ports.
 *
 * <p>{@link io.mohs.store.jdbc.JdbcJobStore} implements {@link io.mohs.engine.JobStore};
 * {@link io.mohs.store.jdbc.JdbcWorkQueue} implements {@link io.mohs.engine.WorkQueue} — the claim moves
 * {@code mohs_ready} to {@code mohs_lease} under {@code SKIP LOCKED} (a single statement on Postgres, a
 * portable form elsewhere); {@link io.mohs.store.jdbc.JdbcLeaseStore} implements
 * {@link io.mohs.engine.LeaseStore} — the completion transaction, fenced by {@code (node_id, epoch, attempt_number)};
 * {@link io.mohs.store.jdbc.JdbcHistoryStore} implements {@link io.mohs.engine.HistoryStore} (the
 * payload serialised through Jackson, never a field of {@code Execution}; state derived in the read
 * model); {@link io.mohs.store.jdbc.JdbcStoreTransactions} implements
 * {@link io.mohs.engine.StoreTransactions} (a savepoint through NESTED — the enqueue unit composes with
 * the host's transaction); {@link io.mohs.store.jdbc.JdbcBatchStore} implements
 * {@link io.mohs.engine.BatchStore}; and {@link io.mohs.store.jdbc.JdbcRateLimitStore} implements
 * {@link io.mohs.engine.RateLimitStore}.
 *
 * <p>{@link io.mohs.store.jdbc.DatabaseClock} also lives here — it implements {@code Clock} plus
 * {@link io.mohs.engine.SyncableClock}, the "database" time source; it is the project's only class where
 * reading the real clock is the purpose rather than a violation (a named exception in
 * {@code ArchitectureTest}).
 *
 * <p>Not part of the public API — see {@code io.mohs.core} for the public contracts.
 */
@NullMarked
package io.mohs.store.jdbc;

import org.jspecify.annotations.NullMarked;
