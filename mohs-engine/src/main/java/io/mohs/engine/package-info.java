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
 * The internal execution engine (claim, dispatch, retry, misfire). Not part of the public API — see
 * {@code io.mohs.core} for the public contracts.
 *
 * <p>The persistence ports (Repository/Data Mapper, PoEAA) that {@code io.mohs.store.jdbc}
 * implements — this package knows no JDBC, only the contracts: {@link io.mohs.engine.JobStore}
 * (definitions; {@link io.mohs.engine.StoredJob} combines
 * {@link io.mohs.core.definition.JobDefinition} with the operational state kept separate from the
 * definitional one), {@link io.mohs.engine.WorkQueue} (the {@code mohs_ready} queue — claim,
 * requeue, cancelling an enqueued entry, manual rearm), {@link io.mohs.engine.LeaseStore} (the
 * {@code mohs_lease} ownership with the {@code (node_id, epoch, attempt_number)} fence and the completion
 * transaction), {@link io.mohs.engine.HistoryStore} ({@code mohs_execution}/{@code mohs_attempt}
 * history and the derived read model), {@link io.mohs.engine.StoreTransactions} (the enqueue unit),
 * {@link io.mohs.engine.BatchStore} (batch counters, {@link io.mohs.engine.BatchCounters}),
 * {@link io.mohs.engine.RateLimitStore} and {@link io.mohs.engine.NodeStore} (the NODE's
 * heartbeat and lease — {@link io.mohs.engine.StoredNode} is its read form).
 *
 * <p>{@link io.mohs.engine.SyncableClock} is the same idea applied to the {@code Clock}: the port
 * lives here and {@code DatabaseClock} (io.mohs.store.jdbc) implements it — the engine never
 * imports {@code NamedParameterJdbcTemplate} or {@code DataSource}.
 *
 * <p>{@link io.mohs.engine.Dispatcher} invokes a claimed execution's handler through
 * {@link io.mohs.engine.HandlerRegistry} (the in-memory {@code JobKey} to {@code JobHandler}
 * registry, populated by {@code io.mohs.autoconfigure}) and publishes
 * {@link io.mohs.core.event.ExecutionEvent} through
 * {@link io.mohs.engine.ExecutionEventPublisher}; the completion may go through
 * {@link io.mohs.engine.CompletionBatcher}'s group commit.
 *
 * <p>{@link io.mohs.engine.Engine} is what ties it all together: the poll loop (the node's
 * heartbeat, the reaper of dead nodes' leases, reconciliation, triggers, the claim with its
 * admission rules and the dispatch, on every tick), and it implements
 * {@link io.mohs.core.MohsLifecycle} directly.
 *
 * <p>{@link io.mohs.engine.MohsExecutors} is the central executor/scheduler factory that
 * {@code Engine}, {@code Dispatcher} and {@code ExecutionEventPublisher} receive injected: no class
 * in this package creates an {@code Executor} or {@code ScheduledExecutorService} by hand.
 */
@NullMarked
package io.mohs.engine;

import org.jspecify.annotations.NullMarked;
