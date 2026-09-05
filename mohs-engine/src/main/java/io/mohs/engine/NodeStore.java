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
package io.mohs.engine;

import java.time.Instant;
import java.util.List;

import io.mohs.core.EngineState;
import io.mohs.core.execution.Execution;

/**
 * The node's heartbeat and lease registry — a Repository (PoEAA), the port
 * {@code io.mohs.store.jdbc} implements.
 *
 * <p>It stopped being merely informative: the reaper decides "dead" from this table's
 * {@code expires_at} (one write per node per tick), in place of the PER-EXECUTION lease renewal that
 * cost roughly five updates per execution on the system's hottest table.
 */
public interface NodeStore {

    /**
     * Records the node's most recent heartbeat — an upsert by {@code nodeId}.
     *
     * <p>The heartbeat now carries the NODE's lease: {@code epoch} (its incarnation) and
     * {@code expiresAt} (the "I am alive until here" promise the reaper consults — the liveness
     * authority that replaced per-execution renewal).
     *
     * @param nodeId the identity of the engine node
     * @param state the current lifecycle state
     * @param epoch the ownership generation used by the completion fence
     * @param at the instant of the operation
     * @param expiresAt the instant when the liveness lease expires
     */
    void heartbeat(String nodeId, EngineState state, long epoch, Instant at, Instant expiresAt);

    /**
     * Every node ever registered, with no "recent" filter — the staleness threshold depends on
     * configuration that does not exist yet ({@code mohs.engine.node-heartbeat-interval}).
     *
     * <p>A {@code List}, not a {@code Stream}: this table's size is bounded by the cluster's size and
     * does not grow without limit as {@link Execution} does.
     *
     * @return all recorded nodes, including stale nodes
     */
    List<StoredNode> findAll();

    /**
     * Removes heartbeats strictly older than {@code cutoff} — each boot generates a new
     * {@code node_id}, so a dead or restarted instance's row only leaves through here.
     *
     * <p>Death is NOT written by whoever died (a crash gives no notice — "dead" is derived from
     * staleness at read time); the purge merely collects what has already become unreadable through
     * age.
     *
     * @param cutoff the exclusive retention cutoff
     * @return how many rows were removed
     */
    int deleteHeartbeatsBefore(Instant cutoff);
}
