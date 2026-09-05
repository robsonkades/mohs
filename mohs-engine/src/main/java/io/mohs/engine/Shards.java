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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import io.mohs.core.execution.ExecutionId;

/**
 * An execution's shard, and the assignment of shards to nodes.
 *
 * <p>{@code shard = hash(execution_id) % 64} — a FUNCTION of the id, never transported state:
 * enqueue, retry, requeue and the reaper all re-derive the same value from wherever they are, with
 * no column in {@code mohs_lease} and no migration.
 *
 * <p>{@code SHARD_COUNT = 64} is fixed, not configurable: 64 divides cleanly into any plausible node
 * count, and 64 shards on a single node cost nothing. The hash is FNV-1a over the id's UTF-8 bytes —
 * NEVER {@code String.hashCode}: a shard written by one JVM must be re-derivable by any other,
 * forever; that stability is contract, pinned by literal values in {@code ShardsTest}. Changing the
 * function would reshuffle the persisted backlog (entries would sit in shards nobody re-derives the
 * same way) — the kind of change that requires a migration, not a refactor.
 *
 * <p>The assignment is DERIVED, not negotiated: each node orders the live ids, finds its own index
 * {@code i} out of {@code n}, and owns {@code { s : s % n == i }}. No leader, no lock; two nodes may
 * disagree for one heartbeat during a membership change — the overlap degrades to exactly the
 * pre-shard behaviour ({@code SKIP LOCKED} resolves it) and heals itself within one heartbeat.
 */
public final class Shards {

    /** Fixed by decision — not a knob. */
    public static final int SHARD_COUNT = 64;

    private static final int FNV_OFFSET_BASIS = 0x811c9dc5;
    private static final int FNV_PRIME = 0x01000193;

    private Shards() {
    }

    /**
     * This execution's shard — deterministic across JVMs and versions (a contract pinned in {@code ShardsTest}).
     *
     * @param executionId the identity of the execution
     * @return the stable shard index of this execution
     */
    public static int of(ExecutionId executionId) {
        Objects.requireNonNull(executionId, "executionId");
        int hash = FNV_OFFSET_BASIS;
        for (byte b : executionId.value().getBytes(StandardCharsets.UTF_8)) {
            hash ^= (b & 0xff);
            hash *= FNV_PRIME;
        }
        return Math.floorMod(hash, SHARD_COUNT);
    }

    /**
     * The shards {@code nodeId} owns given the set of eligible nodes — the list is ordered HERE (on a
     * copy), so the caller may pass the ids in any order and two nodes with the same set derive the
     * same partition.
     *
     * <p>A node outside the set (one that has just joined and does not yet see itself in the read)
     * owns ALL shards — the safe degenerate case: a temporary overlap is the pre-shard behaviour,
     * whereas "owning nothing" would leave the queue stalled.
     *
     * @param nodeId the identity of the engine node
     * @param eligibleNodeIds the nodes eligible to own shards
     * @return the shard indices assigned to this node
     */
    public static List<Integer> ownedBy(String nodeId, List<String> eligibleNodeIds) {
        Objects.requireNonNull(nodeId, "nodeId");
        List<String> sorted = Objects.requireNonNull(eligibleNodeIds, "eligibleNodeIds").stream().sorted().toList();
        int index = sorted.indexOf(nodeId);
        if (index < 0) {
            return everyShard();
        }
        List<Integer> owned = new ArrayList<>();
        for (int shard = index; shard < SHARD_COUNT; shard += sorted.size()) {
            owned.add(shard);
        }
        return owned;
    }

    private static List<Integer> everyShard() {
        return IntStream.range(0, SHARD_COUNT).boxed().toList();
    }
}
