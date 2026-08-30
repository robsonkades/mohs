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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.github.robsonkades.uuidv7.UUIDv7;

import io.mohs.core.execution.ExecutionId;

import static org.assertj.core.api.Assertions.assertThat;

class ShardsTest {

    /**
     * Literals pinned on purpose (the stability contract in {@link Shards}'s Javadoc): the shard is
     * WRITTEN into {@code mohs_ready}/{@code mohs_execution} — if the function changes between
     * versions, a rolling upgrade leaves old rows in shards nobody derives any more. This test breaking
     * means the change requires a data migration; it is not a refactor.
     */
    @Test
    void hashIsPinnedAcrossVersions() {
        assertThat(Shards.of(ExecutionId.of("018f7f2a-1111-7abc-8def-000000000001"))).isEqualTo(53);
        assertThat(Shards.of(ExecutionId.of("018f7f2a-1111-7abc-8def-000000000002"))).isEqualTo(60);
        assertThat(Shards.of(ExecutionId.of("0663b3aa-51a6-7f5f-b81a-524a70b2f0ee"))).isEqualTo(3);
        assertThat(Shards.of(ExecutionId.of("ffffffff-ffff-7fff-bfff-ffffffffffff"))).isEqualTo(24);
    }

    @Test
    void spreadsUuidV7IdsAcrossEveryShard() {
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            int shard = Shards.of(ExecutionId.of(UUIDv7.randomUUIDString()));
            assertThat(shard).isBetween(0, Shards.SHARD_COUNT - 1);
            seen.add(shard);
        }
        // With 10k samples across 64 buckets, an empty bucket would signal structural bias of the hash
        // over the UUIDv7 format (an almost constant timestamp prefix), not statistical fluctuation
        assertThat(seen).hasSize(Shards.SHARD_COUNT);
    }

    @Test
    void everyShardHasExactlyOneOwnerAcrossTheCluster() {
        List<String> nodes = List.of("node-c", "node-a", "node-b");
        Set<Integer> covered = new HashSet<>();
        int total = 0;
        for (String node : nodes) {
            List<Integer> owned = Shards.ownedBy(node, nodes);
            total += owned.size();
            covered.addAll(owned);
        }
        // Full coverage AND a total of 64: together they prove a partition (no gap, no overlap)
        assertThat(covered).hasSize(Shards.SHARD_COUNT);
        assertThat(total).isEqualTo(Shards.SHARD_COUNT);
    }

    /** The assignment derives from the ORDER of the ids, not from the list's order — every node must reach the same partition without negotiating. */
    @Test
    void assignmentIsIndependentOfListOrder() {
        assertThat(Shards.ownedBy("node-b", List.of("node-c", "node-a", "node-b")))
                .isEqualTo(Shards.ownedBy("node-b", List.of("node-a", "node-b", "node-c")));
    }

    @Test
    void singleNodeOwnsEveryShard() {
        assertThat(Shards.ownedBy("node-a", List.of("node-a"))).hasSize(Shards.SHARD_COUNT);
    }

    /** Safe degeneration: outside the list (a stale snapshot of mohs_nodes) or an empty list means owning ALL — an overlap is the pre-shard behaviour (SKIP LOCKED resolves it), a stalled queue is not an option. */
    @Test
    void nodeMissingFromTheListOwnsEveryShard() {
        assertThat(Shards.ownedBy("node-x", List.of("node-a", "node-b"))).hasSize(Shards.SHARD_COUNT);
        assertThat(Shards.ownedBy("node-x", List.of())).hasSize(Shards.SHARD_COUNT);
    }
}
