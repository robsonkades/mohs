# DR-007: Sixty-four fixed shards, with a derived assignment

## Status

Accepted

## Context

With every node claiming from one ordered queue, `SKIP LOCKED` resolves the contention but the
candidate sweeps still overlap: every claimer scans the same index prefix, and the row locks they take
are in each other's way.

Measured with 8 concurrent claimers against one PostgreSQL container, the unsharded arm reached
118,660 rows/s at p99 11.6 ms. The question was how to let claimers stop competing for the same rows
without introducing a coordinator to assign them.

## Decision

**`shard = FNV-1a(execution_id) mod 64`**, with the assignment **derived, not negotiated**:

1. Take the live, `RUNNING` node ids.
2. Sort them (on a copy, so callers may pass any order).
3. Find your own index `i` out of `n`.
4. Own `{ s : s mod n == i }`.

Four properties are deliberate:

| Property | Value | Reason |
| --- | --- | --- |
| Shard count | **64, fixed, not configurable** | 64 divides cleanly into any plausible node count, and 64 shards on a single node cost nothing |
| Hash | **FNV-1a, never `String.hashCode()`** | A shard written by one JVM must be re-derivable by any other, **forever**. That stability is contract, pinned by literal values in `ShardsTest` |
| Storage | A `shard` column exists in `mohs_ready`, but it is a **function of the id** | Enqueue, retry, requeue and the reaper all re-derive it, with no column in `mohs_lease` and no migration |
| Claim shape | **One shard per statement, never a list** | A multi-shard predicate kills the index's ordering. Measured: 25.5 ms/round versus 0.43 ms |

## Consequences

### Positive

- **Measured throughput**: 345,070 rows/s at 8 claimers and 487,261 at 16, against 118,660 unsharded
  at 8 — with p99 falling from 11.6 ms to 7.6 ms.
- `idx_mohs_ready_claim (shard, priority, visible_at)` supplies the claim's ordering with no sort:
  `shard` is an equality predicate, and the rest *is* the `ORDER BY`.
- **No coordinator.** Membership changes need no negotiation; assignment recomputes from a read.
- **Disagreement is bounded and self-healing.** Two nodes may disagree for one heartbeat during a
  membership change; the overlap degrades to exactly the pre-shard behaviour, `SKIP LOCKED` resolves
  it, and it heals within one heartbeat.
- **The degenerate case is safe**: a node that has just joined and does not yet see itself in the read
  owns *all* shards. A temporary overlap is the pre-shard behaviour; "owning nothing" would stall the
  queue.

### Negative

- **A hard ceiling of 64 claiming nodes.** Above that, extra `RUNNING` nodes own no shard and never
  claim — logged **once per transition** (at a 25 ms floor it would be ~40 lines/s per node, burying
  everything else).
- **Changing the hash is a data migration, not a refactor.** Entries already in the queue would sit in
  shards nobody re-derives the same way.
- **A lap is up to 64 statements**, which is why the idle gate exists ([DR-008](DR-008-adaptive-poll-and-the-withdrawn-notify-tier.md))
  and why the lap's time budget is checked **per shard probe**, not only per lap: a degraded database
  at 300 ms per claim would otherwise blow the node's lease mid-tick.
- A rotation cursor must persist between ticks, or a tick that exhausts its budget on shard 0 starves
  the end of the list forever.
- Eligibility is **alive AND `RUNNING`** — a `PAUSED` or `DRAINING` node holding shards would leave
  1/n of the queue stalled while its peers had headroom.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| No sharding, rely on `SKIP LOCKED` alone | Works, and is the fallback behaviour. Measured 2.9× slower at 8 claimers |
| A configurable shard count | 64 covers every plausible cluster, and a knob would invite changing it — which is a data migration |
| `String.hashCode()` | Not specified as stable across JVM implementations. The shard must be re-derivable forever |
| Storing the shard only, without a re-derivable function | Requires carrying it through every path (retry, requeue, the reaper) and a column in `mohs_lease` |
| A multi-shard `IN` predicate in one statement | Measured 25.5 ms/round versus 0.43 ms — it destroys the index's ordering |
| Negotiated assignment (a coordinator, or a rendezvous protocol) | Adds the coordinator this design exists to avoid |

## Evidence

- `mohs-engine/src/main/java/io/mohs/engine/Shards.java` — the hash, the count, the assignment, and
  the reasoning for each.
- `mohs-engine/src/test/java/io/mohs/engine/ShardsTest.java` — literal values pinning the hash.
- `mohs-engine/src/main/java/io/mohs/engine/WorkQueue.ReadyEntry` — range validation at the point the
  data enters the type, because a row outside `[0, 64)` would rot unclaimed in silence.
- Sharding measurements recorded 2026-08-21.
