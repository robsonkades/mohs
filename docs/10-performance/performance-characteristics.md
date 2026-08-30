# Performance characteristics

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (measurements recorded 2026-08-14 to 2026-08-23)

> **Read the caveat first.** Every number below was measured on a **local development machine**
> (Windows, 24 logical threads) against **Docker Desktop containers**, not production hardware and
> not CI. JDK Temurin 25.0.4. Images: `postgres:16-alpine` / PostgreSQL 18 in the later rounds,
> `mysql:8.0`, `mcr.microsoft.com/mssql/server:2022-latest`, embedded H2 2.4.240.
>
> **These numbers are not portable.** They serve to compare dialects against each other within one
> round, and to detect relative regression in future rounds on the same machine. Do not use them for
> capacity planning on your hardware.

## The structural properties (these *do* transfer)

Independent of hardware:

| Property | Why it holds |
| --- | --- |
| **Claim cost does not depend on history size** | The claim statement references only `mohs_ready` and `mohs_lease`. Verified as a release gate: throughput flat between ~0 and 2 M history rows in a clean A/B on the same binary and session |
| **`GET /overview`'s throughput count costs the window, not the archive** | `idx_mohs_attempt_throughput(finished_at, outcome)` makes it a short range scan. Measured ~1.6 ms at 2 M history rows |
| **`GET /overview`'s backlog count costs the backlog** | It is `COUNT(*)` with no `WHERE` over `mohs_ready`. Measured ~13.2 ms at a 500 k backlog. **The endpoint hurts on the queue, not on history** |
| **Ownership table size is bounded** | `nodes × dispatch-concurrency` — thousands of rows, never millions. That is what makes the derived concurrency cap an always-cached index scan |
| **One payload read per claim round, never per execution** | `HistoryStore#findPayloads` takes a batch |
| **One definitions scan per tick, never per job** | The tick's snapshot serves admission, dispatch and the reaper |
| **One `mohs_nodes` read per tick** | Serves both the reaper and the shard assignment |
| **One completion transaction per flush, not per execution** | Group commit, 256 results or 5 ms |
| **Idle cost is one existence probe per tick**, not 64 shard statements | The idle gate |

## Throughput, measured

### Single node, end-to-end drain of 50,000 executions

| Configuration | Throughput |
| --- | --- |
| Defaults (`poll=5s`, `batch=50`) | **10/s** — the arithmetic ceiling of that configuration, not a system limit |
| Tuned (`poll=50ms`, `batch=1000`, `dispatch=1024`, events 256, Hikari 300) | **~4,000–4,200/s** |
| After the table split | **12,200–14,500/s** across 10 warm rounds in 3 sessions |

The first row is the single most important tuning lesson in this document: **the defaults are
latency-oriented, not throughput-oriented**, and a 5-second poll with a 50-row batch mathematically
caps at 10 executions per second.

### Cluster scale, four node-processes on one machine

| Nodes | Throughput | Scale factor |
| --- | --- | --- |
| 1 | 6.6 k/s | 1.00× |
| 2 | 9.0 k/s | **1.37×** |
| 4 | 15.0 k/s | **2.29×** |

**Sublinear, and knowingly so** — four node-processes on one machine share CPU, one PostgreSQL
container, and one disk. The gate this measurement served was "does it scale at all with N
processes", not "does it scale linearly on real hardware".

### The claim query in isolation

Sharded round-robin claim, 64 shards, one PostgreSQL container:

| Arm | Shards | Concurrent claimers | rows/s | p50 | p99 |
| --- | --- | --- | --- | --- | --- |
| Pre-shard baseline | — | 8 | 118,660 | 6.52 ms | 11.64 ms |
| Round-robin, 1 shard | 1 | 8 | 261,744 | 2.62 ms | 7.86 ms |
| Round-robin, 64 shards | 64 | 8 | **345,070** | 1.90 ms | 7.58 ms |
| Round-robin, 64 shards | 64 | 16 | **487,261** | 2.80 ms | 8.35 ms |

The claim query itself is far from the bottleneck at realistic operating points. What bounds
end-to-end throughput is dispatch and completion, not candidate selection.

## Latency, measured

### Dispatch latency, idle single node

| Percentile | Value |
| --- | --- |
| p50 | 25.3 ms |
| p95 | 59.8 ms |
| max | 65.5 ms |

At `poll-interval = 25ms`, that is roughly what the poll floor predicts.

### Dispatch latency, idle four-node cluster

| Percentile | Value |
| --- | --- |
| p50 | **461 ms** |
| p95 | 1,649 ms |
| max | 1,852 ms |

The attribution is exact and it matters:

| Node | Dispatched | p50 |
| --- | --- | --- |
| The one that received the POST | 6 of 20 | **25 ms** |
| The other three | 14 of 20 | 504 / 612 / 844 ms |

**This is the cross-node wake-up gap, stated plainly.** The local hand-off (`signalWorkScheduled`)
wakes only the JVM that performed the enqueue. Any other node discovers the work through its
adaptive poll, whose ceiling is `max-poll-interval` (2 s by default).

A LISTEN/NOTIFY tier was implemented and measured to close exactly this gap, and **withdrawn**: the
notifying transaction does not take part in group commit and serialised the ingest. The measured
cost was decisive — with `pg_notify` per enqueue, a 10,000-execution REST load went from ~15 s to
~29 s wall time, and average POST latency from ~8–19 ms to **1,281–1,514 ms**. A conflated variant
recovered the ingest but did not justify the mechanism.

**The practical mitigation is configuration**: lower `max-poll-interval` if cross-node dispatch
latency matters more than idle query cost.

## Idle cost, measured

The idle gate's effect, measured A/B on the same binary:

| Cluster | Before | After | Reduction |
| --- | --- | --- | --- |
| 1 node | 96.0 queries/s | **4.02/s** | 24× |
| 4 nodes (cluster total) | 108.8/s | **16.0/s** | 6.8× |
| 4 nodes (per node) | 27.2/s | **4.00/s** | |

And the A/B confirmed nothing else regressed:

| Metric | Pre-gate | Post-gate |
| --- | --- | --- |
| Dispatch latency, 1 idle node, p50 | 41.1 ms | 35.3 ms |
| Drain 50 k, 1 node | 12.2–12.7 k/s | 12.3–12.6 k/s |
| Drain 50 k, 4 nodes | 20.0 / 22.3 / 20.3 k/s | 19.3 / 24.3 / 20.7 / 21.3 k/s |

### A declared weakness of the probe

The probe's plan depends on the state of `mohs_ready`, and one case is bad (PostgreSQL 18):

| Non-visible backlog | Plan | Buffers | Time |
| --- | --- | --- | --- |
| 1,000 | Index Only Scan | 1 | 0.008 ms |
| 10,000 | **Seq Scan** | 121 | 0.29 ms |
| 200,000 | **Seq Scan** | 2,410 | 5.53 ms |
| 1,000,000 | **Seq Scan** | 12,049 | 27.5 ms |

And with dead tuples present, the index-only path degrades badly: 50,000 dead tuples in the node's
own shards turned a 0.031 ms / 11-buffer Index Only Scan into **10.45 ms / 38,607 buffers** with
`Heap Fetches: 50000`.

**Operational reading**: a large backlog of *not-yet-visible* entries (a big scheduled-for-later
queue) makes the idle probe expensive. The aggressive autovacuum settings on `mohs_ready` exist to
keep the dead-tuple case from arising.

## Write amplification

Per execution, measured on PostgreSQL:

| Metric | Single-table era | Node-lease era | After the split |
| --- | --- | --- | --- |
| Updates on the executions table | 6.7–7.0 | **2.00** | — |
| Tuple versions per execution (history) | 3.9–4.0 | — | **2.000** (1 INSERT + 1 advisory UPDATE) |
| Commits per execution | 3.9 | — | **0.037–0.048** (group commit) |
| WAL bytes per execution | 2,200–3,200 | 2,100–5,500 | 2,337–2,880 |

The commits-per-execution figure is the group commit working: fewer than one commit per five
executions.

## Cost model — where the time goes

At the tuned operating point, per execution:

| Cost | Round trips |
| --- | --- |
| Enqueue (history + queue + optional idempotency) | 1 transaction |
| Claim | 1 transaction, amortised over `batch-size` executions |
| Payload read | 1 query, amortised over the claim batch |
| Handler | Whatever it does |
| Completion | 1 transaction, amortised over up to 256 results |
| **Total database transactions per execution** | Roughly `1 + 1/batch + 1/256` |

Per tick, regardless of load:

| Cost | Statements |
| --- | --- |
| Heartbeat | 1 |
| Definitions scan | 1 |
| Nodes read | 1 |
| Reaper | 1 (plus 1 completion when orphans exist) |
| Stray-lease reconcile | 1 |
| Stale-node purge | 1 |
| Due-trigger sweep | 1 (plus 1 transaction per job that fires) |
| Claim | 1 probe when idle, or up to 64 statements per lap |
| Cancel poll | 1, **only when there is in-flight work** |

## Scalability characteristics

| Dimension | Behaviour |
| --- | --- |
| **Nodes** | Sublinear but real (measured 2.29× at four nodes on one machine). Bounded at **64 nodes** by `SHARD_COUNT` — above that, extra nodes own no shard and never claim, with a WARN |
| **Jobs** | The definitions scan is one query per tick, and the table is cold. Thousands of definitions are fine; the tick's snapshot is the cost |
| **Backlog** | The claim is `O(batch)` per statement via the index. A large **non-visible** backlog hurts the idle probe (see above) |
| **History** | Does not affect the claim. Affects only queries you write yourself |
| **In-flight per node** | `dispatch-concurrency`. Above it the claim stops, which is the intended backpressure |
| **Concurrent limited jobs** | The rate-limit bucket row is a serialisation point with a 2 s wait ceiling |

## Known bottlenecks

| Bottleneck | Nature | Mitigation |
| --- | --- | --- |
| Cross-node dispatch latency when idle | Structural — the poll is the only cross-node backstop | Lower `max-poll-interval` |
| The rate-limit bucket row | One row per limit, serialised at charge time | Shorter transaction tail (already done — measured 2.3× at 4 clients, 3.5× at 8); split the limit if it becomes the wall |
| `GET /overview` backlog count | `COUNT(*)` over the whole queue | Nothing today; the endpoint's cost tracks the backlog |
| The 64-shard ceiling | A hard cap on claiming nodes | Not configurable |
| SQL Server without RCSI | The overview counts take shared locks on the hot tables | Enable `READ_COMMITTED_SNAPSHOT` |
| Priority starvation | `BACKGROUND` can starve under sustained higher-priority load | None — a documented risk |
| MySQL claim throughput | Historically well below PostgreSQL and SQL Server in the single-table era (a `UNION ALL` rewrite was measured and **rejected**: p99 improved but throughput fell 32%) | The split-table design removed the `IN`-predicate problem that caused it; not re-measured per dialect since |

## A note on measurement discipline

Several optimisations in the history of this codebase were **implemented, measured and reverted**:

- The LISTEN/NOTIFY wake-up tier (serialised the ingest).
- An `INCLUDE` clause on the claim index (2.7× index size, +43% WAL, zero read benefit).
- A `UNION ALL` claim rewrite for MySQL (−32% throughput).
- Weekly time partitioning on PostgreSQL (a production class with its own failure mode, buying a
  benefit — retention by partition drop — that did not yet exist).

The rule the codebase applies to itself: **without a number, it is not an optimisation**. Apply the
same rule to anything you change here.
