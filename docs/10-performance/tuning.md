# Tuning

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

## Start by knowing what you are optimising for

The defaults are **latency-oriented**: a 25 ms poll floor gives fast dispatch on an idle system, and
the adaptive backoff keeps the idle cost low. They are **not** throughput-oriented, and the single
most instructive measurement in the project makes that concrete:

| Configuration | Drain throughput |
| --- | --- |
| `poll=5s`, `batch=50` | **10/s** — the arithmetic ceiling of that configuration |
| `poll=50ms`, `batch=1000`, `dispatch=1024`, events 256, Hikari 300 | ~4,000/s |

If you have a backlog and you have not tuned, you are almost certainly measuring your configuration
rather than the system.

## The knobs, ordered by impact

### 1. `dispatch-concurrency` — the master knob

It is **three things at once**:

- The node's ceiling on in-flight executions.
- The claim's per-lap budget: `min(batch-size, dispatch-concurrency − inFlight)`.
- The size of the built-in `io` runner.

| Workload | Guidance |
| --- | --- |
| I/O-bound jobs (the common case) | Go high — 512, 1024. Virtual threads make this cheap; the real constraint is the connection pool |
| CPU-bound jobs | Do not raise this to raise CPU parallelism. Declare a `cpu` runner and size it from the core count |
| Mixed | Size for the I/O work and route CPU work to a `cpu` runner |

**Raising it demands raising the connection pool too**, or executions will queue on connection
acquisition, which is the worst place to queue.

### 2. `batch-size` and `claim-rounds` — under backlog

| Knob | Effect |
| --- | --- |
| `batch-size` | Rows per claim statement. Higher amortises the claim's fixed cost |
| `claim-rounds` | How many claims one tick may chain while batches keep coming back full |

`claim-rounds` exists to loosen the coupling between throughput and `poll-interval`. Measured:

| Poll | Rounds | Throughput |
| --- | --- | --- |
| 50 ms | 1 | 4,023 / 4,222 |
| 250 ms | 1 | 2,277 |
| **250 ms** | **8** | **3,605 / 3,739** — +58–64% over the control |
| 1 s | 8 | 2,134 |

So a longer poll with more rounds recovers most of the throughput of a short poll — useful when you
want fewer, larger interactions with the database.

**The rounds are bounded**, and knowing why matters: the tick emits its heartbeat **once, before** the
rounds, so `claimRounds × claim-latency` approaching `node-lease-ttl` would let the node's lease
expire mid-round and have a peer's reaper duplicate everything in flight. The budget is
`node-lease-ttl / 4`, checked at each lap boundary **and at each shard probe**. A longer tick also
delays the timeout and cancel signals — one more reason for rounds to be few.

### 3. `poll-interval` and `max-poll-interval` — the latency/cost trade

| Goal | Setting |
| --- | --- |
| Lowest dispatch latency | Low `poll-interval` (25 ms default) |
| Lowest idle cost | High `max-poll-interval` |
| **Lowest cross-node dispatch latency** | **Low `max-poll-interval`** — this is the knob for it |

The last row is the important one. The local hand-off wakes only the JVM that enqueued; any other
node discovers work through its poll. Measured on an idle four-node cluster: the node that received
the POST dispatched at p50 25 ms, the other three at 504 / 612 / 844 ms — bounded by
`max-poll-interval`.

Remember the two caps on the actual sleep: the nearest armed trigger shortens it (floored at
`poll-interval`), and `node-lease-ttl / 3` overrides both.

### 4. The connection pool

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 250      # high — virtual threads do not hold a thread while blocked
      connection-timeout: 3000    # low — fail fast rather than queue
```

The tuned reference point used Hikari 300 with `dispatch-concurrency` 1024. The rule of thumb: the
pool must cover the in-flight executions that actually touch the database, plus the engine's own
tick traffic (roughly 6–8 statements per tick), plus the completion flusher, plus whatever the host
application uses.

**Do not set a global `transaction-isolation`** without reading
[migrations](../06-data/migrations.md#v5--the-one-migration-that-moves-rows) — the PostgreSQL `V5`
migration refuses to run outside `READ COMMITTED` rather than silently losing rows.

### 5. `event-concurrency`

Only matters if you have listeners doing real work. The publisher **drops** events when saturated
(by contract), so a low value with heavy listeners means silent loss — visible as a WARN, not as a
metric.

### 6. `node-lease-ttl` — recovery versus tick floor

| Lower it | Raise it |
| --- | --- |
| Faster recovery after a crash (the floor is one TTL) | Fewer heartbeat writes |
| Forces a faster minimum tick cadence (`TTL/3`) | Allows a slower idle cadence |
| Shrinks the claim lap budget (`TTL/4`) | |

**Below about 10 s, revisit the rate-limit bucket's internal 2 s statement timeout** — the wait must
fit comfortably inside the TTL, or the ceiling that protects the heartbeat becomes what consumes it.

### 7. Group commit

On by default at 256 results / 5 ms. The only knob is the opt-out:

```yaml
mohs:
  engine:
    completion-flush-on-every-result: true
```

Turn it off only if you cannot accept the durability window (up to 5 ms between "the handler
finished" and "the result is durable"). The cost of turning it off is a synchronous commit per
execution, which measurements put at the top of the wait profile (`LWLock:WALWrite`).

## Per-workload starting points

### High throughput, I/O-bound

```yaml
mohs:
  engine:
    poll-interval: 50ms
    max-poll-interval: 1s
    batch-size: 500
    claim-rounds: 4
    dispatch-concurrency: 512
    event-concurrency: 64
spring:
  datasource:
    hikari:
      maximum-pool-size: 250
      connection-timeout: 3000
```

### Low latency, low volume

```yaml
mohs:
  engine:
    poll-interval: 25ms
    max-poll-interval: 250ms     # the knob for CROSS-NODE latency
    batch-size: 50
    dispatch-concurrency: 64
```

Accept a higher idle query rate in exchange.

### CPU-bound work

```yaml
mohs:
  engine:
    dispatch-concurrency: 64     # the io runner and the claim bound
  runners:
    heavy:
      mode: cpu
      core-size: 8
      max-size: 8
      queue-capacity: 32
```

```java
@OnDemandJob(id = "render-report", runner = "heavy")
void render(ReportRequest r) { … }
```

Note the CPU runner defaults: `max-size = core-size` (a fixed pool — more threads than cores does not
help CPU-bound work) and `queue-capacity = 0` (direct hand-off: it grows to `max-size`, then rejects
immediately, with no hidden queue).

### Minimal footprint

```yaml
mohs:
  engine:
    poll-interval: 1s
    max-poll-interval: 30s
    batch-size: 20
    dispatch-concurrency: 16
```

Idle cost approaches one existence probe per 30 s per node, thanks to the idle gate.

## Database-side tuning

### PostgreSQL

`mohs_ready` and `mohs_lease` already carry `fillfactor = 70` and aggressive autovacuum settings in
the schema, because they are pure insert/delete churn. Beyond that:

| Setting | Note |
| --- | --- |
| `synchronous_commit` | The single biggest lever on commit-bound throughput. Turning it off trades durability for speed — **a decision, not a tuning tweak** |
| `max_connections` | Must cover every node's pool |
| Autovacuum on the hot tables | Already tuned in the schema; monitor for a long-running transaction holding back the xmin horizon |

### SQL Server

| Setting | Note |
| --- | --- |
| `READ_COMMITTED_SNAPSHOT ON` | **Required — the boot refuses without it** (DR-001). It is what keeps every read non-blocking and correct on the hot tables; monitor the `tempdb` version store it introduces |
| `idle_in_transaction`-equivalent timeouts | Relevant to the frozen-node scenario documented in [resilience](../04-engineering/resilience.md) |

### MySQL

The engine sets `READ COMMITTED` explicitly on the claim, the completion and the trigger firing, so
the server default (`REPEATABLE READ`) does not matter for those. It **does** matter for anything a
host application does around a Mohs call.

## Diagnosing where the ceiling is

| `mohs.claim.batch.size` | `inflight / capacity` | Diagnosis | Action |
| --- | --- | --- | --- |
| Full | Low | **Claim-bound** | Raise `claim-rounds` or `batch-size`; lower `poll-interval` |
| Small | High | **Dispatch-bound** | Raise `dispatch-concurrency` and the pool; or the work is genuinely slow |
| Small | Low | Idle | Nothing to tune |
| Full | High | Both saturated | Add nodes |

Then look at `mohs.claim.latency` p99: if it is rising while the batch size stays full, the database
is the wall, not the configuration.

## Tuning that is not available

| Wish | Status |
| --- | --- |
| Retry backoff base/cap/jitter | Internal constants (1 s / 10 min) |
| Group-commit size and interval | Internal (256 / 5 ms); only the opt-out is a property |
| Shard count | **Fixed at 64.** Changing it would require a data migration |
| SSE stream interval | Fixed at 2 s |
| Reclaim, firing and misfire caps | Internal (500 / 500 / 1,440) |
| Rate-limit lock timeout | Internal (2 s) |
| Per-job priority weights | Fixed by the enum |

## The measurement discipline to copy

Before changing anything:

1. Record a baseline on **your** hardware, with the harnesses in `mohs-benchmark`.
2. Change **one** variable.
3. Re-measure with the **same** binary and the **same** session — session drift between rounds is
   real, and the benchmark scripts use a palindromic ordering (`1,2,4,4,2,1`) precisely to neutralise
   it.
4. Discard the first round of each cell as warm-up.
5. Attribute the change to a mechanism, not to a coincidence.

The codebase holds itself to this, and several plausible optimisations were reverted because the
number did not support them. See
[performance characteristics](performance-characteristics.md#a-note-on-measurement-discipline).
