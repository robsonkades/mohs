# Data lifecycle and retention

Status: Active · Last Reviewed: 2026-09-01 · Source of Truth: Repository

**Read this before running Mohs in production.** History retention exists but is **opt-in**
(`mohs.engine.history-retention`, DR-002): with the property unset, three tables grow without bound
and pruning them is your responsibility.

## Growth profile per table

| Table | Grows with | Bounded? | Automatic cleanup |
| --- | --- | --- | --- |
| `mohs_job_definitions` | Number of jobs | **Yes** — by declarations | Soft retire; rows never deleted |
| `mohs_rate_limits` | Number of limits | **Yes** | None needed |
| `mohs_batches` | Number of batches created | **Only with `history-retention` set** | The hourly sweep, once no member remains |
| `mohs_nodes` | Node incarnations (one per boot) | Effectively yes | **Yes** — the stale purge, on every tick |
| `mohs_ready` | The current backlog | **Yes** — self-limiting: a claim deletes the row | Structural |
| `mohs_lease` | Work executing across the cluster (`nodes × dispatch-concurrency`) | **Yes** — thousands, never millions | Structural: a completion deletes the row |
| `mohs_execution` | Every execution | **Only with `history-retention` set** | The hourly sweep, terminal rows outside the window |
| `mohs_attempt` | Every attempt | **Only with `history-retention` set** | The hourly sweep, once the execution is gone |
| `mohs_idempotency` | Every idempotent enqueue | **Yes** — by `idempotency-retention` (default 7d) | The hourly idempotency prune |

## What is purged automatically

The stale-node purge, in `Engine#purgeStaleNodeRows`, riding along on the tick:

```java
Instant cutoff = clock.instant().minus(nodeLeaseTtl.multipliedBy(STALE_NODE_RETENTION_LEASES));
int purged = nodeStore.deleteHeartbeatsBefore(cutoff);
```

| Property | Value |
| --- | --- |
| Retention | `node-lease-ttl × 10` — 2.5 minutes at the 15 s default |
| Frequency | Every tick, on every node |
| Isolation | Runs inside `runMaintenance`, so a failure does not stop the claim |
| Logging | INFO when rows were actually removed |

**It is not death detection.** Death is derived at read time from the promise's age. The purge
merely collects rows no reader can use, because each boot generates a new `node_id`.

Two details worth knowing:

- It derives from `node-lease-ttl`, **not** `lease-ttl`. Using the execution TTL let an operator who
  lowered `lease-ttl` delete the row of a node whose promise was still alive — and an absent row *is*
  death at read time, so a peer's reaper would reclaim work in flight on a live node.
- Every node issues the same `DELETE` on every tick, which is a classic deadlock candidate on SQL
  Server. That is precisely why the step is isolated.

## Built-in history retention (`mohs.engine.history-retention`)

Set a positive window and the engine sweeps history hourly, on the tick, on every node
([DR-002](../15-design-decisions/records/DR-002-history-retention.md)):

| Property | Value |
| --- | --- |
| What goes | TERMINAL executions `finished_at` older than the window; attempts whose execution is gone; batches with no remaining member |
| What never goes | Anything live (`PENDING`, queued, leased), an open batch (it always has a live member), and `mohs_idempotency` (its own window — below) |
| Bounds | 1,000 rows per statement per pass; passes repeat only within a 2 s monotonic budget per hourly slot; 1 s query timeout per statement |
| How it seeks | No index was added: ids are UUIDv7, so the sweep ranges the PRIMARY KEY below a bound synthesized from the cutoff instant |
| Backlog | A months-deep backlog (a window enforced for the first time) drains across hourly slots, bounded every step |
| Observability | An INFO line per sweeping slot; failures count into `mohs.tick.failed{step="history-prune"}` |
| Consequence to accept | A `FAILED` row outside the window can no longer be manually retried — it is gone. That is what retention means |

With the property **unset** (the default), nothing prunes history. Consequences to plan for:

| Consequence | Detail |
| --- | --- |
| Unbounded storage growth | Proportional to total executions ever run, plus every attempt |
| Payload retention | Payloads are stored **forever**, which may itself be a data-protection concern |
| Read costs that *do* scale with history | The execution listing's index keeps it `O(limit)` per job, but a full-table scan of history is available to any query you write yourself |
| Read costs that **do not** | The claim (references only `mohs_ready`/`mohs_lease`) and the throughput window (a range scan whose cost is the window's activity) |

### The idempotency window is its own contract

`mohs_idempotency` answers to `mohs.engine.idempotency-retention` (default 7 days), pruned hourly by
the engine — never to history retention. The two windows are deliberately independent, in both
directions: a key must keep deduplicating for its whole window even after its execution's history is
swept (deleting the key row with the execution would re-open the dedup window early), which also
means the execution id a deduplicated enqueue returns may point at history that is already gone.

## Operating with the sweep off

With `history-retention` unset, retention is the **operator's** responsibility. What the schema
supports today:

### Deleting old history

```sql
-- Order matters: attempts reference executions logically (no FK is declared, but the
-- read model joins them).
DELETE FROM mohs_attempt
 WHERE finished_at < :cutoff;

DELETE FROM mohs_execution
 WHERE state IN ('SUCCEEDED','FAILED','CANCELLED')
   AND finished_at < :cutoff;
```

| Guidance | Reason |
| --- | --- |
| **Delete in batches**, never one unbounded statement | An unbounded delete is a long transaction that holds back PostgreSQL's xmin horizon and bloats the hot tables the autovacuum settings were tuned to protect |
| Only delete rows in a **terminal** state with a `finished_at` in the past | A `PENDING` row with no queue entry and no lease is a completion flush in progress, and deleting it would orphan a live execution |
| Prefer off-peak | The tables are shared with the host application's database |
| `mohs_attempt` first, then `mohs_execution` | The detail view joins them; the reverse order leaves attempts pointing at nothing |
| `mohs_idempotency` separately, by its **own** window | It is pruned by the idempotency window, not by history retention — and its index exists for exactly this |

### Deleting old batches

```sql
DELETE FROM mohs_batches
 WHERE created_at < :cutoff
   AND succeeded + failed = total;    -- completed only
```

Only after the corresponding `mohs_execution` rows are gone, or the batch counters become
unresolvable from history.

## What must never be deleted while it is live

| Row | Why |
| --- | --- |
| A `mohs_ready` row | It **is** the backlog. Deleting it loses the execution silently |
| A `mohs_lease` row | It is live ownership. Deleting it makes the owner's completion lose the fence, and the result is discarded |
| A `mohs_nodes` row of a live node | An absent row **is** death at read time, so a peer's reaper reclaims work still running there |
| A `mohs_job_definitions` row | Retire it instead (`Mohs.remove`). Deleting it leaves history pointing at nothing and the queue draining nowhere |
| A `mohs_execution` row that is `PENDING` | Either it is queued/owned, or it is a flush in progress |

## Payload lifecycle

| Aspect | Detail |
| --- | --- |
| Serialisation | Jackson, with the payload's **concrete class name** stored in `payload_type` |
| Deserialisation | Reflective, at claim time |
| Mapper | A **raw `JsonMapper`**, deliberately not the host's context `ObjectMapper` — the persisted format belongs to Mohs, and using the application's HTTP configuration would let it define a durable format shared between nodes, breaking already-written payloads the day it changed |
| Failure | A payload that will not deserialise (a corrupt row, a class gone from the classpath) is **terminal by nature**, with `attemptsExhausted = false`. It does not heal by re-reading |
| Schema evolution | **Whatever Jackson tolerates.** Adding a nullable field is safe; renaming or retyping one breaks executions persisted before the deploy. There is no payload versioning mechanism |
| Empty payloads | Scheduler occurrences persist a concrete `LinkedHashMap`, never `Map.of()` — `payload_type` stores the exact class, and reading it back needs a type Jackson can instantiate again |

## Recommended retention design

Set `mohs.engine.history-retention` and let the built-in sweep do this. The manual route above
remains for the one shape the property does not cover: **separate windows for successes (short) and
failures (long)** — failures are the forensic record, and the built-in window is deliberately one
knob, not a policy language. A deployment that needs the split runs its own recurring job with the
batched, terminal-only `DELETE`s shown above, and leaves the property unset.

## Backup and restore considerations

| Concern | Guidance |
| --- | --- |
| Mohs' tables live in the **host's** database | They are covered by the host's backup policy automatically |
| Restoring to a point in the past | `mohs_nodes` rows will be stale; they expire and are purged within `node-lease-ttl × 10`. `mohs_lease` rows will be orphaned and reclaimed by the reaper — through the retry budget, so **work will re-execute** |
| Restoring while nodes are running | Do not. A restored `mohs_ready`/`mohs_lease` pair against live nodes will produce duplicate execution |
| Before running `V5` on PostgreSQL | **Take a backup.** It is the one migration that copies rows and has no undo |
