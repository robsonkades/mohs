# Data lifecycle and retention

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

**Read this before running Mohs in production.** Two of the nine tables grow without bound and
nothing in the codebase prunes them.

## Growth profile per table

| Table | Grows with | Bounded? | Automatic cleanup |
| --- | --- | --- | --- |
| `mohs_job_definitions` | Number of jobs | **Yes** — by declarations | Soft retire; rows never deleted |
| `mohs_rate_limits` | Number of limits | **Yes** | None needed |
| `mohs_batches` | Number of batches created | **No** | **None** |
| `mohs_nodes` | Node incarnations (one per boot) | Effectively yes | **Yes** — the stale purge, on every tick |
| `mohs_ready` | The current backlog | **Yes** — self-limiting: a claim deletes the row | Structural |
| `mohs_lease` | Work executing across the cluster (`nodes × dispatch-concurrency`) | **Yes** — thousands, never millions | Structural: a completion deletes the row |
| `mohs_execution` | **Every execution, forever** | **No** | **None** |
| `mohs_attempt` | **Every attempt, forever** | **No** | **None** |
| `mohs_idempotency` | Every idempotent enqueue | **No** | A prune method exists — **nothing calls it** |

## What is actually purged

Exactly one thing, in `Engine#purgeStaleNodeRows`, riding along on the tick:

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

## The gap: history has no retention

`mohs_execution` and `mohs_attempt` are **append-only with a single terminal update**, and there is
no policy, no scheduled task, and no property to purge them.

Consequences to plan for:

| Consequence | Detail |
| --- | --- |
| Unbounded storage growth | Proportional to total executions ever run, plus every attempt |
| Payload retention | Payloads are stored **forever**, which may itself be a data-protection concern |
| Read costs that *do* scale with history | The execution listing's index keeps it `O(limit)` per job, but a full-table scan of history is available to any query you write yourself |
| Read costs that **do not** | The claim (references only `mohs_ready`/`mohs_lease`) and the throughput window (a range scan whose cost is the window's activity) |

The `V5` migration's own header calls the removed partitioning "a benefit that does not yet exist —
retention by partition drop was a later phase". That later phase is **not in this repository**.

### The idempotency window is the retention window

`ScheduleCommand#idempotencyKey`'s contract says the key deduplicates "for as long as the execution
exists — the window is that of execution retention, which is unbounded while no retention policy
exists". So **reusing an old key returns the old execution**, indefinitely.

`HistoryStore#pruneIdempotencyBefore(cutoff)` exists, is described as "called by housekeeping, never
on the hot path", and is properly indexed (`V7` measured 83.2 ms → 0.97 ms on 2 M rows). But a
repository-wide search finds **no production caller and no scheduled invocation**. See
[technical debt](../technical-debt.md).

## Operating without a retention policy

Until a policy exists, retention is the **operator's** responsibility. What the schema supports
today:

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

Not implemented; this is what the schema is shaped for.

| Element | Recommendation |
| --- | --- |
| Trigger | A recurring Mohs job of your own — the tool schedules its own housekeeping perfectly well |
| Batching | Delete in chunks of a few thousand, with a bounded loop and a time budget |
| Windows | Separate windows for successes (short) and failures (long) — failures are the forensic record |
| Idempotency | A window matched to your business retry horizon, typically far shorter than history |
| Verification | `mohs.execution.total` versus row counts, to confirm the prune keeps up with ingest |
| Ordering | Attempts, then executions, then batches, then idempotency |

A worked starting point:

```java
@RecurringJob(id = "mohs-retention", cron = "0 30 3 * * *", zone = "UTC")
void prune() {
    Instant cutoff = clock.instant().minus(Duration.ofDays(30));
    int deleted;
    do {
        deleted = jdbc.update("""
            DELETE FROM mohs_attempt WHERE finished_at < ? LIMIT 5000
            """, Timestamp.from(cutoff));           // dialect-specific LIMIT syntax
    } while (deleted == 5000);
    // ... then mohs_execution, then mohs_batches, then mohs_idempotency
}
```

Note that the exact `DELETE … LIMIT` syntax differs per dialect (`DELETE … LIMIT` on MySQL, a
`ctid`/`TOP` form on PostgreSQL/SQL Server), which is one of the reasons a portable implementation
inside the library is a real piece of work rather than a small addition.

## Backup and restore considerations

| Concern | Guidance |
| --- | --- |
| Mohs' tables live in the **host's** database | They are covered by the host's backup policy automatically |
| Restoring to a point in the past | `mohs_nodes` rows will be stale; they expire and are purged within `node-lease-ttl × 10`. `mohs_lease` rows will be orphaned and reclaimed by the reaper — through the retry budget, so **work will re-execute** |
| Restoring while nodes are running | Do not. A restored `mohs_ready`/`mohs_lease` pair against live nodes will produce duplicate execution |
| Before running `V5` on PostgreSQL | **Take a backup.** It is the one migration that copies rows and has no undo |
