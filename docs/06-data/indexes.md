# Indexes

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

Every index in the schema, the query it serves, and the measurement behind it where one exists.

## The complete list

| Index | Table | Columns | Serves | Dialects |
| --- | --- | --- | --- | --- |
| *(PK)* | `mohs_ready` | `execution_id` | Requeue, cancel-queued, retry insert | All |
| `idx_mohs_ready_claim` | `mohs_ready` | `(shard, priority, visible_at)` | **The claim** | All |
| *(PK)* | `mohs_lease` | `execution_id` | The fenced completion delete, the cancel flag | All |
| `idx_mohs_lease_node` | `mohs_lease` | `(node_id, epoch)` | The reaper's orphan scan; the drain's own-lease read | All |
| `idx_mohs_lease_job` | `mohs_lease` | `job_key` | The **derived concurrency cap** (`countByJob`) | All |
| *(PK)* | `mohs_execution` | `execution_id` | Point lookup, terminal update, `findPage`'s cursor | All |
| `idx_mohs_execution_job` | `mohs_execution` | `(job_key, execution_id DESC)` | `findPage` filtered by job: equality first, then the `ORDER BY`/cursor | All |
| `idx_mohs_execution_corr` | `mohs_execution` | `correlation_id` (partial `WHERE NOT NULL` on PostgreSQL) | Batch member lookup | All |
| *(PK)* | `mohs_attempt` | `(execution_id, number)` | Attempt insert, and the detail view — predicate and ordering both | All |
| `idx_mohs_attempt_throughput` | `mohs_attempt` | `(finished_at, outcome)` | The `GET /overview` throughput window | All |
| *(PK)* | `mohs_idempotency` | `(job_key, idempotency_key)` | **The deduplication check itself** | All (`NONCLUSTERED` on SQL Server) |
| `idx_mohs_idempotency_created` | `mohs_idempotency` | `created_at` | The retention prune | PostgreSQL, H2, MySQL |
| `ix_mohs_idempotency_created` | `mohs_idempotency` | `created_at`, **CLUSTERED** | Same, plus the table's physical order | SQL Server only |
| *(PK)* | `mohs_job_definitions` | `id` | | All |
| *(UNIQUE)* | `mohs_job_definitions` | `job_key` | Every lookup by domain identity | All |
| `idx_mohs_job_next_fire` | `mohs_job_definitions` | `next_fire_at` | The due-trigger read the engine performs on **every tick**, ahead of the firing and the claim | All |
| *(PK)* | `mohs_rate_limits` | `name` | | All |
| *(PK)* | `mohs_nodes` | `node_id` | The heartbeat upsert | All |
| *(PK)* | `mohs_batches` | `id` | The counter increment and the read | All |

## The claim index

`idx_mohs_ready_claim (shard, priority, visible_at)` is the one index the whole design turns on.

**The column order *is* the claim's `ORDER BY`.** The claim is deliberately single-shard per
statement, so `shard` is an equality predicate and `(priority, visible_at)` supplies the ordering
with no sort step.

Two decisions recorded against it:

| Decision | Reasoning |
| --- | --- |
| **One shard per statement, never a list** | A multi-shard predicate kills the index's ordering. Measured: 25.5 ms per round versus 0.43 ms |
| **No `INCLUDE` clause**, despite an earlier plan calling for one | `FOR UPDATE` requires a `LockRows` node, which forces heap access — the candidate sweep is never index-only. Measured on PostgreSQL 18 with a 50 k backlog: identical plan and identical buffers with and without the `INCLUDE`, while the `INCLUDE` cost **2.7× the index size** (4,208 kB versus 1,552 kB) and **+43% WAL** on the same 50 k inserts (20 MB versus 14 MB). Pure write amplification, zero read benefit |

## The throughput index

`idx_mohs_attempt_throughput (finished_at, outcome)` is what makes `GET /overview` "cheap by
construction".

`finished_at` leads, so the window becomes a short range scan whose cost is **proportional to the
window's activity, never to the size of history**. `outcome` sits second so the `GROUP BY` is served
without a heap fetch. And `finished_at` grows monotonically, so inserts land at the index tail —
the same locality UUIDv7 gives the primary keys.

Measured 2026-08-23 (`OverviewLatencyScenario`, idle database): the throughput count costs the
**window**, not the archive — about 1.6 ms at 2 M history rows. What costs is the other read, the
backlog `COUNT(*)` over `mohs_ready` — about 13.2 ms at a 500 k backlog. **The endpoint hurts on the
queue, not on history.**

## The `findPage` index

`idx_mohs_execution_job (job_key, execution_id DESC)` and **not** `(job_key, created_at)`.

The only consumer of `job_key` on this table is `findPage`, which orders and paginates by
`execution_id` (UUIDv7, time-ordered). Equality first, then the `ORDER BY`, serves the listing **and**
the cursor as an index condition. Measured: 0.61 ms → 0.24 ms on a selective job, and the plan
becomes `O(limit)` instead of `O(rows of the job)`.

`created_at` in second position served no query at all: `from`/`to` filter `scheduled_at`, and
retention is a bulk delete.

## The two PostgreSQL-only indexes that no longer exist

They are gone as of the primary-key normalisation. `idx_mohs_execution_id` and
`idx_mohs_attempt_exec` existed only because PostgreSQL's keys led with a time column, which
partitioning had required; with the keys normalised to `(execution_id)` and `(execution_id, number)`
each index became the key itself, or its exact prefix, and the four dialects now index the history
identically.

The measurement that justified `idx_mohs_attempt_exec` — **19 ms / 15,700 buffers** on 1.1 M
attempts against **0.035 ms / 7 buffers** — still describes what the detail view costs with and
without an access path on `execution_id`. It is now the primary key that provides it, and the
`ORDER BY number` comes with it rather than needing a sort.

## Two indexes that were removed, and why

Recorded in `V7__idempotency_retention_index.sql` as measured corrections rather than architecture
decisions:

### Added: `idx_mohs_idempotency_created`

`pruneIdempotencyBefore` is `DELETE … WHERE created_at < ?`, and the table had no index on that
column — a full scan of a table that grows with **every idempotent enqueue**. On PostgreSQL the scan
also leaves proportional dead tuples, so the prune became progressively more expensive.

Measured on PostgreSQL 16, 2 M rows / 327 MB, pruning 3,599 rows:

| | Plan | Buffers | Time |
| --- | --- | --- | --- |
| Before | Seq Scan, `Rows Removed by Filter: 1996401` | 24,689 | 83.2 ms |
| After | Index Scan | 55 | **0.97 ms** |

The scan cost the whole table on every sweep, regardless of how much there was to prune.

### Dropped: `idx_mohs_execution_created`

It existed on H2, MySQL and SQL Server — and **no query filters or orders by `created_at` alone**.
PostgreSQL, correctly, never had it. The only predicate mentioning the column is the completion CAS
(`execution_id = ? AND created_at = ?`), which matches on the primary key and demotes `created_at`
to a filter.

The cost was **not** on the terminal write: the `UPDATE` does not touch `created_at`, and a
secondary index whose key did not change is not maintained. Measured on SQL Server 2025: 3 logical
reads on the terminal `UPDATE` with and without the index, with the plan being a lone Clustered
Index Update.

The cost was the **INSERT**, on the hottest table in the system: **6 → 3 logical reads per row**
after dropping it.

## Partial and filtered indexes

PostgreSQL uses partial indexes where the predicate is a constant of the query:

```sql
CREATE INDEX idx_mohs_execution_corr ON mohs_execution (correlation_id)
    WHERE correlation_id IS NOT NULL;
```

Historically the technique was applied much more aggressively on the legacy single table, and the
measurements are worth keeping as a rule of thumb: a partial claim index cut index size by **95.2%**
on PostgreSQL and **84.2%** on SQL Server with stable claim throughput.

The technique also carries a documented trap: **a partial index is only eligible when the query's
predicate *implies* the index's**. When retry became claimable, `IN ('ENQUEUED','RETRY_WAITING')`
stopped implying `= 'ENQUEUED'`, and without updating the pair the plan degraded to a sequential
scan plus sort of the whole table on every tick.

The split-table design removed the need for that class of index entirely: `mohs_ready` *is* the
backlog, so there is nothing to filter out.

## The `IS NOT NULL` conjunct trick

Also from the legacy schema, and worth recording because the same reasoning would apply again:

A reaper index of `(lease_expires_at) WHERE state = 'RUNNING'` was **too attractive** to the
planner. Every `UPDATE … WHERE id = ? AND state = 'RUNNING'` — the completion CAS, the lease
renewal, the cancel request — implied the index's predicate, so the planner chose it over the
primary key: a full scan of the `RUNNING` entries per statement. Measured: **8.4 ms per completion
versus 0.05 ms via the PK**, with the index bloated by claim-to-completion churn.

Adding `AND lease_expires_at IS NOT NULL` to the index predicate made the CAS **ineligible** — it
does not mention the column — so the primary key wins **by construction, not by luck of cost**. The
reaper stayed eligible because its query carries the conjunct trivially.

The same technique appears on the owner index, with a measured 41 buffers / 1.84 ms per completion
versus 6 buffers / 0.11 ms via the PK.

## Index maintenance guidance

| Situation | Guidance |
| --- | --- |
| Adding an index to an existing PostgreSQL installation | `CREATE INDEX CONCURRENTLY`. Note that `CREATE INDEX IF NOT EXISTS` **does not alter** an index that already exists under the same name — the migrations guard by *shape*, not by name, for exactly this reason |
| Changing an index predicate | Create the new one concurrently, drop the old concurrently, then rename |
| Bloat on `mohs_ready` / `mohs_lease` | PostgreSQL storage parameters already set `fillfactor = 70` and aggressive autovacuum. If bloat still appears, the diagnosis is a long-running transaction holding back the xmin horizon |
| SQL Server fragmentation | The clustered index on `mohs_idempotency` is `created_at` and therefore append-friendly; the rest of the hot tables are keyed on UUIDv7, which is also append-friendly |
| Adding an index of your own | Nothing prevents it — Mohs never inspects the schema. If you fork, keep `schema-*.sql` and the `V*.sql` chain in step: the structural guardian compares them |
