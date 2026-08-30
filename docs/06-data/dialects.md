# Dialects

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (`io.mohs.store.jdbc.dialect`)

## Support tiers

| Dialect | Tier | Production | Boot behaviour |
| --- | --- | --- | --- |
| PostgreSQL | 1 | Yes | Silent |
| MySQL 8.0+ | 2 | Yes | Silent |
| SQL Server | 2 | Yes | Silent |
| H2 | **3 — test/dev only** | **No** | **WARN**: `mohs.jdbc.dialect=h2: H2 is Tier 3 — a test/dev backend, NOT supported in production` |

The H2 warning is a warning rather than an error because the demo and the development loop depend on
it deliberately. The reason it is not production-grade: its `SKIP LOCKED` has a real, measured race
(around 33% double-lock). Claim correctness still holds — it comes from the guarded CAS, not from
the lock — but nobody should discover that in production.

## The selection is explicit, never detected

```yaml
mohs:
  jdbc:
    dialect: postgresql    # h2 | postgresql | mysql | sqlserver — MANDATORY
```

An unset dialect **fails the boot**, naming the four valid values.

The reasoning, from `JdbcDialect`'s Javadoc: detecting through `Connection.getMetaData()` is fragile
across driver forks and versions. This is the same pattern Quartz uses with
`org.quartz.jobStore.driverDelegateClass`.

## What actually differs

The interface is deliberately small — only the genuine divergences:

| Method | Default | Overridden by |
| --- | --- | --- |
| `migrationLocation()` | — (abstract in practice) | All four |
| `topClause()` | `""` | SQL Server → `"TOP (:limit) "` |
| `limitClause()` | `"LIMIT :limit"` | SQL Server → `""` |
| `lockFreeReadHint()` | `""` | SQL Server → `"WITH (NOLOCK) "` |
| `splitTimestamp(Instant)` | UTC `LocalDateTime` | PostgreSQL → UTC `OffsetDateTime` |
| `readSplitTimestamp(rs, col)` | `LocalDateTime` | PostgreSQL → `OffsetDateTime` |
| `selectReadyCandidates(...)` | ANSI `FOR UPDATE SKIP LOCKED` | SQL Server → `TOP` + table hint |
| `claimReady(...)` | Three statements: select, delete, batched insert | PostgreSQL → one CTE statement |

**Each implementation owns the claim's entire SQL template**, not concatenable fragments. SQL
Server's `TOP` changes **position** in the query — right after `SELECT`, not at the end like `LIMIT`
— so a composition of generic fragments does not close cleanly. This is how Hibernate actually
implements `LimitHandler` underneath (it receives the SQL and returns rewritten SQL), and the shape
Quartz's delegates use.

`JdbcDialect` is modelled on Hibernate's `LimitHandler`/`LockingStrategy` shape — small interfaces,
one concern each — **without taking Hibernate as a dependency**: those two interfaces live inside
`Dialect`, which only exists after initialising a `SessionFactory`/`ServiceRegistry`, so using them
in isolation would mean adopting much of the framework anyway.

Each supported database gets its own implementation class **even where the SQL is identical today**,
so as not to couple independent databases to a present-day coincidence of syntax.

## PostgreSQL

| Aspect | Detail |
| --- | --- |
| Row skipping | Native `FOR UPDATE SKIP LOCKED` — this is where the syntax comes from |
| Claim shape | **One statement**: a `picked` CTE with `SKIP LOCKED`, a `DELETE … USING` that consumes the queue, an `INSERT` CTE that writes ownership, and a final `SELECT … ORDER BY` |
| Why the final `SELECT` | An `INSERT`'s `RETURNING` order is not guaranteed, and the port's contract promises `(priority, visible_at)` order in all four dialects |
| Split-table timestamps | `TIMESTAMPTZ`, crossed as UTC `OffsetDateTime`. A `LocalDateTime` would be interpreted in the **session's** zone |
| Partial indexes | Used (`WHERE correlation_id IS NOT NULL`) |
| Storage tuning | `fillfactor = 70` and aggressive autovacuum on `mohs_ready` and `mohs_lease` |
| PK ordering | `mohs_execution(created_at, execution_id)` and `mohs_attempt(finished_at, execution_id, number)` — a preserved partitioning artefact, compensated by two extra indexes |
| Streaming caveat | A `Stream` over a cursor only streams **inside a transaction** (autocommit off). Outside one, the driver materialises the whole result before the first item, regardless of `fetchSize` |
| Unique migration | `V5` — the partitioning removal. See [migrations](migrations.md#v5--the-one-migration-that-moves-rows) |

## MySQL 8.0+

| Aspect | Detail |
| --- | --- |
| Row skipping | Native `SKIP LOCKED` since 8.0 |
| Claim shape | The portable three-statement default |
| **Isolation** | MySQL defaults to `REPEATABLE READ`. Mohs sets `READ COMMITTED` **explicitly** with `REQUIRES_NEW` on the claim, the completion and the trigger firing — this divergence is precisely what the explicit isolation kills |
| Long text | `MEDIUMTEXT` |
| Timestamps | `DATETIME(6)`, microsecond precision. **Never `TIMESTAMP`** — its range ends in 2038, unacceptable for a scheduler's `next_fire_at` |
| Index creation | No `CREATE INDEX IF NOT EXISTS`; indexes are declared inline in `CREATE TABLE`, and the legacy tables used `PREPARE` guards |
| Collation | The default is case-insensitive. This is why the reserved-actor check normalises case and whitespace at the entry boundary — the predicate is also evaluated in the database |

## SQL Server

The dialect with the most divergences, all documented:

| Aspect | Detail |
| --- | --- |
| Row skipping | **No `SKIP LOCKED`.** Uses the table hint `WITH (UPDLOCK, ROWLOCK, READPAST)` — the emulation jOOQ generates for the same purpose |
| Row limit | `TOP (:limit)` immediately after `SELECT` |
| Text | `NVARCHAR` everywhere — plain `VARCHAR` is not Unicode by default. Guarded by `SqlServerUnicodeScanTest` |
| Long text | `NVARCHAR(MAX)` — there is no `CLOB` |
| Boolean | `BIT`, compared against `1`, never `TRUE` |
| Timestamps | `DATETIME2`. **`TIMESTAMP` in T-SQL is a synonym for `ROWVERSION`**, a binary auto-incremented counter — not a date at all |
| Conditional DDL | No `IF NOT EXISTS`; uses `IF OBJECT_ID(...) IS NULL` / `IF NOT EXISTS (SELECT … FROM sys.indexes)` guarding a **single statement without `BEGIN/END`**, because `ResourceDatabasePopulator` splits scripts on `;` and would break a block in half |
| `mohs_idempotency` | PK `NONCLUSTERED`, clustered on `created_at` — a hard 900-byte limit on clustered keys, not a preference |
| Parameter ceiling | ~2,100 per statement. `JdbcSupport.chunksOf` and the claim's `limit` bound (dispatch headroom, ≈1,000) keep every statement below it |
| Lock-free read hint | `WITH (NOLOCK)` on the idle-gate probe only |
| **Known open item** | `GET /overview`'s counts were rewritten over the split tables and **no longer use the hint**, so without RCSI they take shared locks on all three hot tables. Recorded in [technical debt](../technical-debt.md) |
| `DatabaseClock` gap | `CURRENT_TIMESTAMP` is a zoneless `DATETIME` interpreted in the JVM's zone. Recorded, with the fix sketched, in `DatabaseClock#sync` |

### The `NOLOCK` decision, with its accepted errors

`NOLOCK` (read uncommitted), **not** `READPAST`: skipping a locked row systematically undercounts
under load.

The accepted error is stated as the mechanism's worst case rather than "±1 in transition": with no
required order (`COUNT`/`GROUP BY`) the optimiser may choose an allocation-order scan, which under a
concurrent page split **counts a row twice or loses it** — an error proportional to write churn —
and the scan may fail with **error 601** ("data movement"), which here becomes a transient read
failure.

All three are within the probe's declared tolerance: a missed row costs one poll, a dirty row costs
one lap, and error 601 falls into the fail-open fallback that returns the tick to the full lap.

A deployment with `READ_COMMITTED_SNAPSHOT ON` makes the hint redundant — **the operator's decision,
not the library's**.

The hint must **never** be used on a read that hydrates an entity, and today its only caller is the
idle-gate probe — which is why the method is named `lockFreeReadHint` rather than `lockFreeCount`.

## H2

| Aspect | Detail |
| --- | --- |
| Claim shape | The portable default |
| Row skipping | Native `SKIP LOCKED`, with a measured ~33% double-lock race |
| Types | `VARCHAR`, `TEXT`, `BOOLEAN`, `TIMESTAMP` — the closest to PostgreSQL |
| Use | Tests and the development loop only |
| Interrupt caveat | Embedded H2 has no socket and may not honour a thread interrupt — noted in `OverviewStreamBroadcaster#closeScope`, which is why a leaked reader becomes a WARN rather than an unbounded wait |

## The claim contract, identical in all four

Whatever the SQL shape, four properties hold everywhere:

1. The returned list is ordered by `(priority, visible_at)`.
2. Queue removal and ownership insert are **atomic**.
3. Only rows of the requested shard are considered.
4. Rows locked by a concurrent claim are **skipped**, never waited on.

## Adding a dialect

See [extensibility](../04-engineering/extensibility.md#how-to-add-a-new-database-dialect) for the
five-step recipe and the tests required.
