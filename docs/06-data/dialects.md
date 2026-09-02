# Dialects

Status: Active · Last Reviewed: 2026-09-01 · Source of Truth: Repository (`io.mohs.store.jdbc.delegate`)

## Support tiers

| Dialect | Tier | Production | Boot behaviour |
| --- | --- | --- | --- |
| PostgreSQL | 1 | Yes | Silent |
| MySQL 8.0+ | 2 | Yes | Silent |
| SQL Server | 2 | Yes | **Refuses to boot without `READ_COMMITTED_SNAPSHOT ON`** — see [the RCSI requirement](#the-rcsi-requirement) |
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

The reasoning, from `JdbcDelegate`'s Javadoc: detecting through `Connection.getMetaData()` is fragile
across driver forks and versions. This is the same pattern Quartz uses with
`org.quartz.jobStore.driverDelegateClass`.

## Every statement lives in the delegate

`JdbcDelegate` declares **66 statement methods, all abstract**, and each of the four implementations
spells out all 66. Nothing is assembled from fragments and nothing is inherited: reading
`PostgresJdbcDelegate` top to bottom answers *what does Mohs send to PostgreSQL* without
reconstructing a statement from a base class and an override.

That is a deliberate trade. **61 of the 66 come out byte-identical across the four files**, and
keeping them that way is duplication a base class would remove. What it buys:

- a divergence is *visible*, because it sits beside the 61 that agree, rather than implied by an
  override somewhere else;
- adding a database cannot half-inherit a statement that happens to be wrong for it — the compiler
  demands all 66;
- and the SQL a reader is debugging at 3 a.m. is the SQL in the file, not the SQL after inheritance.

`JdbcDelegateStatementDriftTest` is what keeps the duplication honest: it compares the named
parameters of every statement across the four delegates, so a database whose statement quietly stops
binding the same things fails the build.

### The five that genuinely differ

| Statement | Why it diverges |
| --- | --- |
| `readyCandidates`, `readyCandidatesFiltered` | `TOP (:limit)` sits right after `SELECT` on SQL Server, where the others put `LIMIT` at the end, and the row-skipping hint is a table hint rather than a clause |
| `findOrphanedLeases`, `findOrphanedLeasesExceptAlive` | Same limit-position problem |
| `findExecutionPage` | Same, on the history page |

They were seven until 2026-09-01: `visibleWorkExists` and `visibleWorkCount` carried `WITH (NOLOCK)`
on SQL Server, and became byte-identical to the other three when the RCSI requirement retired the
hint (DR-001).

PostgreSQL additionally replaces the claim *algorithm* rather than a statement: `claimReady` becomes
one CTE instead of the portable three.

### What still carries a default

Only `selectReadyCandidates` and `claimReady`, and those are the claim's sequence of steps rather
than SQL. Every statement is abstract, and so is the clock: `nowQuery()` and `readNow()` are a pair,
and both halves are abstract on purpose. The question they answer — does this server's `now` carry a
zone, and how is it crossed back into an instant — used to be a boolean with a fail-safe `false`,
which a delegate could answer by never thinking about it, at the cost of `mohs.time.mode=database`
being refused. An abstract crossing cannot be inherited by accident: an implementation that never
considered the zone does not compile, and one that did gets database time on any dialect.

`JdbcDelegate` is named after Quartz's `StdJDBCDelegate`/`MSSQLDelegate` — one type per database,
one concern each. The contrast with Quartz is the configuration: its
`org.quartz.jobStore.driverDelegateClass` takes a class name, so the property *says* delegate. Mohs
names a database instead (`mohs.jdbc.dialect: postgresql`), because that is what the operator knows;
the delegate is what the library picks as a result.

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
| **RCSI** | **A boot requirement.** `SqlServerRcsiRequirement` refuses to start unless `READ_COMMITTED_SNAPSHOT` is ON — see below |
| Read hints | **None.** The `WITH (NOLOCK)` the idle-gate probe used to carry was retired by the RCSI requirement |
| `DatabaseClock` | Samples `SYSUTCDATETIME()`, not `CURRENT_TIMESTAMP`: the latter is a zoneless `DATETIME` the driver reads back in the JVM's zone, and it is only `datetime2` that resolves finer than ~3.3 ms |

### The RCSI requirement

`READ_COMMITTED_SNAPSHOT ON` is a **stated requirement** of the SQL Server dialect, verified at
boot and refused loudly with the exact command to run
([DR-001](../15-design-decisions/records/DR-001-rcsi-required-on-sql-server.md)):

```sql
ALTER DATABASE [your_database] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE
```

Azure SQL Database ships with it ON; on-premises servers default to OFF.

Both sides of the requirement were measured. Without RCSI, one uncommitted claim holding X locks
blocked the overview counts to a lock timeout (`Msg 1222`) — and the dashboard's stream runs them
every 2 s. The lock-free alternative, `WITH (NOLOCK)`, read a queue depth of 49,000 against 50,000
committed (it saw a claim's uncommitted `DELETE` that then rolled back), admits double-counted or
lost rows under page splits, and can fail with error 601 — a wrong number a dashboard cannot
distinguish from a right one. Row versioning is the only answer that is both non-blocking and
correct, so the dialect requires it and carries **no read hints at all**.

There is no opt-out property: a deployment that cannot enable RCSI substitutes its own
`JdbcDelegate` bean (the check belongs to the shipped dialect, not to substitutes) and owns the
trade-offs. The version store RCSI keeps in `tempdb` is the operator's to monitor, as in any RCSI
deployment.

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

## Adding a database

See [extensibility](../04-engineering/extensibility.md#how-to-add-a-new-database-dialect) for the
five-step recipe and the tests required.
