# Migrations

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (`io.mohs.store.jdbc.MohsFlyway` and `src/main/resources/io/mohs/store/jdbc/migration/`)

## The library owns its schema

Mohs runs **its own Flyway instance** with **its own history table**, `mohs_schema_history`.

Never `flyway_schema_history`. Mohs is an embedded library sharing a database with a host
application that may have its own Flyway with its own history table — hijacking or colliding with
the host's migration chain is the classic embedded-library defect.

```java
Flyway.configure()
      .dataSource(dataSource)
      .table("mohs_schema_history")
      .locations(dialect.migrationLocation())   // e.g. classpath:io/mohs/store/jdbc/migration/postgresql
      .baselineOnMigrate(true)
      .baselineVersion("0")
      .load()
      .migrate();
```

The classpath location sits **outside** the default `db/migration` on purpose, so it is never swept
by the host's own Flyway.

### `baselineVersion = 0`, and why not 1

Flyway requires a baseline to migrate a non-empty schema with no history — and "non-empty" here is
the **rule**, not the exception, because the schema is shared with the host.

A baseline at 0 skips **no** migration at all. The trap would be the default of 1, which would mark
`V1` as applied in a database where only the host's tables exist, and would then never create
Mohs' own.

## The chain

| Version | Purpose | Dialects |
| --- | --- | --- |
| `V1__mohs_baseline` | The original hand-written schema, **verbatim and idempotent** | All four |
| `V2__node_lease` | Adds `epoch` and `expires_at` to `mohs_nodes`; reshapes the owner index | All four |
| `V3__table_split` | Creates `mohs_ready`, `mohs_lease`, `mohs_execution`, `mohs_attempt`, `mohs_idempotency` **alongside** the old tables (expand) | All four |
| `V4__drop_legacy_tables` | Drops `mohs_attempts` and `mohs_executions`; drops `running_execution_count` (contract) | All four |
| `V5__drop_partitioning` | Converts the two partitioned history tables to normal tables | **PostgreSQL only** |
| `V6__batch_name` | Adds `mohs_batches.name`, backfills with the id, then sets `NOT NULL` | All four |
| `V7__idempotency_retention_index` | Adds `idx_mohs_idempotency_created`; drops `idx_mohs_execution_created` where it existed | All four |
| `V8__idempotency_clustered_key` | Makes the PK `NONCLUSTERED` and clusters on `created_at` | **SQL Server only** |

## Every migration is idempotent

`V1` is the earlier hand-written schema **verbatim**, guarded with `IF NOT EXISTS` (T-SQL:
`IF OBJECT_ID(...) IS NULL` and `IF NOT EXISTS (SELECT … FROM sys.indexes)`).

That is what lets an **existing installation adopt Flyway**: tables created by hand from
`schema-*.sql`, with no history, run `V1` as a no-op and gain the history table. `V2` onwards are
ordinary deltas, applied once.

The same discipline runs through the later migrations, and it is guarded by **shape rather than by
name** where a name would be ambiguous. `V2` drops the owner index only when its definition lacks
the `lease_expires_at` predicate:

```sql
IF EXISTS (SELECT 1 FROM pg_indexes
           WHERE tablename = 'mohs_executions' AND indexname = 'idx_mohs_executions_owner'
             AND indexdef NOT LIKE '%lease_expires_at%') THEN
    DROP INDEX idx_mohs_executions_owner;
END IF;
```

`CREATE INDEX IF NOT EXISTS` does **not** alter an index that already exists under the same name —
guarding by name alone would silently keep the wrong shape.

## Two installation paths, kept in step by tests

| Path | Files | When |
| --- | --- | --- |
| Flyway | `io/mohs/store/jdbc/migration/<dialect>/V*.sql` | The default (`mohs.jdbc.migrate=true`) |
| Hand install | `schema-<dialect>.sql` | A DBA-managed schema, with `mohs.jdbc.migrate=false` |

`SchemaPostgresRoundTripTest`, `SchemaMySqlRoundTripTest` and `SchemaSqlServerRoundTripTest` assert
that both paths produce the **same** schema against a real container. Two installation routes that
silently diverge would be a support nightmare.

## Parallel replica start

`ConcurrentMigrationScenario` covers the first instant of every deploy: N replicas start
simultaneously against an empty database, each calling `migrate()` on boot, with nobody coordinating
the order.

What it asserts: **exactly one replica applies each version**, none fails to boot, and the resulting
schema matches that of a lone migration. The start is a `CountDownLatch`, not a sleep — the whole
value of the scenario is that every replica reaches `migrate()` within the same window of
microseconds.

Flyway's own locking on the history table is what provides the mutual exclusion.

## Ordering: why the dependency graph, not registration order

`MohsFlyway` is a bean, and **every bean that touches a Mohs table takes it as a constructor
parameter** — the stores, the work queue, the trigger firer, the node store. The migration therefore
runs when that bean is created, and the ordering is guaranteed by the **dependency graph** rather
than by registration order.

That matters for a specific host-side case: a host bean that injects `Mohs` and writes in its own
constructor forces the whole chain and still passes through the migration first. Mohs' own writers
were already late by construction — the scanner and the rate-limit registrar are
`afterSingletonsInstantiated`, and the engine is a `SmartLifecycle` — but the host had no edge at
all.

The bean is **always** present; `mohs.jdbc.migrate=false` only skips the `migrate()` call.

## `V5` — the one migration that moves rows

PostgreSQL only, and the most operationally significant migration in the tree. Read this before
upgrading a large database.

**What it does**: removes weekly partitioning from `mohs_execution` and `mohs_attempt`. There is no
`ALTER` that converts a partitioned table to a normal one, so the tables are **recreated and the
data copied**.

### The order, and why each step is where it is

```
1. guard: is it actually partitioned?   → if not, RETURN (no-op)
2. guard: is the isolation READ COMMITTED? → if not, RAISE
3. guard: is the table pair complete?   → if not, RAISE
4. SET LOCAL lock_timeout = '2s'
5. LOCK TABLE mohs_attempt, mohs_execution IN ACCESS EXCLUSIVE MODE
6. CREATE the flat table
7. INSERT ... SELECT  (explicit column lists on BOTH sides)
8. DROP the old table (partitions go with it)
9. RENAME the new one
10. RENAME the constraints back (RENAME TO moves the table, not its constraints)
11. recreate the indexes
12. ANALYZE
```

| Step | Why it is exactly there |
| --- | --- |
| **The lock comes before the copy** | Without it, another live node can commit an `INSERT` between the copy's snapshot and the `DROP`, and that row dies with the old table — **with the migration reporting success**. Reproduced on PG 18: 1,000 rows copied, 1 concurrent commit, 1,000 rows at the end. With the seal, the concurrent writer *blocks* instead of losing the write |
| **`lock_timeout` is the mandatory pair of the lock** | An `AccessExclusiveLock` request that enters the queue blocks everyone behind it, **reads included**. Better a fast, visible migration failure that can be retried than an outage of the history table behind some unrelated long transaction. 2 s, not 10 s: if the lock does not come quickly, it will not come |
| **Both tables are locked at once, in writer order** | `JdbcLeaseStore` completes attempt → execution. Locking in the inverse order would deadlock against a live completion; locking `mohs_attempt` only afterwards would leave the whole `mohs_execution` copy as a loss window |
| **The isolation check** | Under `REPEATABLE READ` or `SERIALIZABLE` the `INSERT … SELECT` sees the snapshot from *before* the lock (Flyway has already read the history table in this transaction), and the `DROP` takes everything committed in between — the same silent loss the lock exists to close. PL/pgSQL cannot change the isolation level, and the level comes from the **host's** `DataSource`, so the migration **refuses to run blind** rather than betting on the default. `READ UNCOMMITTED` is accepted because PostgreSQL executes it as `READ COMMITTED` |
| **The incomplete-pair guard raises rather than skipping the lock** | A guard that merely *skipped* the lock would let the copy run unsealed — the silent loss back again, and quiet |
| **Constraint renames** | `RENAME TO` moves the table but not its constraints. From PG 17 onward the `NOT NULL` constraints also have `pg_constraint` entries and would inherit `_flat_`. **A constraint name is contract** — it appears in error messages and in `ALTER … DROP CONSTRAINT`, and the structural schema guardian compares both installation paths |
| **`ANALYZE`** | A recreated table wakes with `reltuples = -1` and no column statistics; autoanalyze only passes at the next naptime (up to 60 s), which is exactly the minute after the deploy |

### The operational cost, stated plainly

- Peak space: **2× the larger of the two tables**, plus indexes.
- WAL proportional to the copy.
- The tables are **sealed — not even readable** — from the `LOCK` until the `COMMIT`.

**On a large database this is a maintenance window, not a routine deploy.** The recommended path is
to run `V5` by hand in a `READ COMMITTED` session during a window; the boot then passes over it,
because the no-op guard exits before requiring anything.

Flyway runs each migration in one transaction and PostgreSQL has transactional DDL, so a failure
after the `DROP` rolls the whole conversion back — verified. **There is no half-converted schema.**

## `V8` — SQL Server's clustered-key change

A hard limit rather than a preference. See [data model](data-model.md#mohs_idempotency) for the full
reasoning; the migration itself:

1. If the PK is currently `CLUSTERED`, drop it (looking the name up in `sys.indexes` rather than
   assuming one).
2. Create `ix_mohs_idempotency_created` as the clustered index on `created_at`.
3. Re-add the PK as `NONCLUSTERED`.
4. Drop `idx_mohs_idempotency_created` from `V7` — redundant in this dialect, since the clustered
   index now covers it.

## Adding a migration

| Rule | Reason |
| --- | --- |
| Make it **idempotent** | The hand-install path must be able to adopt Flyway |
| Guard by **shape** where a name is ambiguous | `CREATE … IF NOT EXISTS` does not alter an existing object |
| **Never edit an applied migration** | Flyway validates checksums |
| Write it for **every dialect** it applies to | Or document why it is dialect-specific, as `V5` and `V8` do |
| Update the matching `schema-<dialect>.sql` | The round-trip tests compare the two paths |
| State the operational cost in a header comment | Especially if it moves rows or takes a table-level lock |
| Add or extend the dialect's `MohsFlyway*Test` | It is the structural guardian |

## Rollback

**There is no automated rollback.** Flyway "undo" is not used, and no down-migrations exist.

| Situation | Path back |
| --- | --- |
| A failed migration | Fix forward. A PostgreSQL failure rolls back to the previous state within its own transaction |
| Rolling back the application jar | The schema is generally forward-compatible: `V2`'s comment notes that `lease_expires_at` was kept precisely to support rollback to the previous jar without a migration, and `V1`'s `expires_at` nullability provides mixed-version tolerance |
| Undoing `V5` | **Not possible** without restoring a backup. Take one before running it on a large database |
