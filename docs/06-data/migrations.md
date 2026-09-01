# Installing and upgrading the schema

Status: Active · Last Reviewed: 2026-08-30 · Source of Truth: Repository (`mohs-store-jdbc/src/main/resources/`)

> **You install the schema. Mohs never does.**
>
> There is no migration engine in the library, no `mohs.jdbc.migrate` property and no DDL executed at
> boot. Apply the schema **before** the application starts. An embedded library does not run DDL
> against a database it does not own: the database belongs to the host application, and in most
> organisations that run one at scale, to a team that does not accept an application changing the
> schema at startup — still less a third-party jar inside it.

## What to apply

| You are | Apply | Where it is in the jar |
| --- | --- | --- |
| Installing for the first time | `schema-<dialect>.sql` — the complete current schema | `schema-postgresql.sql` at the classpath root |
| Upgrading an existing database | the `V*.sql` deltas **after** the version you are on | `io/mohs/store/jdbc/migration/<dialect>/` |

`<dialect>` is one of `h2`, `postgresql`, `mysql`, `sqlserver` — the same four values as
`mohs.jdbc.dialect`.

**`schema-<dialect>.sql` does not upgrade.** It creates what is missing and leaves alone what is
there, so running it against a database at an older version gains you the new *tables* and none of
the `ALTER`s. A SQL Server database at `V7` needs `V8` run against it; nothing else will produce that
change. The numbering is global but the applicability is not: `V5` exists only for PostgreSQL and
`V8` only for SQL Server, so a gap in the numbers under your dialect's directory is expected, not a
missing file. The *Dialects* column in [the chain](#the-chain) says which files are yours.

## What happens if you forget

The application starts, wires its beans — a store's constructor only builds a `JdbcTemplate`, so
nothing notices — and fails on the first statement with the driver's own message:

```
ERROR: relation "mohs_rate_limits" does not exist
```

It creates nothing and corrupts nothing. But the raw error is not an instruction, so it is worth
saying here: that message means the schema was never applied.

## Which versions have you applied?

**Mohs does not know, and does not record it.** There is no history table — `mohs_schema_history`
went away with the migration engine. Tracking what a given database has seen is yours, and the
honest options are:

| Approach | Notes |
| --- | --- |
| Fold the `V*.sql` files into the host's own Flyway or Liquibase chain | The best answer if you already run one. Copy them in under your own versioning — but they are **not** single-statement SQL: the PostgreSQL deltas (`V2`, `V3`, `V5`) carry `DO $$ … END $$;` blocks whose inner `;` shreds a naive splitter. Flyway parses them natively; Liquibase needs `splitStatements="false"` on the `<sqlFile>` |
| Apply them from a deployment job, recording the version yourself | A table, a config value, a runbook line — anything you will actually keep current |
| Inspect the schema | Last resort. The [data model](data-model.md) says what each version added; `V8`, for instance, is visible as `pk_mohs_idempotency` being `NONCLUSTERED` |

This is the cost of the library not touching your database, and it is stated plainly rather than
hidden. What it buys is that no third-party jar inside your application changes your schema at
startup — which is the trade that was made, in that direction, deliberately.

## Applying it

### By hand

```bash
psql   -U postgres -d yourdb          -f schema-postgresql.sql
mysql  -u root -p   yourdb          <   schema-mysql.sql
sqlcmd -S localhost -U sa -d yourdb -i  schema-sqlserver.sql
```

### From a Spring Boot application

The ordinary Boot mechanism reaches the file inside the jar. This is what `mohs-demo` does, and the
shape a host copies for a dev or test profile:

```yaml
spring:
  sql:
    init:
      mode: always
      schema-locations: classpath:schema-h2.sql
```

`schema-<dialect>.sql` is idempotent, so a repeated start is harmless. For production, prefer your
existing migration tooling over `spring.sql.init`: schema changes belong to whatever already owns
them in your deployment.

> **This is for `schema-<dialect>.sql` only — never for the `V*.sql` deltas.** Boot splits a script
> on `;`, and the PostgreSQL deltas carry `DO $$ … END $$;` blocks it would cut in half. `V5` is the
> one that moves rows: cut in half, it leaves the new table created and the history *not* copied, and
> the application then starts and reads an empty table without ever raising an error. Apply the
> deltas with `psql`/`sqlcmd`, or with a migration tool that parses them.

One more sharp edge if you copy the block above: `schema-locations` **replaces** Boot's default
(`optional:classpath*:schema.sql`) rather than adding to it. If the host has its own `schema.sql`,
keep it — `schema-locations: classpath:schema-postgresql.sql,optional:classpath*:schema.sql`.

### Ordering, in a cluster

**Apply the schema first, then roll the replicas.** Nothing enforces this any more, and nothing
races either: replicas apply nothing, so there is no migration lock and no "exactly one applies each
version" to reason about. A replica that starts too early simply fails on its first statement and
does not start.

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
| `V9__due_trigger_index` | Adds `idx_mohs_job_next_fire`; `mohs_job_definitions` carried no index beyond its keys, and the engine reads it on every tick | All four |

Two of them are worth reading before you run them, and they have their own sections below: `V5`
moves rows and takes a table lock, and `V8` rebuilds a primary key.

## Every file is idempotent

`V1` is the earlier hand-written schema **verbatim**, guarded with `IF NOT EXISTS` (T-SQL:
`IF OBJECT_ID(...) IS NULL` and `IF NOT EXISTS (SELECT … FROM sys.indexes)`), and the same discipline
runs through the later ones.

That matters more now than it did: with no history table, a script re-run by accident — a redeployed
job, a rerun pipeline step — has to be harmless, because nothing will stop it.

Idempotency is guarded by **shape rather than by name** where a name would be ambiguous. `V2` drops
the owner index only when its definition lacks the `lease_expires_at` predicate:

```sql
IF EXISTS (SELECT 1 FROM pg_indexes
           WHERE tablename = 'mohs_executions' AND indexname = 'idx_mohs_executions_owner'
             AND indexdef NOT LIKE '%lease_expires_at%') THEN
    DROP INDEX idx_mohs_executions_owner;
END IF;
```

`CREATE INDEX IF NOT EXISTS` does **not** alter an index that already exists under the same name —
guarding by name alone would silently keep the wrong shape.

## Two copies of the truth, and what keeps them equal

`schema-<dialect>.sql` and the `V*.sql` chain describe the same schema by two routes. Nothing at
runtime reconciles them, so a delta that stops keeping up with the installer is not a failure on our
side — it is a wrong schema on the database of whoever was upgrading.

`SchemaPostgresChainMatchesInstallerTest` is what keeps them honest: it builds one database from
`schema-postgresql.sql` and another by applying `V1..Vn` in order, then compares every `mohs_*`
column and the complete definition of every index. If you fork the schema, keep that test passing.

## `V5` — the one migration that moves rows

PostgreSQL only, and the most operationally significant file in the tree. Read this before upgrading
a large database.

**What it does**: removes weekly partitioning from `mohs_execution` and `mohs_attempt`. There is no
`ALTER` that converts a partitioned table to a normal one, so the tables are **recreated and the
data copied**.

### The order, and why each step is where it is

```
1. guard: is it actually partitioned?      → if not, RETURN (no-op)
2. guard: is the isolation READ COMMITTED? → if not, RAISE
3. guard: is the table pair complete?      → if not, RAISE
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
| **`lock_timeout` is the mandatory pair of the lock** | An `AccessExclusiveLock` request that enters the queue blocks everyone behind it, **reads included**. Better a fast, visible failure that can be retried than an outage of the history table behind some unrelated long transaction. 2 s, not 10 s: if the lock does not come quickly, it will not come |
| **Both tables are locked at once, in writer order** | `JdbcLeaseStore` completes attempt → execution. Locking in the inverse order would deadlock against a live completion; locking `mohs_attempt` only afterwards would leave the whole `mohs_execution` copy as a loss window |
| **The isolation check** | Under `REPEATABLE READ` or `SERIALIZABLE` the `INSERT … SELECT` sees the snapshot from *before* the lock — the instant the transaction first read anything — and the `DROP` takes everything committed in between, the same silent loss the lock exists to close. PL/pgSQL cannot change the isolation level, and the level comes from the **host's** `DataSource`, so the migration **refuses to run blind** rather than betting on the default. `READ UNCOMMITTED` is accepted because PostgreSQL executes it as `READ COMMITTED` |
| **The incomplete-pair guard raises rather than skipping the lock** | A guard that merely *skipped* the lock would let the copy run unsealed — the silent loss back again, and quiet |
| **Constraint renames** | `RENAME TO` moves the table but not its constraints. From PG 17 onward the `NOT NULL` constraints also have `pg_constraint` entries and would inherit `_flat_`. **A constraint name is contract** — it appears in error messages and in `ALTER … DROP CONSTRAINT`, and the structural guardian compares it across both installation paths |
| **`ANALYZE`** | A recreated table wakes with `reltuples = -1` and no column statistics; autoanalyze only passes at the next naptime (up to 60 s), which is exactly the minute after the deploy |

### The operational cost, stated plainly

- Peak space: **2× the larger of the two tables**, plus indexes.
- WAL proportional to the copy.
- The tables are **sealed — not even readable** — from the `LOCK` until the `COMMIT`.

**On a large database this is a maintenance window, not a routine deploy.** Run it in a
`READ COMMITTED` session during a window, and **take a backup first: there is no undo.**

**Apply it in a single transaction.** PostgreSQL has transactional DDL, and only inside one
transaction does a failure after the `DROP` roll the whole conversion back — verified. Applied
statement by statement, a failure in the middle can leave a half-converted schema.

**Drain the cluster first.** With nodes still running, the pool is exhausted waiting on the sealed
tables, the heartbeat cannot get a connection, and the peers reap a live node.

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
| Make it **idempotent** | Nothing records what ran; an accidental re-run has to be harmless |
| Guard by **shape** where a name is ambiguous | `CREATE … IF NOT EXISTS` does not alter an existing object |
| **Never edit a published file** | Nothing validates checksums any more. An edited delta does not fail a boot — it silently leaves two databases with different schemas depending on when each was installed |
| Write it for **every dialect** it applies to | Or document why it is dialect-specific, as `V5` and `V8` do |
| Update the matching `schema-<dialect>.sql` | They are two copies of one truth, and the structural guardian compares them |
| State the operational cost in a header comment | Especially if it moves rows or takes a table-level lock. The comment is what the operator reads before running it by hand |

## Rollback

**There is no automated rollback**, and no down-migrations exist.

| Situation | Path back |
| --- | --- |
| A failed delta | Fix forward. Applied in one transaction, a PostgreSQL failure rolls back to the previous state |
| Rolling back the application jar | The schema is generally forward-compatible: `V2`'s comment notes that `lease_expires_at` was kept precisely to support rollback to the previous jar, and `V1`'s `expires_at` nullability provides mixed-version tolerance |
| Undoing `V5` | **Not possible** without restoring a backup. Take one before running it on a large database |
