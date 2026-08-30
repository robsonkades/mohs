# DR-011: The library owns its schema, in its own Flyway history table

## Status

Accepted

## Context

Mohs is embedded ([DR-001](DR-001-embedded-library-not-a-server.md)), so its nine tables live in the
host application's database and schema. That host very likely runs Flyway itself, with its own
`flyway_schema_history`.

Two failure modes follow immediately. If Mohs used the host's Flyway instance, it would hijack a
migration chain it does not own. If it used the default history table name, the two chains would
collide.

There is also a **third** problem, and it is the one that shapes the design: the schema is shared, so
"the database is non-empty" is the **rule**, not the exception — and Flyway refuses to migrate a
non-empty schema with no history.

## Decision

Mohs runs **its own Flyway instance**, with:

| Setting | Value | Reason |
| --- | --- | --- |
| `table` | **`mohs_schema_history`** | Never `flyway_schema_history`, which belongs to the host |
| `locations` | `classpath:io/mohs/store/jdbc/migration/<dialect>` | Outside the default `db/migration`, so the host's own Flyway never sweeps it |
| `baselineOnMigrate` | `true` | Required to migrate a non-empty schema |
| `baselineVersion` | **`0`** | A baseline at 0 skips **no** migration. The trap is the default of 1, which would mark `V1` as applied in a database where only the host's tables exist, and would then never create Mohs' own |

And a second decision that makes adoption possible: **`V1` is the earlier hand-written schema
verbatim, and idempotent** (`IF NOT EXISTS`; on T-SQL, `IF OBJECT_ID(...) IS NULL` and
`IF NOT EXISTS (SELECT … FROM sys.indexes)`).

A parallel installation path is maintained: `schema-<dialect>.sql`, for a DBA-managed schema with
`mohs.jdbc.migrate=false`. **Round-trip tests assert the two paths produce the same schema.**

## Consequences

### Positive

- **An existing installation adopts Flyway for free**: tables created by hand, with no history, run
  `V1` as a no-op and gain the history table.
- **Parallel replica start is safe.** Flyway's locking on the history table gives mutual exclusion,
  and `ConcurrentMigrationScenario` asserts exactly one replica applies each version with none failing
  to boot.
- **Ordering is guaranteed by the dependency graph, not registration order.** Every bean that touches
  a Mohs table takes `MohsFlyway` as a constructor parameter — so even a *host* bean that injects
  `Mohs` and writes in its own constructor passes through the migration first.
- The externally-managed path is genuinely supported, and the round-trip tests stop the two paths
  silently diverging — which would otherwise be a support nightmare.

### Negative

- **Two Flyway instances run in one application**, which is surprising until explained.
- **Every migration must be written for four dialects** (or explain why it is dialect-specific, as `V5`
  and `V8` do), and the matching `schema-*.sql` must be kept in step.
- **Idempotency guards must be by *shape*, not by name**, where a name is ambiguous: `CREATE INDEX IF
  NOT EXISTS` does **not** alter an index that already exists under the same name, so `V2` inspects the
  index definition rather than trusting the name.
- **One migration needs a maintenance window.** PostgreSQL's `V5` copies the whole history under an
  `ACCESS EXCLUSIVE` lock: peak space is 2× the larger table plus indexes, and the tables are sealed —
  not even readable — for the duration. It also **refuses to run outside `READ COMMITTED`** rather than
  silently losing rows written by another node during the copy, a loss reproduced on PG 18.
- Adding a dialect means adding a migration folder, a `schema-*.sql`, and three tests.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Use the host's Flyway instance | Hijacks a chain Mohs does not own; the host controls its locations and its baseline |
| Use `flyway_schema_history` | Direct collision with the host's own chain |
| `baselineVersion = 1` (the default) | Would mark `V1` as applied in a database containing only the host's tables, and Mohs' schema would never be created |
| Ship only `schema-*.sql` and require a DBA | Puts an upgrade burden on every consumer for every version |
| Ship only migrations, with no hand-install path | Removes the option for organisations where the application has no DDL rights |
| Auto-create the schema with `ddl-auto`-style generation | There is no ORM, and the schema's index design *is* the performance story |

## Evidence

- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/MohsFlyway.java` — the configuration and the full
  reasoning, including the `baselineVersion = 0` trap.
- `mohs-spring-boot-starter/src/main/java/io/mohs/autoconfigure/MohsAutoConfiguration.java` — the
  dependency-graph ordering.
- `V5__drop_partitioning.sql` — the isolation guard, the lock ordering, and the declared operational
  cost.
- `ConcurrentMigrationScenario`, `MohsFlyway*Test`, `Schema*RoundTripTest`.
