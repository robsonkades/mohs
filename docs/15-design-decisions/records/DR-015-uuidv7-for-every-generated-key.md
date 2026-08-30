# DR-015: Every generated primary key is UUIDv7

## Status

Accepted

## Context

Three key strategies were available for a table that takes the system's highest insert rate:

- **Database-sequential** (`IDENTITY`, `SERIAL`, `AUTO_INCREMENT`, `SEQUENCE`) — requires a round trip
  before the row exists, or a dialect-specific generated-keys dance.
- **UUIDv4** — client-side and allocation-free, but **pure randomness**, so inserts scatter across the
  whole index instead of staying at the tail.
- **UUIDv7** — client-side, time-ordered, and lexicographically sortable as a string.

## Decision

**Every generated primary key is UUIDv7**, from `io.github.robsonkades:uuidv7`, on every dialect.

Two prohibitions travel with it, and one exemption:

| Rule | Enforcement |
| --- | --- |
| No `UUID.randomUUID()` anywhere | **ArchUnit**, matched by the target's *owner*, using `accessTargetWhere` rather than `callMethod` so a method **reference** (`UUID::randomUUID`) cannot slip through |
| No `IDENTITY` / `SERIAL` / `AUTO_INCREMENT` / `SEQUENCE` in any schema | **Prose only** — ArchUnit does not read SQL. The gap is declared in the rule's own Javadoc |
| Natural keys are fine | `job_key`, `mohs_rate_limits.name`, `mohs_attempt.number` — what is banned is the database-sequential surrogate |

Ids are stored as `VARCHAR(255)` / `NVARCHAR(255)` rather than a native UUID type, for portability
across four dialects.

## Consequences

### Positive

- **Insert locality.** Time-ordered keys keep inserts at the index tail on the hottest table in the
  system, instead of scattering them as v4 would.
- **No allocation round trip.** The id exists before the row does, which is what lets the enqueue unit
  build history, queue and idempotency rows in one transaction with the id already known — and lets
  `Enqueued` be returned as a receipt with a durable `executionId`.
- **Keyset pagination is chronological for free.** `GET /executions` orders by descending id and gets
  "most recent first" without a timestamp column in the index.
- **The rule is executable**, including the method-reference form that a naive `callMethod` rule would
  miss, and it does **not** trip on `UUIDv7` legitimately returning a `java.util.UUID`.

### Negative

- **A dependency on a small third-party library** (`io.github.robsonkades:uuidv7`), which the project's
  own rules treat as significant.
- **36 bytes per id as text**, against 8 for a bigint — larger indexes and larger rows.
- **Half the invariant is unenforced.** ArchUnit cannot read SQL, so the ban on sequential keys in a
  schema is prose. A SQL scan would close it, and is recorded as a recommendation.
- Ids are opaque strings, so anyone wanting the embedded timestamp must decode the UUID themselves.

## A worked consequence: the SQL Server key-length limit

The decision interacts with another one — `NVARCHAR` everywhere on SQL Server — and produced a hard
failure that had to be designed around rather than discovered.

With `NVARCHAR`, `mohs_idempotency`'s composite key `(job_key, idempotency_key)` measures
`2 × (255 × 2) = 1020` bytes, above the **900-byte ceiling for a clustered index** (the
non-clustered ceiling rose to 1700 in SQL Server 2016+; the clustered one did not). Measured: 225+225
characters inserts, **256+255 fails with `Msg 1946` at enqueue time**.

The resolution keeps the four dialects consistent rather than narrowing the column: the PK becomes
`NONCLUSTERED` and the clustered index becomes `created_at` — which is monotonic, so inserts stay at
the tail and the retention prune becomes a range delete on the clustered index itself. Both sides of
that trade are recorded, including the ones against it (two structures maintained per insert, the
dedup `SELECT` becoming a seek plus key lookup, and `PAGELATCH_EX` concentration on the last page).

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| `IDENTITY` / `SERIAL` / `SEQUENCE` | A round trip before the row exists, and a different mechanism per dialect. It would also make the enqueue unit's single transaction awkward |
| UUIDv4 | Unordered, so it shatters insert locality on the hottest table, and it is not sortable as a cursor |
| ULID | Equivalent properties; UUIDv7 is the standardised form and has a JDK-compatible representation |
| A native `uuid` column type | Not portable across the four dialects |
| Composite natural keys | `job_key` is one, and it is used. Executions have no natural key |

## Evidence

- `mohs-demo/src/test/java/io/mohs/ArchitectureTest.java` — `ids_are_generated_as_uuidv7_never_v4`,
  with the `accessTargetWhere` rationale and the declared SQL gap.
- `mohs-engine/src/main/java/io/mohs/engine/Engine.java`,
  `mohs-engine/src/main/java/io/mohs/engine/MohsImpl.java`,
  `mohs-engine/src/main/java/io/mohs/engine/ScheduleCommandImpl.java` — every generation site.
- `mohs-store-jdbc/src/main/resources/io/mohs/store/jdbc/migration/sqlserver/V8__idempotency_clustered_key.sql`
  — the key-length consequence, with both sides of the trade.
- `mohs-api/src/main/java/io/mohs/core/ExecutionQuery.java` — cursor pagination relying on the
  ordering.
