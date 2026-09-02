# DR-001 — `READ_COMMITTED_SNAPSHOT` is a boot requirement of the SQL Server dialect

Status: Accepted · Date: 2026-09-01 · Closes: TD-06

## Context

SQL Server is the only supported dialect whose default `READ COMMITTED` is locking: plain reads
take shared locks, and the claim holds exclusive ones on the three hot tables. Both failure sides
were **measured** on SQL Server 2022:

- **Blocking.** With one uncommitted claim holding X locks on 1,000 `mohs_ready` rows,
  `countActiveInQueue` did not merely contend — it blocked to a lock timeout (`Msg 1222`). The
  dashboard's SSE stream runs these counts every 2 s, so an ordinary claim burst turns the overview
  into a stream of timeouts.
- **Wrong numbers.** The lock-free alternative, `WITH (NOLOCK)`, read a queue depth of 49,000
  against 50,000 committed rows: it saw the uncommitted `DELETE` of a claim that then rolled back.
  On an operational dashboard a wrong number is indistinguishable from a right one. `NOLOCK` also
  admits double-counted/lost rows under page splits and error 601 ("data movement").

With `READ_COMMITTED_SNAPSHOT ON` (RCSI) the same count answered correctly and without blocking.
Row versioning is the only option that is both non-blocking and correct. Azure SQL Database ships
with RCSI ON; on-premises servers default to OFF — which makes the misconfiguration both likely and
silent: it passes every functional test and degrades only under concurrent load.

## Decision

RCSI is a **stated requirement** of the SQL Server dialect, verified at boot and refused loudly:

1. `SqlServerRcsiRequirement.verify(DataSource)` (in `mohs-store-jdbc`) inspects
   `sys.databases.is_read_committed_snapshot_on` for the connection's database. OFF fails the boot
   with the exact `ALTER DATABASE [db] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE` to
   run; an inspection that cannot run fails the boot with the `SQLException` as the cause.
2. The starter runs the check when `mohs.jdbc.dialect=sqlserver` is selected, at delegate creation —
   the earliest bean that knows the dialect, before anything else touches the database. "Before" is
   guaranteed by bean dependency, not by luck: every bean that reaches the database (`mohsClock` in
   `database` mode, the stores, the engine) injects the delegate, so the container cannot create any
   of them first.
3. The two `WITH (NOLOCK)` hints on the idle-gate probe (`visibleWorkExists`, `visibleWorkCount`)
   are **retired**: under required RCSI the plain read is non-blocking and correct, and the hint
   would force read-uncommitted through the versioned read. `SqlServerJdbcDelegate` now carries no
   read hints at all, and both statements became byte-identical to the other three dialects (seven
   genuine T-SQL divergences became five).
4. The SQL Server test support runs the whole suite against a dedicated database with RCSI ON — the
   supported configuration — keeping `master` (which cannot enable RCSI) as the permanent negative
   case for the requirement's own test.

There is **no opt-out property**. A deployment that genuinely cannot enable RCSI already has the
dialect's extension point: declare a custom `JdbcDelegate` bean and own the trade-offs — the check
belongs to the shipped dialect, not to substitutes.

## Consequences

- A misconfigured SQL Server fails at boot with an actionable message, instead of degrading in
  production under load. Boot now requires the database to be reachable when the dialect is
  `sqlserver` (the other dialects hit the database moments later, at engine start).
- The overview counts, the idle-gate probe and the backlog gauge are non-blocking **and correct**
  on all four dialects, with the guarantee coming from row versioning rather than per-statement
  hints.
- The operator must enable RCSI once per database (a no-op on Azure SQL). Documented in
  [dialects](../../06-data/dialects.md); the version-store cost in `tempdb` that RCSI introduces is
  the operator's to monitor, as with any RCSI deployment.
- Error 601 disappears from the failure catalogue (it was a `NOLOCK` artifact).

## Alternatives considered

- **Spread `WITH (NOLOCK)` to the overview counts** — rejected: measured wrong numbers (49,000 vs
  50,000), plus double-count/lost-row anomalies and error 601. A dashboard that lies is worse than
  one that blocks.
- **WARN at boot instead of refusing** — rejected: the failure mode is silent by nature; one WARN
  line at boot is exactly the kind of signal that is discovered at 3 a.m., after the incident. The
  precedent is `mohs.time.mode=database`, which also refuses to start on an unverifiable premise.
- **An opt-out property (`require-rcsi=false`)** — rejected: configuration for a scenario nobody
  has, and the escape hatch already exists (a substituted `JdbcDelegate` skips the check).
- **Keep `NOLOCK` on the probe only, hint the counts case by case** — rejected: it splits one
  correctness argument across statements and leaves every future read to re-decide it.
