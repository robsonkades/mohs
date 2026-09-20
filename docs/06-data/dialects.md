# Dialects

The configured dialect is explicit: `mohs.jdbc.dialect` accepts `postgresql`, `mysql`, `sqlserver` or `h2`. H2 remains a test/development backend. The JDBC adapter implements the same complete-operation contracts and is tested on all four databases.

## Shared SQL and actual differences

`JdbcDelegate` supplies common statements. Concrete delegates override syntax or execution shape only where necessary. Named-parameter drift checks complement real protocol tests; text equality alone does not prove locking correctness. Complete transactions also contain SQL in their cohesive operation implementations, such as catalog publication and acceptance.

| Concern | PostgreSQL | MySQL | SQL Server | H2 |
|---|---|---|---|---|
| Operational timestamp | TIMESTAMP(6) | DATETIME(6) | DATETIME2(6) | TIMESTAMP(6) |
| Database observation | clock_timestamp() | UTC_TIMESTAMP(6) | SYSUTCDATETIME() | Fresh offset-aware clock alias |
| Claim | Locking picked CTE and UPDATE RETURNING | Locking pick then guarded UPDATE | UPDATE OUTPUT over a TOP pick, with UPDLOCK/ROWLOCK/READPAST and LOOP JOIN | Locking pick then guarded UPDATE |
| Limit | LIMIT | LIMIT | TOP | LIMIT |
| Ready/leased indexes | Partial | Composite with work_state | Filtered | Composite with work_state |
| Text keys | VARCHAR with C collation | ASCII binary | NVARCHAR with BIN2 collation | VARCHAR exact comparison |

Claims change one work row from READY to LEASED, clearing visibility and assigning session, token and grant time. They do not move records between queue and lease tables. The returned order is priority, visibility and execution ID within the selected candidate set. There is no global FIFO contract.

Session and canonically ordered job locks decide admission and strict positive per-job concurrency caps. Candidate row skipping avoids selected row waits where supported; it does **not** promise that the whole operation never waits. Session/job locks and index access can wait or deadlock. The own transaction uses READ_COMMITTED and a deadline; a failed transaction must be retried as a complete operation.

The claim's eligibility horizon cannot exceed a fresh database observation. Its persisted grant time comes from another observation after admission locks. Completion checks session, execution, token and attempt in its transaction. Retry changes the same work row back to READY; terminal completion removes it and commits history, batch counters and matching fixed-delay rearm together.

## SQL Server read configuration

`SqlServerRcsiRequirement` requires READ_COMMITTED_SNAPSHOT ON. Plain joined reads then observe a committed execution/work pair without dirty-read hints. Ownership writes retain explicit locks. The setting is checked at startup; there is no weaker read-mode fallback.

On a fresh database, configure it before starting Mohs:

```sql
ALTER DATABASE [your_database] SET READ_COMMITTED_SNAPSHOT ON;
```

For an existing shared database, coordinate the change with its owner. This command can wait for other transactions. Monitor row-version storage with the database's other resources.

SQL Server uses NVARCHAR for text, COUNT_BIG for large counters and explicit BIGINT limit parameters. Bounded IN lists stay below its parameter ceiling. OUTPUT without INTO also means application triggers must not be installed on the work target.

## Binding precision and remaining optimizer conversions

All operational instants cross JDBC as UTC LocalDateTime, with eligibility rounded up and observations rounded down to microseconds. No query needs CAST, CONVERT or string-to-date parsing to represent these values. See the [type contract](type-contract.md).

The pinned Microsoft driver 13.4 declares ordinary timestamp parameters as datetime2 without explicit scale, including when setObject receives scale 6. SQL Server treats this as scale 7. The driver source's Parameter.GetTypeDefinitionOp passes a null scale outside its encryption-specific branch; a real parameter-metadata test reproduces it. Changing the standard call or using the vendor five-argument call did not change that metadata in the probe.

Inspect actual plans for parameter width, collation and timestamp-scale conversions on the target database. The current schema retains the common six-digit timestamp contract; no archived plan report is included in this repository. Do not enable encryption or change query semantics merely to influence parameter metadata.

Primary source: [Microsoft JDBC 13.4 Parameter implementation](https://github.com/microsoft/mssql-jdbc/blob/v13.4.0/src/main/java/com/microsoft/sqlserver/jdbc/Parameter.java).

## Installation and testing

The four [installers](schema.md) are the first-release baseline. There are no V1–V15 execution paths or SQL wrappers that translate previous schemas. MySQL uses DATETIME to preserve the supported year range; PostgreSQL now stores zoneless UTC timestamps, so the former TIMESTAMPTZ binding rationale is historical. Cursor streaming still requires an appropriate connection/transaction lifetime and is separate from bounded coordinator pages.

Keep cross-database tests for acquisition races, stale owners, unknown commit, recovery versus heartbeat, cancellation/retry, catalog admission, exact key comparison, time precision and row shapes. H2's results do not substitute for the three server engines.
