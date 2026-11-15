# Schema installation

Mohs has not released a schema upgrade contract. The first release uses one canonical installer per supported database. Install into an empty application-owned database or schema before starting workers.

| Database | Installer |
| --- | --- |
| PostgreSQL | [schema-postgresql.sql](../../mohs-store-jdbc/src/main/resources/schema-postgresql.sql) |
| MySQL | [schema-mysql.sql](../../mohs-store-jdbc/src/main/resources/schema-mysql.sql) |
| SQL Server | [schema-sqlserver.sql](../../mohs-store-jdbc/src/main/resources/schema-sqlserver.sql) |
| H2 | [schema-h2.sql](../../mohs-store-jdbc/src/main/resources/schema-h2.sql) |

The installers can be applied again to an already matching schema without dropping data. They are not diff engines: applying a current installer to a different pre-release layout does not upgrade it. Mohs ships no V1–V15 chain, backfill, dual-write, compatibility views or automatic conversion of development databases.

If an existing development database contains data that matters, retain a backup and plan an explicit export/recreation for that data. The refactoring and tests do not erase application databases. Disposable test containers install the canonical definitions directly.

The application owns credentials, database creation and startup ordering. SQL Server requires READ_COMMITTED_SNAPSHOT ON; startup verifies that requirement and does not alter it silently. H2 installs the `mohs_clock_now` alias supplied by the JDBC module so session expiry uses a fresh wall clock rather than transaction-start time. See [type contracts](type-contract.md) and [transactions](../04-engineering/transactions.md).

`SchemaInstallerTest` installs each database, creates a real session, reapplies the installer and compares columns, primary keys, nullability and indexes while verifying that the session survives. Protocol and round-trip tests validate the behavior of the resulting schema. Use the [benchmark guide](../10-performance/benchmarks.md) to collect evidence for the current schema; removed historical experiment reports are not included.
