# Canonical schema

The executable DDL lives in the JDBC module. Documentation does not maintain a second copy of CREATE TABLE statements:

- [PostgreSQL](../../mohs-store-jdbc/src/main/resources/schema-postgresql.sql)
- [MySQL](../../mohs-store-jdbc/src/main/resources/schema-mysql.sql)
- [SQL Server](../../mohs-store-jdbc/src/main/resources/schema-sqlserver.sql)
- [H2](../../mohs-store-jdbc/src/main/resources/schema-h2.sql)

Use the [installation contract](migrations.md) for a fresh database and [type contract](type-contract.md) for JDBC/domain alignment. Job/resource/idempotency keys compare exactly, generated identities have 36 characters, instants use UTC microseconds, durations use BIGINT microseconds and batch counters use BIGINT.

`job_key` is the definition primary key. Every definition names a registered executable and immutable input contract. A named recurrence also stores its current input snapshot. Execution snapshots retain their own input independently of later changes to the recurrence. Java class names are not persisted.

`mohs_catalog` arbitrates publication and first session join even when there are no definitions or nodes. An execution's immutable birth and latest attempt are distinct from active acquisition state. A receipt can outlive execution history. `mohs_attempt` and `mohs_job_bindings` are absent; historical migrations that introduced or removed them are not runtime compatibility mechanisms.

Application code must use the complete storage operations for acceptance, firing, claim, completion, cancellation and retry. Direct manipulation of individual tables can violate invariants that cross rows; reducing table count is not a substitute for these transactions. The supported read configuration exposes a coherent committed joined row. A PENDING execution with no work, or contradictory work membership, raises an invariant diagnostic.
