# DR-012: The SQL dialect is an explicit choice, never auto-detected

## Status

Accepted

## Context

Mohs supports four databases and its hottest statement differs between them: PostgreSQL can express
the whole claim as one CTE statement, SQL Server has no `SKIP LOCKED` and puts its row limit in a
different **position** in the query, and the split tables are `TIMESTAMPTZ` on PostgreSQL and zoneless
elsewhere.

The convenient thing is to detect the database from `Connection.getMetaData()` and choose
automatically. Frameworks do it; so does Hibernate.

## Decision

**`mohs.jdbc.dialect` is mandatory, with no default. An unset value fails the boot**, naming the four
valid values (`h2`, `postgresql`, `mysql`, `sqlserver`).

The reasoning, from `JdbcDialect`'s own Javadoc: detection through `getMetaData()` is **fragile across
driver forks and versions**. This is the same pattern Quartz uses with
`org.quartz.jobStore.driverDelegateClass`.

Two design decisions travel with it:

1. **Each implementation owns the claim's entire SQL template**, not concatenable fragments. SQL
   Server's `TOP` changes *position*, so a composition of generic fragments does not close cleanly —
   which is how Hibernate actually implements `LimitHandler` underneath, and the shape Quartz's
   delegates use.
2. **Every supported database gets its own class, even where the SQL is identical today**, so as not
   to couple independent databases to a present-day coincidence of syntax.

## Consequences

### Positive

- **The dialect is visible in configuration**, so an operator reading `application.yaml` knows which
  SQL will run.
- **No surprise on a driver upgrade.** A fork like a managed-cloud driver that reports a different
  product name cannot silently change the SQL.
- **H2 can be warned about specifically.** Selecting it logs a WARN at boot stating that H2 is a
  test/dev backend, not supported in production — because its `SKIP LOCKED` has a measured ~33%
  double-lock race. Claim correctness still holds (it comes from the guarded CAS), but nobody should
  discover that in production.
- The interface stays small: only `migrationLocation`, `topClause`, `limitClause`,
  `lockFreeReadHint`, the two `splitTimestamp` crossings, and the two claim methods — modelled on
  Hibernate's `LimitHandler`/`LockingStrategy` shape **without taking Hibernate as a dependency**.

### Negative

- **One more mandatory property.** It is the only one Mohs requires, and its absence is the most
  common first-boot failure.
- **A mismatch is possible**: nothing stops `dialect: mysql` against a PostgreSQL `DataSource`. The
  failure is a SQL syntax error at first use rather than a clear message at boot.
- Adding a dialect touches the enum, the auto-configuration, a migration folder, a `schema-*.sql`, and
  three test classes.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Auto-detect from `Connection.getMetaData()` | Fragile across driver forks and versions — the stated reason, and the same conclusion Quartz reached |
| Auto-detect with an explicit override | Still ships the fragile path as the default, so the failure mode remains |
| Validate the declared dialect against the metadata at boot | Not currently done. It would turn a mismatch into a clear boot error at the cost of one round trip, and is worth considering |
| One generic SQL dialect for all four | SQL Server has no `SKIP LOCKED` and no `LIMIT`. There is no common subset that performs |
| Depend on Hibernate's `Dialect` | Those interfaces live inside `Dialect`, which only exists after initialising a `SessionFactory`/`ServiceRegistry` — using them in isolation would mean adopting much of the framework |

## Evidence

- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/dialect/JdbcDialect.java` — the interface, the
  Hibernate/Quartz precedent, and the "explicit, never auto-detection" statement.
- `mohs-spring-boot-starter/src/main/java/io/mohs/autoconfigure/MohsAutoConfiguration.java` — the boot
  failure and the H2 warning.
- `dialect/PostgresJdbcDialect.java` and `dialect/SqlServerJdbcDialect.java` — the two that actually
  diverge, each with a static-initialiser guard so a drifted `replace` anchor cannot silently drop the
  inadmissible-job filter.
