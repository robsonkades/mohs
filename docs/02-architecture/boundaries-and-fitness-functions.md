# Boundaries and fitness functions

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

Mohs' architectural rules are not prose. They are **executable**, in three independent mechanisms.
This document is the complete inventory, including the gaps each mechanism admits.

## Mechanism 1 — the reactor itself

The strongest guarantee, because a violation does not compile.

| Boundary | Enforced by |
| --- | --- |
| The public API cannot see the engine | `mohs-api` has no `mohs-engine` dependency |
| The engine cannot see the store | `mohs-engine` has no `mohs-store-jdbc` dependency |
| REST cannot see the engine or the store | `mohs-rest` depends only on `mohs-api` (plus web/Jackson) |
| The cron parser knows nothing of the domain | `mohs-cron` has no dependencies at all beyond JSpecify |
| The test kit is not on the production path | `mohs-test` is a separate artifact, declared `test` scope by its consumers |

## Mechanism 2 — the rules that are no longer executable

**There is no ArchUnit suite.** It lived in `mohs-demo/src/test`, which no longer exists, and with
it went every rule that needed a whole-system classpath to check. This section names them rather
than quietly dropping them, because a convention nobody checks is worth less than one everybody
knows is unchecked:

| Rule | Still enforced by |
| --- | --- |
| Internal packages do not leak into the public API | **The reactor** (Mechanism 1) at module granularity. A leak *within* a module is no longer caught |
| `io.mohs.rest` sees only the public API | **The reactor**. This is why `Mohs` carries read methods that exist for REST alone |
| The engine is free of `java.sql`/`javax.sql` | **The reactor** — `mohs-engine` does not have `mohs-store-jdbc` on its classpath. A raw JDBC type reaching a port signature from elsewhere is not caught |
| The test kit does not reach production code | **The reactor** — `mohs-test` is `test` scope for its consumers |
| Only the starter speaks `spring-boot-autoconfigure` | **Nothing.** Convention |
| The engine never reads the wall clock directly (`Instant.now()`, `System.currentTimeMillis()`) | **Nothing.** Convention — and the one most likely to be broken by accident, because the wrong call compiles and passes every test |
| Ids are UUIDv7, never `UUID.randomUUID()` | **Nothing.** Convention |
| No `synchronized` methods, no `ThreadLocal`, in the engine and the store | **Nothing.** Convention |
| Every production package declares `@NullMarked` | **Nothing.** Convention |
| `io.mohs.core.*` and `io.mohs.rest.*` are free of dependency cycles | **Nothing.** Convention |

Restoring them means a module that sees every other on one classpath. `mohs-demo` was that module,
and it is the only shape the reactor allows.

## Mechanism 3 — source and schema scans

| Guard | Location | What it checks |
| --- | --- | --- |
| `TerminalStateWriteScanTest` | `mohs-store-jdbc/src/test/.../TerminalStateWriteScanTest.java` | Reads its own module's `src/main/java` to guard where terminal-state writes may appear. It lives in the module whose SQL it guards |
| `SqlServerUnicodeScanTest` | `mohs-store-jdbc/src/test` | Guards `NVARCHAR` usage in the SQL Server dialect — `VARCHAR` is not Unicode there by default |
| `KeyGenerationScanTest` | `mohs-store-jdbc/src/test` | Both halves of the UUIDv7 invariant: no `randomUUID` outside the UUIDv7 library in the module's `src/main/java`, and no `IDENTITY`/`SERIAL`/`AUTO_INCREMENT`/`SEQUENCE` in any `.sql` |
| `SchemaPostgresRoundTripTest`, `SchemaMySqlRoundTripTest`, `SchemaSqlServerRoundTripTest` | `mohs-store-jdbc/src/test` | Exercise every store against a real container on the schema an operator would install |
| `SchemaPostgresChainMatchesInstallerTest` | `mohs-store-jdbc/src/test` | Builds one database from `schema-postgresql.sql` and one from the `V*.sql` chain, then compares columns and index definitions. With no migration engine running them, this is the only thing keeping the installer and the upgrade path from drifting apart |
| `JdbcDelegateStatementDriftTest` | `mohs-store-jdbc/src/test` | Compares the named parameters of all 66 statements across the four delegates — the price of spelling every statement out per database |

## Runtime guardrails

Not tests, but the same intent — an invariant that would otherwise be prose becomes an error or a
warning at the moment it is violated:

| Guardrail | Trigger | Effect |
| --- | --- | --- |
| Static initialiser check on the filtered claim SQL | `CLAIM_READY_FILTERED` / `TSQL_READY_CANDIDATES_FILTERED` lose their `:inadmissible` predicate because a `replace` anchor drifted | `ExceptionInInitializerError` at class load — the filter cannot silently disappear |
| Batch-delete driver check | A JDBC driver returns `Statement.SUCCESS_NO_INFO` for the fenced lease delete | `IllegalStateException` naming the problem: without a row count the fence cannot tell winners from losers |
| Missing dialect | `mohs.jdbc.dialect` not set | Boot fails with a message listing the four valid values |
| H2 in use | `mohs.jdbc.dialect=h2` | WARN at boot: H2 is a test/dev backend, not supported in production |
| REST enabled | `mohs.api.enabled=true` | WARN naming the base path and exactly what the unauthenticated API can do |
| Dashboard with no API | `mohs-ui` on the classpath but `mohs.api.enabled=false`, or a non-default `base-path` | WARN — otherwise the page loads, every fetch 404s, and there is no log at all |
| `watchdog-timeout <= node-lease-ttl` | Engine assembly | `IllegalArgumentException` — the bound sits *on top of* node liveness, it is not a shorter lease |
| A job's `timeout >= watchdog-timeout` | Boot, per definition | WARN: the watchdog would release ownership before the job's own deadline |
| `retryPolicy` names a bean that does not exist | Boot, per definition | `IllegalStateException` naming the job and the bean — silently falling back to `retries` would hide the intent |
| `poll-interval > node-lease-ttl / 3` | `Engine#start` | WARN naming the effective capped cadence — liveness wins, but silently would be a tuning mystery |
| Duplicate job id, or a method with two job annotations | Boot | `IllegalStateException` naming both declaring methods. **Always fails**, unconditionally — this is identity, not drift |
| `@OnExecution` with an impossible signature or filter | Boot | `IllegalStateException` — a method that could never be called must not boot |
