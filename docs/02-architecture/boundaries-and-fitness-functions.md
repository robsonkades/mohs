# Boundaries and fitness functions

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

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

## Mechanism 2 — ArchUnit

`mohs-demo/src/test/java/io/mohs/ArchitectureTest.java`. `mohs-demo` is the only module that sees
every other module on one classpath, which is what makes a whole-system rule checkable.

| Rule | What it forbids | Notes |
| --- | --- | --- |
| `internal_packages_do_not_leak_into_public_api` | Any non-internal `io.mohs..` class depending on `io.mohs.engine..` or `io.mohs.store.jdbc..` | Lists the **internal** packages (stable) rather than the public ones (growing), so a new public subpackage cannot escape the rule by being forgotten |
| `rest_only_sees_public_api` | `io.mohs.rest..` depending on the engine or the store | This is why `Mohs` carries read methods that exist for REST alone |
| `test_kit_does_not_leak_into_production` | Anything outside `io.mohs.test..` depending on it | |
| `engine_is_free_of_jdbc` | `java.sql..` / `javax.sql..` inside `io.mohs.engine..` | Catches the leak **by type** — a `ResultSet` in a port signature, for instance |
| `only_the_starter_speaks_boot_autoconfigure` | `org.springframework.boot.autoconfigure..` outside `io.mohs.autoconfigure..` | One named exception: `MohsApplication`, the demo bootstrap, which is an application rather than a library |
| `engine_never_reads_wall_clock_directly` | `Instant.now()` and `System.currentTimeMillis()` in the engine and the store | One named exception: `DatabaseClock`. `System.nanoTime()` is deliberately **out of scope** — it is monotonic time, which is what measuring an interval requires |
| `no_synchronized_methods_in_concurrency_critical_code` | The `synchronized` **method modifier** in the engine and the store | |
| `no_thread_local_in_concurrency_critical_code` | `ThreadLocal` / `InheritableThreadLocal` in the engine and the store | |
| `ids_are_generated_as_uuidv7_never_v4` | Any call **or method reference** to `java.util.UUID.randomUUID` | Uses `accessTargetWhere` rather than `callMethod` precisely so `UUID::randomUUID` (a method *reference*) cannot slip through. Matching is by the target's owner, so `io.github.robsonkades.uuidv7.UUIDv7` legitimately returning a `java.util.UUID` does not trip it |
| `all_production_packages_declare_null_marked` | A production package without a `@NullMarked package-info.java` | |
| `core_subpackages_are_free_of_cycles` | A dependency cycle among `io.mohs.core.*` | Does not prescribe edge direction — only that no edge closes a cycle |
| `rest_subpackages_are_free_of_cycles` | A dependency cycle among `io.mohs.rest.*` | |

### Gaps the rules themselves declare

These are recorded in the test's own Javadoc rather than hidden — the honest distance between a
prose rule and an executable one:

| Rule | Gap |
| --- | --- |
| `no_synchronized_methods_in_concurrency_critical_code` | Only the method **modifier** is caught. A `synchronized (lock) { … }` block is not modelled by ArchUnit, which has no instruction-level bytecode inspection in its public API |
| `ids_are_generated_as_uuidv7_never_v4` | Covers Java only. The other half of the invariant — no `IDENTITY`/`SERIAL`/`AUTO_INCREMENT`/`SEQUENCE` in any schema — stays in prose, because ArchUnit does not read SQL |

## Mechanism 3 — source and schema scans

| Guard | Location | What it checks |
| --- | --- | --- |
| `TerminalStateWriteScanTest` | `mohs-store-jdbc/src/test/.../TerminalStateWriteScanTest.java` | Reads its own module's `src/main/java` to guard where terminal-state writes may appear. It lives in the module whose SQL it guards |
| `SqlServerUnicodeScanTest` | `mohs-store-jdbc/src/test` | Guards `NVARCHAR` usage in the SQL Server dialect — `VARCHAR` is not Unicode there by default |
| `SchemaPostgresRoundTripTest`, `SchemaMySqlRoundTripTest`, `SchemaSqlServerRoundTripTest` | `mohs-store-jdbc/src/test` | Assert that the **Flyway migration path** and the **hand-install `schema-*.sql` path** produce the same schema. Two installation routes that silently diverge would be a support nightmare |
| `MohsFlyway*Test` per dialect | `mohs-store-jdbc/src/test` | Structural guardians of the migration chain against a real container |

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
| `retryPolicy` declared | Boot, per definition | WARN: not honoured yet; only `retries` counts |
| `poll-interval > node-lease-ttl / 3` | `Engine#start` | WARN naming the effective capped cadence — liveness wins, but silently would be a tuning mystery |
| Duplicate job id, or a method with two job annotations | Boot | `IllegalStateException` naming both declaring methods. **Always fails**, unconditionally — this is identity, not drift |
| `@OnExecution` present | Boot | `IllegalStateException` — accepting it silently would mean the method never receives an event |

## Boundary violations found

None, beyond what is recorded in [technical debt](../technical-debt.md). Specifically checked:

- No production class outside `io.mohs.autoconfigure` references `io.mohs.engine` or
  `io.mohs.store.jdbc` from a public package.
- No module cycle exists in the reactor.
- Test classes for `io.mohs.engine` living in `mohs-store-jdbc/src/test` and `mohs-test/src/test`
  are a deliberate placement (engine tests that need a real store), not a production leak — no
  production code crosses that boundary.

## Recommended additions

Not present today; listed with the concrete gap each would close.

| Proposed fitness function | Gap it closes |
| --- | --- |
| A SQL scan asserting no `IDENTITY`/`SERIAL`/`AUTO_INCREMENT`/`SEQUENCE` in any `schema-*.sql` or migration | The declared half of the UUIDv7 invariant that ArchUnit cannot see |
| A byte-code or source scan for `synchronized (…) { }` blocks in the engine and the store | The declared gap in the `synchronized` rule |
| A test asserting every controller in `io.mohs.rest` has a registering `@Bean` in `MohsRestAutoConfiguration` | Would have caught [TD-01](../technical-debt.md) — a working, tested controller that is never wired |
| A dependency-convergence or `maven-enforcer` rule | There is no enforcer plugin in the build today |
