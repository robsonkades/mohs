# Testing strategy

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

Mohs tests behaviour at the narrowest useful boundary, then repeats database-sensitive contracts
against the supported engines. A normal reactor `verify` compiles every module, builds the dashboard
and runs the Surefire suites. The ten load and chaos `*Scenario` classes are intentionally excluded
from Surefire's default naming pattern and run explicitly.

## Test layers

| Layer | Main locations | What it proves |
| --- | --- | --- |
| Domain and engine units | `mohs-core/src/test` | State transitions, scheduling, retries, admission, cancellation, listeners and lifecycle |
| JDBC integration | `mohs-store-jdbc/src/test` | Claims, fencing, migrations, retention and dialect-specific SQL against real PostgreSQL, MySQL and SQL Server containers, plus H2 |
| REST contracts | `mohs-rest/src/test` | Endpoint status, pagination, validation, bounded error details and SSE behaviour |
| Spring Boot integration | `mohs-spring-boot-starter/src/test` | Auto-configuration, bean conditions, configuration validation and dashboard resource serving |
| Architecture and source guards | tests alongside their owning modules | Package boundaries and invariants that reflection or compilation alone cannot express |
| Operational scenarios | `mohs-benchmark/src/test` and `mohs-benchmark/scripts` | Multi-node behaviour, throughput, rolling updates, crash recovery and database pauses |

There are currently 99 conventionally named Java test classes and ten explicitly invoked scenario
classes. Treat those counts as a repository snapshot; the test names and build result are the
durable evidence.

## Determinism and time

Engine tests inject a `Clock`, commonly `MutableClock`, so scheduling, leases and misfire behaviour
do not depend on wall-clock sleeps. Concurrency tests use observable state and bounded waits instead
of assuming that a fixed sleep means work has completed. Tests that consume events must register the
listener before triggering the action and wait for the specific event they assert.

Database clock tests account for query round-trip time. Samples whose RTT exceeds the configured
limit are discarded, downward offset corrections are allowed, and the clock still guarantees that
emitted instants never move backwards.

## Database tests

Testcontainers supplies the same database families used in production. A Docker startup failure is
an environment failure only after the container logs confirm it; do not classify an assertion,
migration or SQL error as infrastructure noise.

```bash
./mvnw -pl mohs-store-jdbc -Dtest=JdbcWorkQueueTest test
./mvnw verify
```

The CI workflow pre-pulls all three container images and runs the complete reactor on JDK 25.

## REST and dashboard contracts

Each controller contract uses the shared REST slice and exercises the public wire model. Error
tests assert status and stable public detail, including cyclic and overly deep cause chains. Starter
integration tests prove that production auto-configuration registers controllers and serves the
dashboard, immutable assets, missing resources and SPA fallback.

The frontend build runs TypeScript checking and the Vite production build through Maven. There is
currently no browser automation suite, so interaction changes also require a manual demo pass.

## Coverage and reports

Surefire XML reports are written below each module's `target/surefire-reports` directory and uploaded
by CI even on failure. JaCoCo writes a report per Java module under `target/site/jacoco`. Coverage is
not aggregated and no percentage threshold is enforced.

## Operational scenarios

Run scenario classes by name:

```bash
./mvnw -pl mohs-benchmark -Dtest=NodeChurnScenario test
```

Use the PowerShell harnesses for process kills and suspensions. See
[benchmarks and harnesses](../10-performance/benchmarks.md) for prerequisites and interpretation
limits.

## Adding a test

Place the test in the module that owns the contract. Prefer public outcomes over private method
shape, inject time, use unique keys for shared databases, and include the failure path when it is
part of the contract. Add a cross-dialect case when SQL syntax, locking, transaction isolation or
type mapping can change the result.
