# Testing strategy

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

## The inventory

| Module | Files under `src/test` | Character |
| --- | --- | --- |
| `mohs-api` | 17 | Pure unit tests over record invariants, validation, sealed-type exhaustiveness |
| `mohs-cron` | 1 | The vendored parser's behaviour, including the Quartz extensions |
| `mohs-engine` | 13 | Pure logic — shards, retry, firing plan, metrics, cancellation, executors, registries |
| `mohs-store-jdbc` | 31 | Integration against real databases via Testcontainers, source-scan guards, plus four engine tests and three `*TestSupport` helpers |
| `mohs-rest` | 16 | Contract tests over a mocked `Mohs`, plus the shared slice configuration |
| `mohs-test` | 3 | The kit's own behaviour |
| `mohs-spring-boot-starter` | 9 | Auto-configuration slices, the scanner, and one shutdown harness |
| `mohs-demo` | 4 | ArchUnit, context load, end-to-end UI and runners |
| `mohs-benchmark` | 10 | Nine `*Scenario` classes plus `ScenarioCluster` — **run by name only** |
| **Total** | **104 files** | |

Counted precisely: **98 of those files contain at least one `@Test`**, totalling **711 `@Test`
methods**. Nine of the 98 are `*Scenario` classes that Surefire's default include pattern does not
match, so **89 classes run in the normal suite**. The remaining six files are support code
(`ScenarioCluster`, three `*TestSupport` classes, `RestSliceConfiguration`,
`ShutdownWithOpenStreamsHarness`).

**No coverage report is produced by the build.** There is no JaCoCo, no Cobertura, no threshold. Any
coverage figure quoted anywhere would be unsubstantiated, so none is quoted here.

## The test pyramid, as it actually exists

```
                    /\
                   /  \        4   end-to-end   (mohs-demo: ArchUnit, context, UI, runners)
                  /----\
                 /      \      9   scenarios    (mohs-benchmark, by name only)
                /--------\
               /          \   28   integration  (mohs-store-jdbc, Testcontainers + scans)
              /------------\
             /              \ 15   contract     (mohs-rest, @WebMvcTest-style)
            /----------------\
           /                  \ 42 unit + wiring (api, cron, engine, test kit, starter)
          /--------------------\
```

Plus a layer that is not a level at all: **executable architecture rules** (ArchUnit, source scans,
schema round-trips).

## What is tested at each level

### Unit — pure logic, no I/O

`mohs-api`, `mohs-cron`, `mohs-engine`, `mohs-test`.

| Target | Examples |
| --- | --- |
| Record invariants | Every compact constructor's rejection path |
| Sealed-type exhaustiveness | `ExecutionEventTest`, `ScheduleViewJsonTest` |
| Pure algorithms | `ShardsTest` (**pins the hash's output to literal values** — the stability is contract), `RetryScheduleTest`, `FiringPlannerTest`, `NextFireCalculatorTest`, `CronExpressionTest` |
| Concurrency helpers | `CancellationSignalTest`, `MohsExecutorsTest` |
| Registries | `HandlerRegistryTest`, `RunnerRegistryTest`, `ExecutionWindowRegistryTest` |
| Metrics | `EngineMetricsTest` — labels are contract |
| Sleep arithmetic | `EngineSleepTest` — the backoff, the trigger cap, the liveness cap |

`ShardsTest` deserves the call-out: it asserts **literal** shard values for known ids, because a
shard written by one JVM must be re-derivable by any other, forever. Changing the hash would be a
data migration, not a refactor, and this test is what makes that visible.

### Integration — against real databases

`mohs-store-jdbc`, 28 classes. **Requires Docker.**

| Group | Coverage |
| --- | --- |
| Per-port stores | `JdbcJobStoreTest`, `JdbcWorkQueueTest`, `JdbcLeaseStoreTest`, `JdbcHistoryStoreTest`, `JdbcBatchStoreTest`, `JdbcRateLimitStoreTest`, `JdbcNodeStoreTest`, `JdbcTriggerFirerTest` |
| Per-dialect | `JdbcWorkQueuePostgresTest`, `…MySqlTest`, `…SqlServerTest`; `JdbcLeaseStorePostgresTest`; `ScheduleCommandPostgresTest` |
| Schema equivalence | `SchemaPostgresChainMatchesInstallerTest` — one database built from `schema-postgresql.sql`, one from the `V*.sql` chain, compared column by column and index by index |
| Stores against a real database | `SchemaPostgresRoundTripTest`, `…MySql…`, `…SqlServer…` |
| Statement drift | `JdbcDelegateStatementDriftTest` — the named parameters of all 66 statements, across the four delegates |
| Source scans | `TerminalStateWriteScanTest`, `SqlServerUnicodeScanTest` |
| Time | `JdbcTimestampsTest`, `DatabaseClockTest` |
| **Engine tests needing a real store** | `EngineTest`, `DispatcherTest`, `CompletionBatcherTest`, `ScheduleCommandImplTest` — in `io.mohs.engine`, but living here |

That last row is deliberate: they are engine tests that need a real store, and they live where the
store is. The production code does not move.

**Without Docker** these fail with `Could not initialize class *TestSupport`. That is an environment
failure, not a regression.

### Contract — the REST layer

`mohs-rest`, 15 classes, over a **mocked `Mohs`**. They assert the wire contract: status codes,
`Location` headers, JSON shape, RFC 7807 bodies, pagination behaviour, and the validation messages.

`RestSliceConfiguration` is the shared slice; each controller has its own `*ContractTest`.

Note that a contract test constructs its controller with an explicit `@Bean`, so it cannot notice a
missing *production* registration — a controller can be fully tested and never served. That is why
`MohsRestAutoConfigurationTest#everyRestControllerIsRegistered` exists: it asserts the registration
itself, which no contract test can.

### Auto-configuration — the wiring

`mohs-spring-boot-starter`, 8 classes.

| Test | Asserts |
| --- | --- |
| `MohsAutoConfigurationTest` | Bean graph, conditionals, the `defaultCandidate = false` isolation |
| `MohsJobScannerTest` | The two-phase scan, duplicate detection, drift reconciliation, orphaning |
| `MohsJobsTest` | Annotation-to-definition translation and handler adaptation |
| `MohsRunnersTest`, `MohsRateLimitsTest` | Assembly, defaults, duplicate and wrong-mode rejection |
| `MohsRestAutoConfigurationTest` | The property gate and the conditionals |
| `MohsUiAutoConfigurationTest` | Resource handling and the SPA fallback |
| `MohsOverviewStreamLifecycleTest` | Shutdown ordering |
| `ShutdownWithOpenStreamsHarness` | The interaction between an open SSE stream and graceful shutdown |

### End-to-end

**There is none, and this is the largest hole in the suite.** `mohs-demo` carries no tests: nothing
loads the full auto-configuration the way a real consumer does, so a wiring defect that every unit
test passes reaches the first application that tries the starter. The auto-configuration tests in
`mohs-spring-boot-starter` cover the beans in isolation, not the assembled application.

### Architecture — no longer executable

The ArchUnit suite lived in `mohs-demo` and went with it. What each rule now rests on — the reactor
for the module boundaries, convention for the rest — is listed in
[boundaries and fitness functions](../02-architecture/boundaries-and-fitness-functions.md).

### Load and chaos

`mohs-benchmark`, nine `*Scenario` classes plus three scripts. **Not run by the normal suite** —
Surefire's default pattern does not match `*Scenario`. See [benchmarks](../10-performance/benchmarks.md).

## Testing principles the codebase applies

| Principle | Evidence |
| --- | --- |
| **No `Thread.sleep` for synchronisation** | Latches, `CompletableFuture` with a timeout, and `MutableClock`. The one sleep in production-adjacent code is `Demo.slowMethod`, and its Javadoc says explicitly it is *"a deliberate bench wait, not synchronisation"* |
| **Time is injected, always** | `MutableClock` makes scheduling and misfire deterministic. Every "when" in the engine comes from a `Clock`, enforced by ArchUnit — which is what makes this possible at all |
| **Real databases for anything touching SQL** | Testcontainers for PostgreSQL, MySQL and SQL Server. H2 covers the fast path |
| **A test seam rather than a mock, where a seam is honest** | `RunnerRegistry` has a package-private constructor taking a factory, existing *only* to prove failure paths that are unreachable with the real builders. `InMemoryJobStore` is the same idea at port level |
| **Assertions must be falsifiable** | `BatchCompletionScenario` uses `retries(1)` precisely so that "counted the terminal failure" and "counted every attempt" produce **different** numbers |
| **Limitations are declared, not hidden** | Every scenario has a "declared limitation" paragraph; every ArchUnit rule with a gap says what the gap is |
| **Guard the invariant where the invariant lives** | `TerminalStateWriteScanTest` reads its own module's source; the schema round-trip tests compare the two installation paths |

## What is **not** covered

Stated honestly, from reading the tree:

| Gap | Impact |
| --- | --- |
| **No coverage measurement at all** | No threshold, no report, no trend. Coverage claims cannot be substantiated |
| **No mutation testing** | Nothing verifies the assertions would fail if the code were wrong, except where a scenario made falsifiability explicit |
| **Process death in-JVM** | `ScenarioCluster` cannot express `kill -9`; that lives only in a PowerShell script that is not part of any automated run |
| **No CI** | Every test in this repository is run manually. Nothing gates a commit |
| **No performance regression gate** | Benchmarks are run by hand, and the results live in prose |
| **No contract test asserting every controller is registered** | Which is why a working, tested controller is unwired in production |
| **No SQL-level scan for forbidden key strategies** | The UUIDv7 invariant is only half enforced |
| **Cross-dialect coverage is uneven** | `JdbcWorkQueue` has per-dialect tests; most other stores are tested against one backend |
| **The dashboard has no tests** | No Vitest, no Playwright, no `npm test` script — `package.json` defines only `dev`, `build`, `typecheck` and `preview` |
| **`@OnExecution` and `retryPolicy`** | Tested only for their *rejection*/warning behaviour, since neither is implemented |

## Running the suite

```bash
./mvnw clean verify                       # everything, including the frontend build
./mvnw test                               # tests only
./mvnw verify -Dskip.frontend=true        # backend only, no Node
./mvnw test -pl mohs-engine               # one module
./mvnw test -pl mohs-engine -Dtest=ShardsTest
./mvnw -pl mohs-benchmark test -Dtest=NodeChurnScenario   # a scenario, by name
```

| Note | Detail |
| --- | --- |
| **Docker is required** for `mohs-store-jdbc` and `mohs-benchmark` | Without it, container-backed tests error on `Could not initialize class *TestSupport` |
| `-Dskip.frontend=true` skips Node entirely | Useful when a `npm run dev` is holding esbuild binaries. **Never build a published jar with it** — `mohs-ui` would ship empty |
| A known flake | The SQL Server container occasionally fails to start in a full reactor `verify`, and passes when `mohs-store-jdbc` is built in isolation |

## Testing your own jobs

```java
class InvoiceJobTest {

    private final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryJobStore jobs = new InMemoryJobStore(clock);

    @Test void the_definition_is_registered_with_the_declared_policy() {
        jobs.upsert(JobDefinition.of("nightly", Invoices.class,
                spec -> spec.cron("0 0 3 * * *", ZoneId.of("UTC")).retries(3)));

        StoredJob stored = jobs.find(JobKey.of("nightly")).orElseThrow();
        assertThat(stored.definition().retries()).isEqualTo(3);
        assertThat(stored.nextFireAt()).isEqualTo(Instant.parse("2026-01-01T03:00:00Z"));
    }
}
```

Recommendations for consumers:

| Level | Approach |
| --- | --- |
| Handler logic | Call the method directly with a payload and a fake `JobContext`. It is an ordinary Spring bean method |
| Cancellation behaviour | Supply a `JobContext` whose `cancellationRequested()` flips, and assert the handler stops |
| Definition and schedule | `InMemoryJobStore` + `MutableClock` |
| Full integration | `@SpringBootTest` with `mohs.jdbc.dialect=h2` |
| **Do not** | Implement `Mohs`, `ScheduleCommand` or `JobContext` yourself — they may gain methods in minor releases |

## Recommended additions

| Recommendation | Gap it closes |
| --- | --- |
| JaCoCo with a per-module threshold | Coverage is unmeasurable today |
| A SQL scan for `IDENTITY`/`SERIAL`/`AUTO_INCREMENT`/`SEQUENCE` | The declared half of the UUIDv7 invariant |
| Frontend tests (Vitest for `lib/`, Playwright for the pages) | The dashboard is entirely untested |
| A nightly job running the chaos scripts | They are the strongest correctness evidence in the project and nothing runs them automatically |
