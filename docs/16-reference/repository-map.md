# Repository map

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

```text
mohs/
├── pom.xml                      the reactor root: io.mohs:mohs-parent:0.0.1-SNAPSHOT
├── mvnw · mvnw.cmd · .mvn/      the Maven Wrapper
├── LICENSE · NOTICE             Apache 2.0; NOTICE records the vendored cron adaptation
├── CLAUDE.md                    working instructions for AI assistants on this repository
├── README.md                    the entry point
│
├── mohs-cron/                   seconds-first cron parsing, vendored, self-contained
├── mohs-api/                    THE PUBLIC API — 100% contract, module-info.java
├── mohs-engine/                 the engine and its ten ports (nine for persistence, one for the clock)
├── mohs-store-jdbc/             JDBC adapters, dialects, migrations, schema files
├── mohs-rest/                   the operational REST API v1
├── mohs-ui/                     the dashboard — no Java, only the built bundle
├── mohs-test/                   the test kit: MutableClock, InMemoryJobStore
├── mohs-spring-boot-starter/    the composition root
├── mohs-demo/                   a development application — NEVER published
├── mohs-benchmark/              load and chaos harnesses — NEVER published
├── mohs-bom/                    the bill of materials (deliberately parentless)
│
└── docs/                        this documentation
    └── old/                     the previous documentation set, retained for provenance
```

## Where the important things live

| Looking for | Path |
| --- | --- |
| **The public API** | `mohs-api/src/main/java/io/mohs/core/` |
| The facade | `mohs-api/.../io/mohs/core/Mohs.java` |
| What a job is | `mohs-api/.../io/mohs/core/definition/JobDefinition.java` |
| **The engine loop** | `mohs-engine/.../io/mohs/engine/Engine.java` |
| The dispatch pipeline | `mohs-engine/.../io/mohs/engine/Dispatcher.java` |
| The persistence ports | `mohs-engine/.../io/mohs/engine/{JobStore,WorkQueue,LeaseStore,HistoryStore,…}.java` |
| **The claim SQL** | `mohs-store-jdbc/.../io/mohs/store/jdbc/dialect/` |
| **The schema** | `mohs-store-jdbc/src/main/resources/schema-*.sql` |
| **The migrations** | `mohs-store-jdbc/src/main/resources/io/mohs/store/jdbc/migration/<dialect>/` |
| **The properties** | `mohs-spring-boot-starter/.../io/mohs/autoconfigure/MohsProperties.java` |
| **The bean wiring** | `mohs-spring-boot-starter/.../io/mohs/autoconfigure/MohsAutoConfiguration.java` |
| The `@MohsJob` scanner | `mohs-spring-boot-starter/.../io/mohs/autoconfigure/MohsJobScanner.java` |
| **The architecture rules** | `mohs-demo/src/test/java/io/mohs/ArchitectureTest.java` |
| **The chaos and load harnesses** | `mohs-benchmark/src/test/java/io/mohs/store/jdbc/*Scenario.java` |
| The benchmark scripts | `mohs-benchmark/scripts/*.ps1` |
| The dashboard source | `mohs-ui/frontend/src/` |

## Module layout in detail

### `mohs-api` — the published contract

```text
mohs-api/src/main/java/
├── module-info.java                    exports all seven packages
└── io/mohs/core/
    ├── Mohs · MohsLifecycle · EngineState · ScheduleCommand
    ├── Batch · BatchBuilder · ExecutionQuery
    ├── JobSnapshot · NodeSnapshot · RunnerSnapshot
    ├── RateLimitSnapshot · BatchSnapshot · OverviewSnapshot · ThroughputReading
    ├── job/          JobKey · JobRef
    ├── schedule/     Schedule (sealed) · CronSpec · IntervalSpec · OnDemandSpec · Misfire
    ├── definition/   JobDefinition · @MohsJob · @RecurringJob · @OnDemandJob
    │                 JobSpec · PolicySpec · DefinitionSource
    ├── execution/    Execution · Attempt · ExecutionId · ExecutionState
    │                 JobContext · Priority
    ├── event/        ExecutionEvent (sealed, 8 variants) · ExecutionListener
    │                 ExecutionInterceptor · @OnExecution · ExecutionEventType
    └── resource/     MohsRunner · RunnerMode · RateLimit · ExecutionWindow
```

The subpackage dependency graph is **acyclic**, and that is enforced by ArchUnit.

### `mohs-engine` — the loop and the ports

```text
io/mohs/engine/
├── Engine.java                  the poll loop; implements MohsLifecycle (1,768 lines)
├── MohsImpl.java                implements Mohs
├── Dispatcher.java              invokes handlers; maps outcomes
├── CompletionBatcher.java       group commit
├── ScheduleCommandImpl.java     the fluent chain and the enqueue unit
├── RunnerRegistry · MohsExecutors · HandlerRegistry · ExecutionWindowRegistry
├── FiringPlanner · NextFireCalculator · RetrySchedule · Shards   (pure logic)
├── CancellationSignal · EngineMetrics · EngineSettings
├── BatchCompletionCallbacks · BatchCounters · ExecutionEventPublisher
├── StoredJob · StoredNode                                        (read forms)
└── PORTS: JobStore · WorkQueue · LeaseStore · HistoryStore · NodeStore
          BatchStore · RateLimitStore · TriggerFirer · StoreTransactions
          SyncableClock · JobHandler
```

### `mohs-store-jdbc` — the adapters

```text
main/java/io/mohs/store/jdbc/
├── Jdbc{Job,WorkQueue,Lease,History,Node,Batch,RateLimit}Store · JdbcTriggerFirer
├── JdbcStoreTransactions · JdbcSupport · JdbcTimestamps
├── DatabaseClock · MohsFlyway
└── dialect/  JdbcDialect · {H2,Postgres,MySql,SqlServer}JdbcDialect · ClaimedReady

main/resources/
├── schema-{h2,postgresql,mysql,sqlserver}.sql        the hand-install path
└── io/mohs/store/jdbc/migration/<dialect>/V1..Vn     the Flyway path
```

Note: `src/test/java` also contains **`io.mohs.engine`** tests (`EngineTest`, `DispatcherTest`,
`CompletionBatcherTest`, `ScheduleCommandImplTest`) — engine tests that need a real store, living
where the store is. This split package is also why the module carries only an
`Automatic-Module-Name` rather than a `module-info`.

### `mohs-rest` — one subpackage per controller

```text
io/mohs/rest/
├── ApiPaths · ActorResolver · HeaderActorResolver
├── CursorPage · AcceptedExecutionResponse · RuntimePatchResponse · ExecutionLocations
├── error/       the seven domain exceptions + RestExceptionHandler
├── overview/    OverviewController · OverviewStreamBroadcaster · OverviewResponse · ThroughputView
├── job/         JobsController · JobResponse · ScheduleView (sealed) + views · ScheduleJobRequest
├── execution/   ExecutionsController · ExecutionResponse · ExecutionSummaryResponse · AttemptResponse
├── batch/       BatchesController · BatchResponse · BatchState        ← NOT registered as a bean
├── ratelimit/   RateLimitsController · RateLimitResponse · RateLimitPatchRequest
├── runner/      RunnersController · RunnerResponse
└── node/        NodesController · NodeResponse
```

The 1:1 mapping between subpackage and controller is a **navigability rule**, not an accident.

### `mohs-ui` — the dashboard

```text
mohs-ui/
├── pom.xml                       frontend-maven-plugin + resource copy
└── frontend/
    ├── package.json · vite.config.ts · tsconfig*.json
    ├── index.html
    ├── public/                   favicon, logo
    └── src/
        ├── main.tsx · router.tsx · index.css
        ├── pages/                Overview · Jobs · Executions · RateLimits · Runners
        ├── components/           app shell + 20 domain components
        ├── components/ui/        24 shadcn primitives
        ├── lib/                  api.ts, hooks, formatting, live updates
        └── types/api.ts          the wire types, mirroring io.mohs.rest
```

Built output goes to `target/classes/mohs-ui-webapp`, served under `/mohs-ui` — deliberately **not**
`classpath:/static`, which Spring Boot serves at `/`.

### `mohs-benchmark` — the evidence

```text
mohs-benchmark/
├── src/test/java/io/mohs/store/jdbc/
│   ├── ScenarioCluster.java              N engines in one JVM, real PostgreSQL
│   └── *Scenario.java                    nine scenarios, run BY NAME only
└── scripts/
    ├── chaos-recovery.ps1                S6 (kill -9) · S8 (db pause) · SUSPEND (freeze)
    ├── cluster-scale.ps1                 Idle · Drain · Latency, N node-processes
    └── write-amplification.ps1           commits, tuple versions, WAL per execution
```

## Directories that are build output

Present on disk, absent from git: `*/target/`, `mohs-ui/frontend/node/`,
`mohs-ui/frontend/node_modules/`, `mohs-ui/frontend/dist/`.

## `docs/old/`

The previous documentation set, retained for provenance. It contains the earlier design documents,
the historical decision records, the accumulated performance baselines and the previous plans.

**The documentation in `docs/` was written from the source code**, not from those files. Where the two
disagree, the code — and therefore this documentation — is authoritative.
