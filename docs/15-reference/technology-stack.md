# Technology stack

Status: Active · Last Reviewed: 2026-09-04 · Source of Truth: Repository (`pom.xml`, `package.json`)

Versions are taken from the build files. Where a version is managed by the Spring Boot BOM rather than
pinned explicitly, that is stated instead of guessing a number.

## Language and platform

| Technology | Version | Purpose | Where |
| --- | --- | --- | --- |
| Java | **25** | The whole backend | `maven.compiler.release=25` |
| Maven | Wrapper included | Build | `./mvnw` |
| TypeScript | ~7.0.2 | The dashboard | `mohs-ui/frontend` |
| Node.js | v22.12.0, pinned | Building the dashboard | `frontend-maven-plugin` |

Java 25 features actually used: records, sealed interfaces, pattern matching in `switch`, record
deconstruction patterns, unnamed variables (`_`), text blocks, virtual threads.

**Preview features are deliberately not used.** A class file compiled with `--enable-preview` locks
the host application to that exact JDK — unacceptable for an embedded library. That is why
`StructuredTaskScope` (JEP 505, preview on JDK 25) is not used, and why the one structural fan-out in
the tree is written in its shape with the stable API.

## Frameworks and libraries

| Technology | Version | Purpose | Where used |
| --- | --- | --- | --- |
| Spring Boot | **4.1.1** | Auto-configuration, lifecycle, the test slice | Imported as a BOM, not inherited as a parent |
| Spring Framework | BOM-managed | `spring-core` (annotations), `spring-context`, `spring-tx`, `spring-jdbc` | Across the reactor |
| Spring Web MVC | BOM-managed, **`<optional>`** | The REST API and the dashboard's static handler | `mohs-rest`, the starter |
| Jackson | BOM-managed (Jackson 3 — `tools.jackson.databind`) | Payload serialisation, REST bodies | `mohs-store-jdbc`, `mohs-rest` |
| Micrometer | BOM-managed (`micrometer-core`) | The `mohs.*` metrics | `mohs-engine` |
| SLF4J | BOM-managed | Logging façade | Everywhere |
| JSpecify | BOM-managed | Nullness annotations | Every module |
| `io.github.robsonkades:uuidv7` | **1.1.0** | UUIDv7 generation | `mohs-engine`, `mohs-store-jdbc` |
| HikariCP | BOM-managed, supplied by the host | Connection pooling | The host's `DataSource` |

**Not present anywhere**: any message-broker client, HTTP client, cloud SDK, cache client, ORM, or
reactive library. Verified against every module POM.

## Databases

| Database | Tier | Driver | Notes |
| --- | --- | --- | --- |
| PostgreSQL | 1 — production | `org.postgresql:postgresql` (BOM-managed) | Single-statement claim; `TIMESTAMPTZ` split tables; the only dialect with `V5` |
| MySQL | 2 — production, 8.0+ | `com.mysql:mysql-connector-j` (BOM-managed) | Native `SKIP LOCKED`; `DATETIME(6)`; explicit `READ COMMITTED` matters most here |
| SQL Server | 2 — production | `com.microsoft.sqlserver:mssql-jdbc` (BOM-managed) | `TOP` + table hints instead of `SKIP LOCKED`; `NVARCHAR`; `V8` |
| H2 | **3 — test/dev only** | `com.h2database:h2` (BOM-managed) | WARNs at boot; ~33% double-lock race in `SKIP LOCKED` |

Mohs declares the drivers at **test/runtime scope only** — the host supplies its own.

## Testing

| Technology | Version | Purpose |
| --- | --- | --- |
| JUnit 5 | BOM-managed | The whole suite |
| AssertJ | BOM-managed | Assertions |
| Mockito | BOM-managed | The REST contract tests |
| Testcontainers | BOM-managed (`junit-jupiter`, `postgresql`, `mssqlserver`, `mysql`) | Real databases |
| Spring Boot Test | BOM-managed | Context slices |

**Not present**: JMH, mutation testing, any frontend test framework. JaCoCo (0.8.14) produces a
per-module report under `target/site/jacoco`; there is no aggregation across modules and no
threshold, so no single coverage figure is asserted.

## Frontend

| Technology | Version | Purpose |
| --- | --- | --- |
| React | ^19.2.8 | The dashboard |
| TanStack Router | ^1.170.21 | Routing, with per-route code splitting |
| TanStack Query | ^5.62.0 | Server state |
| TanStack Table | ^9.1.2 | Tables |
| Recharts | ^3.8.0 | Charts |
| Tailwind CSS | ^4.3.3 | Styling, via the Vite plugin |
| Radix UI + shadcn | ^1.6.7 / ^4.16.2 | Component primitives |
| lucide-react | ^1.31.0 | Icons |
| cmdk | ^1.1.1 | The command palette |
| react-day-picker | ^10.0.1 | Date filtering |
| Vite | ^8.2.1 | Build and dev server |
| Fontsource Geist / JetBrains Mono | ^5.3.0 | Fonts, self-hosted |

**Scripts defined**: `dev`, `build` (`tsc -b && vite build`), `typecheck`, `preview`. **No `test` and
no `lint`.**

## Build plugins

| Plugin | Version | Purpose |
| --- | --- | --- |
| `maven-compiler-plugin` | 3.15.0 | Release 25, `-parameters` |
| `maven-surefire-plugin` | 3.5.2 | Tests; its default include pattern keeps `*Scenario` out |
| `maven-jar-plugin` | **3.4.2** | `Automatic-Module-Name`. Pinned so a published jar does not vary by build machine |
| `spring-boot-maven-plugin` | 4.1.1 | Repackage, `mohs-demo` only |
| `frontend-maven-plugin` | 1.15.1 | Node, `npm ci`, `npm run build` |
| `maven-resources-plugin` | 3.3.1 | Copies the bundle onto the classpath |
| `spring-boot-configuration-processor` | BOM-managed | IDE metadata for `mohs.*` |

## Licensing

| Item | Value |
| --- | --- |
| Project licence | Apache 2.0, declared in the POM and packaged into every jar's `META-INF` |
| Vendored code | `io.mohs.cron`, adapted from `org.springframework.scheduling.support` (Apache 2.0) |
| Obligation | `NOTICE` records the adaptation and its one functional divergence, in `QuartzCronField#nextOrSame` |

## Technology choices worth noting

| Choice | Rationale |
| --- | --- |
| **No ORM** | The claim query's exact shape *is* the performance story, and an ORM would hide it |
| **No message broker** | Persistence is the host's database; adding a broker would contradict the embedded premise |
| **No reactive stack** | Virtual threads give thread-per-task concurrency with blocking JDBC, which is what a scheduler over a database actually needs |
| **Jackson 3** (`tools.jackson`) | Current with Spring Boot 4. A databind failure now propagates `DatabindException` directly rather than wrapped in `IllegalArgumentException`, which the REST layer handles explicitly |
| **A raw `JsonMapper` for payloads**, not the context `ObjectMapper` | The persisted format belongs to Mohs; using the host's HTTP configuration would let it define a durable format shared between nodes and break already-written payloads the day it changed. The REST layer converts request bodies with the same mapper, for the same reason from the other side |
| **Micrometer, always on** | With no registry in the host, a local `SimpleMeterRegistry` keeps the engine identical — no conditional path in hot code |
| **JSpecify over Spring's or JetBrains' annotations** | The emerging standard, and already available transitively |
| **No migration engine at all** | The operator installs the schema; an embedded library does not run DDL against a database it does not own |

## Absent tooling

Listed so nobody looks for it:

| Absent | Consequence |
| --- | --- |
| CI (any provider) | Nothing gates a commit |
| Code coverage | No measurement, no threshold |
| Static analysis (Checkstyle, Spotless, PMD, SpotBugs, ErrorProne) | Formatting and style are consistent by convention |
| Dependency vulnerability scanning | No automated CVE surfacing |
| `maven-enforcer-plugin` | No dependency-convergence check |
| OpenAPI generation | The REST contract is documented in prose, here |
| Distributed tracing | `ExecutionInterceptor` is the integration point |
| A release profile | See [modules and publishing](../12-build/modules.md#release-process) |
