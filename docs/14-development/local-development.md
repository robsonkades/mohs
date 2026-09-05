# Local development

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## Prerequisites

| Requirement | Version | Needed for |
| --- | --- | --- |
| JDK | **25** | Everything. `maven.compiler.release=25` |
| Maven | The wrapper is included (`./mvnw`) | Everything |
| **Docker** | Any recent version | `mohs-store-jdbc` and `mohs-benchmark` — Testcontainers starts PostgreSQL, MySQL and SQL Server |
| Node.js | Installed **automatically** by the build (v22.12.0, pinned) | Only if you develop the dashboard by hand |
| `pwsh` | PowerShell 7+ | The benchmark and chaos scripts. **Windows PowerShell 5.1 will not do** |

Nothing else. No local database is required — H2 runs embedded, and Testcontainers manages the rest.

## Clone and build

```bash
git clone https://github.com/robsonkades/mohs.git mohs
cd mohs

./mvnw clean verify                     # everything, including the frontend
./mvnw clean verify -Dskip.frontend=true  # backend only, no Node
```

The first build downloads Node and runs `npm ci`, so it is slow. Later builds are much faster.

> **Never build a published jar with `-Dskip.frontend=true`.** `mohs-ui` would ship empty.

## Running the demo application

`mohs-demo` is a Spring Boot application whose only purpose is development. It runs on H2 by default
and defines three sample jobs.

Build and install reactor dependencies once, then run the goal only in the application module:

```bash
./mvnw install -pl mohs-demo -am -DskipTests
./mvnw -pl mohs-demo spring-boot:run -Dspring-boot.run.arguments="--spring.datasource.hikari.connection-timeout=3000"
```

The install command packages the demo and dashboard; `-DskipTests` is for this local startup
step, not test validation. Use `./mvnw.cmd` in PowerShell. The commands below use Bash line
continuations; in PowerShell use one line and quote each `-D...` argument as a whole.

Its defaults come from `SpringApplication#setDefaultProperties`, not from an `application.yaml` — a
library must not ship one, and `defaultProperties` loses to any external source.

To turn on the API and the dashboard:

```bash
./mvnw -pl mohs-demo spring-boot:run \
  -Dspring-boot.run.arguments="--mohs.api.enabled=true --spring.datasource.hikari.connection-timeout=3000"
```

Then:

| URL | What |
| --- | --- |
| `http://localhost:8080/mohs-ui` | The dashboard |
| `http://localhost:8080/api/mohs/v1/overview` | The polling anchor |
| `http://localhost:8080/api/mohs/v1/jobs` | The registered jobs |

### Running against PostgreSQL locally

From the repository root, using a new local database:

```bash
docker run -d --name mohs-postgres -p 127.0.0.1:5432:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=mohs postgres:16-alpine
docker exec mohs-postgres pg_isready -U postgres -d mohs
```

Wait for `pg_isready` to report that connections are accepted, then apply the installer:

```bash
docker cp mohs-store-jdbc/src/main/resources/schema-postgresql.sql mohs-postgres:/tmp/schema-postgresql.sql
docker exec mohs-postgres psql -v ON_ERROR_STOP=1 -U postgres -d mohs -f /tmp/schema-postgresql.sql
./mvnw -pl mohs-demo spring-boot:run -Dspring-boot.run.arguments="--mohs.jdbc.dialect=postgresql --spring.datasource.url=jdbc:postgresql://localhost:5432/mohs --spring.datasource.username=postgres --spring.datasource.password=postgres --spring.datasource.hikari.connection-timeout=3000 --spring.sql.init.mode=never --mohs.api.enabled=true"
```

These credentials are for the disposable local example. `spring.sql.init.mode=never`
overrides the demo's H2 initializer: changing only the JDBC URL and dialect would otherwise
apply `schema-h2.sql` to PostgreSQL. Use the [migration guide](../06-data/migrations.md)
when upgrading an existing database.

## The dashboard development loop

```bash
cd mohs-ui/frontend
npm ci
npm run dev            # Vite on :5173, proxying /api/mohs to localhost:8080
```

Start the demo application (with `mohs.api.enabled=true`) in another terminal. Vite's proxy forwards
SSE **without buffering**, so a dashboard under `npm run dev` receives `/overview/stream` frames
exactly as it would when served from the jar.

| Script | Does |
| --- | --- |
| `npm run dev` | Vite dev server with HMR |
| `npm run build` | `tsc -b && vite build` |
| `npm run typecheck` | `tsc -b --noEmit` |
| `npm run preview` | Serve the built bundle |

There is **no `npm test` and no linter** — see [testing strategy](../11-testing/testing-strategy.md).

> **On Windows, `npm ci` can fail with `EPERM` while `npm run dev` is running** because esbuild/rollup binaries are
> held open. Use `./mvnw verify -Dskip.frontend=true` for backend work — that is exactly why the flag
> exists.

## Running tests

```bash
./mvnw test                                        # the whole suite
./mvnw test -pl mohs-engine                        # one module
./mvnw test -pl mohs-engine -Dtest=ShardsTest      # one class
./mvnw test -pl mohs-engine -Dtest=ShardsTest#hashIsPinnedAcrossVersions
./mvnw verify -pl mohs-store-jdbc -am              # a module plus its dependencies
./mvnw verify -rf :mohs-rest                       # resume the reactor from a module
```

| Note | Detail |
| --- | --- |
| **Docker must be running** for `mohs-store-jdbc` | Otherwise: `Could not initialize class *TestSupport` — environment, not regression. Without Docker, skip them explicitly: `./mvnw test -DexcludedGroups=docker` (every container-backed class is `@Tag("docker")`; the H2 tests and the source scans still run, and a red suite then means a regression) |
| Container startup failure | Inspect Docker resources and container logs before classifying the failure; rerun the affected module after resolving the cause |
| Benchmarks are excluded by default | Surefire's default pattern does not match `*Scenario`. Run them by name |

```bash
./mvnw -pl mohs-benchmark test -Dtest=NodeChurnScenario
pwsh mohs-benchmark/scripts/chaos-recovery.ps1 -Scenario S6
```

## Debugging

```bash
./mvnw -pl mohs-demo spring-boot:run \
  -Dspring-boot.run.arguments="--spring.datasource.hikari.connection-timeout=3000" \
  -Dspring-boot.run.jvmArguments="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5005"
```

### Useful breakpoints

| Where | Why |
| --- | --- |
| `Engine#tick` | The whole cycle |
| `Engine#claimLaps` | The claim lap, the budget, the shard cursor |
| `Engine#admit` | Why a claimed execution was sent back |
| `Dispatcher#dispatch` | The handler invocation and the outcome mapping |
| `LeaseStore#complete` (i.e. `JdbcLeaseStore`) | The fence's verdict |
| `MohsJobScanner#scanJob` | Why a job was or was not registered |
| `FiringPlanner#plan` | What a due trigger will fire |

### Making the loop slow enough to follow

```
--mohs.engine.poll-interval=5s
--mohs.engine.max-poll-interval=5s   # disables the backoff — a fixed cadence
```

Setting them equal is the documented way to turn the adaptive backoff off, and it is what the
deterministic tests do.

### Diagnosing threads

```bash
jcmd <pid> Thread.print | grep -A5 mohs-
jcmd <pid> Thread.dump_to_file -format=json /tmp/threads.json
```

Every Mohs thread is named for exactly this. See
[health and diagnostics](../09-observability/health-and-diagnostics.md#thread-dump) for what to
expect.

**Note**: `-Djdk.tracePinnedThreads` was removed in JDK 24 and is a **silent no-op** on JDK 25. Use
JFR's `jdk.VirtualThreadPinned` event.

### Watching the database

The following queries use PostgreSQL syntax.

```sql
SELECT COUNT(*) FROM mohs_ready WHERE visible_at <= now();
SELECT node_id, COUNT(*) FROM mohs_lease GROUP BY node_id;
SELECT job_key, next_fire_at, paused FROM mohs_job_definitions;
SELECT execution_id, state, scheduled_at FROM mohs_execution ORDER BY created_at DESC LIMIT 20;
```

The demo includes `spring-boot-h2console`. To enable the console locally, set
`spring.h2.console.enabled=true`; the dependency alone does not enable it.

## Common tasks

### Add a job to the demo

```java
// mohs-demo/src/main/java/io/mohs/demo/Demo.java
@RecurringJob(id = "my-job", every = "PT10S", retries = 2)
void myJob() {
    log.info("hello");
}
```

Restart. The scanner registers it and the upsert arms its trigger.

### Add a REST endpoint

1. Put the controller in the matching `io.mohs.rest.<area>` subpackage — the 1:1 mapping is a
   navigability rule.
2. Use `${mohs.api.base-path:" + ApiPaths.V1 + "}` in `@RequestMapping`; an annotation cannot read a
   property binding, so the placeholder is the only mechanism there.
3. **Register a `@Bean` in `MohsRestAutoConfiguration`** — a controller with no bean is never served,
   and a contract test will not notice. `MohsRestAutoConfigurationTest#everyRestControllerInThePackageIsRegisteredWhenTheApiIsOn`
   fails the build if you forget.
4. Write a `*ContractTest` over a mocked `Mohs`.
5. Remember the boundary: `io.mohs.rest` may not see the engine or the store. If you need new data,
   add a read method to the `Mohs` facade.

### Add a configuration property

1. Add a component to the relevant record in `MohsProperties`, with a `@param` doc — that is what the
   configuration processor reads.
2. Consume it in `MohsAutoConfiguration`.
3. Validate it in `EngineSettings` if it is an engine parameter, with a message that names the
   property and the value.
4. Cover it in `MohsAutoConfigurationTest`.
5. Update [the configuration reference](../07-configuration/configuration-reference.md).

### Change the schema

1. Add `V<n>__description.sql` for **every** applicable dialect, under
   `io/mohs/store/jdbc/migration/<dialect>/`.
2. Make it **idempotent**; guard by *shape* where a name would be ambiguous.
3. Update the matching `schema-<dialect>.sql` — the round-trip tests compare the two paths.
4. **Never edit a published migration.** Nothing validates checksums any more, so an edited delta does not fail a boot — it silently leaves databases with different schemas depending on when they were installed.
5. State the operational cost in a header comment if it moves rows or takes a table-level lock.
6. Extend `SchemaPostgresChainMatchesInstallerTest`, the structural guardian that compares the installer against the delta chain.

### Add a dialect

See [extensibility](../04-engineering/extensibility.md#how-to-add-a-new-database-dialect).

## The end-of-change checklist

Derived from the practices visible throughout the tree:

- [ ] Did you keep the boundary you touched? Nothing in the build verifies most of them since the ArchUnit suite went away — see [boundaries and fitness functions](../02-architecture/boundaries-and-fitness-functions.md)
- [ ] Does every new production package have a `@NullMarked package-info.java`?
- [ ] Is every "when" from an injected `Clock` and every duration from `System.nanoTime()`?
- [ ] Are new value objects records with validation in the compact constructor?
- [ ] Does each new error message name the field, the value, and ideally the fix?
- [ ] If it is a performance change: **is there a before/after number?**
- [ ] If you added a `catch`: does it handle, log with the cause, or rethrow?
- [ ] If you added an executor or a queue: does it have a ceiling and a rejection policy?
- [ ] If you added a wait: does it have a deadline?
- [ ] Did you record *why*, not *what*, in the comment?

## IDE notes

| Item | Note |
| --- | --- |
| Java level | Set the project SDK to 25 |
| `-parameters` | Configured in the POM; make sure the IDE honours it, or Spring MVC parameter names break |
| Lombok | **Not used** |
| Annotation processors | Only `spring-boot-configuration-processor`, in the starter |
| Nullability | JSpecify. IntelliJ understands `@NullMarked`/`@Nullable`; configure it to treat them as the nullity annotations |
| Formatter | **None is configured.** Match the surrounding code |
