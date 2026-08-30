# DR-001: Mohs is an embedded library, not a standalone server

## Status

Accepted

## Context

A job scheduler can be delivered in two shapes: a service you deploy and talk to, or a library you
embed in the application whose jobs it runs. The choice determines nearly everything else — the
security model, the deployment story, the transactional guarantees available to a handler, and
whether "schedule this job as part of my business transaction" is even expressible.

## Decision

Mohs is a **library**. It has no process, no port and no image of its own. An application declares
`io.mohs:mohs-spring-boot-starter`, points a `DataSource` at a relational database, and gets durable,
clustered job execution inside its own JVM.

Three consequences were accepted deliberately at the same time:

1. Persistence is the **host application's database**, with a `mohs_` prefix on every table.
2. The REST API and the dashboard are served on the **host's own web server**, never on a port of
   Mohs' own.
3. Mohs ships **no `application.yaml`** in any published jar.

## Consequences

### Positive

- **The enqueue composes with the host's transaction.** `StoreTransactions` uses `PROPAGATION_NESTED`,
  so `mohs.schedule(...)` inside a `@Transactional` method joins that transaction as a savepoint.
  That is the transactional-outbox pattern available for free, and it is impossible with a separate
  service.
- **The host's security applies.** Since the API is served inside the host's filter chain, an
  application that protects itself protects Mohs too.
- **No new operational surface.** No image to build, no service to monitor, no additional network hop.
- **The handler is an ordinary Spring bean method.** No serialisation boundary, no RPC.

### Negative

- **Mohs shares the host's resources** — the connection pool, the CPU, the heap. A misconfigured
  `dispatch-concurrency` starves the application it lives in.
- **There is no isolation between the scheduler and the application.** A handler that leaks memory
  takes the scheduler with it.
- **Scaling is coupled to the application's replica count**, up to the 64-shard ceiling.
- **The library must be extraordinarily careful not to disturb its host** — which is why every bean
  of a generic framework type is `defaultCandidate = false`, why the exception advice is scoped to
  `io.mohs.rest`, and why the dashboard is served from a dedicated classpath location rather than one
  Spring Boot serves at `/`.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| A standalone scheduler service | Loses transactional composition with the host entirely, and requires an RPC boundary for every handler invocation |
| An embedded library **with its own web server** for the API | Explicitly rejected in `MohsUiAutoConfiguration`: a server of ours would sit outside the host's Spring Security filter chain, and an application that protected itself carefully would still expose pause/cancel/retry on a side port |
| Shipping an `application.yaml` with sane defaults | An `application.yaml` at a library jar's classpath root **competes with the host's own** — only one is loaded, decided by classpath order. Application configuration belongs to the application |

## Evidence

- `mohs-spring-boot-starter/src/main/java/io/mohs/autoconfigure/MohsUiAutoConfiguration.java` — the
  "host's own server, deliberately as the only mode" rationale.
- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/JdbcStoreTransactions.java` — `NESTED`
  propagation and why.
- `mohs-demo/src/main/java/io/mohs/MohsApplication.java` — why local defaults go through
  `setDefaultProperties` rather than a resource file.
- The absence of any Dockerfile, manifest or CI configuration in the repository.
