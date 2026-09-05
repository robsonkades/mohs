# Deployment

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## What the repository contains

**No deployment artefacts.** Verified by a repository-wide search:

| Artefact | Present |
| --- | --- |
| `Dockerfile` | **No** |
| `docker-compose.yml` | **No** |
| Kubernetes manifests | **No** |
| Helm chart | **No** |
| Terraform / Pulumi / CloudFormation | **No** |
| CI and release workflows | **Yes**, GitHub Actions builds/tests and publishes library artifacts; it does not deploy a service |
| Environment-specific configuration | **No** — there is not even an `application.yaml` |

This is a **consequence of what Mohs is**, not an omission at the deployment layer: Mohs is a
library. It has no process, no port and no image of its own. It inherits the host application's
deployment entirely.

What follows is therefore **guidance for the host application**, derived from what the code assumes
about its runtime.

## Runtime requirements

| Requirement | Value |
| --- | --- |
| JRE | **Java 25** (`maven.compiler.release=25`) |
| Spring Boot | 4.1.1 in the host, or a compatible version |
| Database | PostgreSQL, MySQL 8.0+ or SQL Server (which **requires `READ_COMMITTED_SNAPSHOT ON`** — the boot refuses without it). **H2 is dev/test only** and warns at boot |
| JDBC driver | Supplied by the host — Mohs declares none at compile scope |
| Web server | Only if the REST API or the dashboard is enabled (`spring-boot-starter-webmvc`) |
| Network | Database connectivity only. Nodes never talk to each other directly |
| Filesystem | None. Mohs writes no files |

## Sizing

### Database connections

Size the host's pool for measured simultaneous database use: handler transactions, the
engine loop, completion flushing, optional snapshot reads and application requests.
Virtual-thread concurrency is not a connection-count target. Start with a measured baseline
and watch pool wait time and database saturation before increasing it.

```yaml
spring:
  datasource:
    hikari:
      connection-timeout: 3000
```

Hikari's acquisition timeout must be strictly below `mohs.engine.node-lease-ttl`.
The actual pool is validated at startup; the default 30-second Hikari timeout is incompatible
with the default 15-second node lease. For other pools and opaque wrappers, verify the same
relationship yourself. See [configuration](../07-configuration/configuration-reference.md).

### Memory

Budget for in-flight deserialized payloads, definitions, completion buffering, the cron cache
and batch callbacks, in addition to the host application's memory. Large payloads and many
dynamic definitions can dominate the library's footprint.

### CPU and nodes

Route CPU-bound work to a bounded CPU runner. Mohs has 64 fixed claim shards, so more than
64 active nodes leaves some nodes unable to claim. Scaling depends on the database and the
workload; repository benchmarks describe their own environments, not deployment guarantees.

## Container guidance

If the host application is containerised — a starting point, not a supplied artefact:

```dockerfile
FROM eclipse-temurin:25-jre
COPY target/your-app.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

| Setting | Guidance |
| --- | --- |
| `terminationGracePeriodSeconds` | **Greater than** `mohs.lifecycle.shutdown.grace-period` **plus** the web server's graceful-shutdown phase. 60 s is a sane floor for the 30 s default |
| `spring.lifecycle.timeout-per-shutdown-phase` | Must exceed the drain grace, or Spring aborts the phase mid-drain |
| Memory limits | The JVM is container-aware; leave headroom for direct buffers and metaspace |
| SIGTERM handling | Spring Boot handles it; the shutdown sequence is in [startup and shutdown](startup-and-shutdown.md) |

## Probes

```yaml
livenessProbe:
  httpGet: { path: /actuator/health/liveness, port: 8080 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /actuator/health/readiness, port: 8080 }
  periodSeconds: 5
startupProbe:
  httpGet: { path: /actuator/health/liveness, port: 8080 }
  failureThreshold: 30
  periodSeconds: 5
```

| Probe | Rule |
| --- | --- |
| Liveness | **Never depend on the database.** A database outage would restart every healthy pod. Do not include the engine's state either: a `DRAINING` node is winding down deliberately, and a `PAUSED` node is an operator's decision |
| Readiness | `state == RUNNING` is reasonable **if** you want a paused node out of the load balancer. Mohs' work does not arrive through the load balancer, so this affects only REST and the dashboard |
| Startup | Preferable to a guessed `initialDelaySeconds` — boot includes the first clock sample and the schema checks |

**Mohs contributes a `HealthIndicator` under the `mohs` key** when the actuator is present, and it
never reads the database. See [health and diagnostics](../09-observability/health-and-diagnostics.md)
for how to wire the probes to it.

## The schema at deploy time

**It is a step of your deploy, not of the boot.** Mohs executes no DDL, so nothing happens
automatically and no replica races another to apply anything.

| You are | Do this, before the replicas start |
| --- | --- |
| Installing fresh | Apply `schema-<dialect>.sql` — the complete current schema, idempotent |
| Upgrading | Apply the `V*.sql` deltas after the version this database is on |

Both ship inside `mohs-store-jdbc`'s jar. See
[installing and upgrading the schema](../06-data/migrations.md) for how to extract and apply them,
and for the part that is now yours: **nothing records which versions a database has already seen.**

**Ordering matters and nothing enforces it.** A replica that starts against a database missing a
column fails on its first statement with the driver's error and does not start — it corrupts
nothing, but it also does not wait. Apply the schema first, then roll the replicas.

**Plan maintenance for table-rebuilding deltas**, including MySQL `V10` and PostgreSQL's `V5`, which converts the partitioned history
tables to normal ones by copying rows under an `ACCESS EXCLUSIVE` lock. Peak space is 2× the larger
table plus indexes, and the tables are sealed — not even readable — for the duration. Run it in a
`READ COMMITTED` session during a window. **Take a backup first: there is no undo.**

## Multi-environment configuration

Mohs ships **no `application.yaml`** in any published jar, deliberately: one at a library jar's
classpath root competes with the host application's own, and only one is loaded, decided by classpath
order. Application configuration always belongs to the application.

Use the host's own profile mechanism:

```yaml
# application.yaml
mohs:
  jdbc:
    dialect: postgresql

---
spring.config.activate.on-profile: dev
mohs:
  jdbc:
    dialect: h2      # WARNs at boot; correct for dev
  api:
    enabled: true

---
spring.config.activate.on-profile: prod
mohs:
  engine:
    dispatch-concurrency: 512
    batch-size: 500
    claim-rounds: 4
  api:
    enabled: false   # or true, behind a SecurityFilterChain
```

## Deployment checklist

- [ ] Java 25 runtime
- [ ] A production dialect (**not** H2) with `mohs.jdbc.dialect` set — it is the one mandatory property
- [ ] A JDBC driver on the classpath
- [ ] A connection pool sized for `dispatch-concurrency` plus the tick plus the host's own usage
- [ ] `terminationGracePeriodSeconds` > drain grace + web graceful shutdown
- [ ] `spring.lifecycle.timeout-per-shutdown-phase` > drain grace
- [ ] Probes that do not depend on the database for liveness
- [ ] A migration plan, especially if PostgreSQL `V5` has not yet been applied
- [ ] **A retention plan** — configure opt-in history retention or manage cleanup externally
- [ ] A `SecurityFilterChain` if `mohs.api.enabled=true`
- [ ] Metrics scraped; alert on `mohs.lease.reclaimed` and `mohs.tick.failed`
- [ ] Replica count at or below 64
- [ ] NTP on every node, or `mohs.time.mode=database`

## What is deliberately not offered

| Not offered | Reason |
| --- | --- |
| A standalone scheduler server | Mohs is embedded. A separate server would sit outside the host's security perimeter |
| A separate port for the API or the dashboard | Same reason, stated explicitly in `MohsUiAutoConfiguration`: an application that protected itself carefully would still expose pause/cancel/retry on a side port |
| An official image | There is no process to image |
| A Helm chart | There is no workload of Mohs' own to template |
