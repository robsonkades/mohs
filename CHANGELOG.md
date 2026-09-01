# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with one qualification while the
version starts with `0.`: the public API in `io.mohs.core` may still change between minor versions,
and every such change is listed here under **Changed** with what to do about it.

The compatibility contract that will hold from `1.0.0` on — what may gain methods, what is sealed,
what the metric names and the shard hash promise — is in
[modules and publishing](docs/12-build/modules.md#versioning-policy).

## [Unreleased]

Nothing has been released yet. This section is the running record of what `0.1.0` will contain.

### Added

- `RetryPolicy` (`io.mohs.core.execution`): the per-job retry SPI a definition names through
  `@MohsJob(retryPolicy = "beanName")`. It replaces the `retries` budget while it returns a delay,
  is consulted on both failure paths — a handler that threw and a lease reclaimed from a dead node —
  and a job naming a bean that does not exist now fails the boot instead of silently falling back.
- `@OnExecution` is delivered. An annotated method becomes a subscriber of the engine's event
  stream, filtered by job and event type, with the same asynchronous best-effort contract as
  `ExecutionListener`. `job()` now defaults to empty, meaning every job — which is also what makes
  `BATCH_COMPLETED` observable, since a batch belongs to no single job.
- A Spring Boot Actuator `HealthIndicator` under the `mohs` key, contributed when the host brings
  the actuator (`spring-boot-health` is an optional dependency of the starter). `RUNNING` is `UP`,
  `PAUSED`/`DRAINING` are `OUT_OF_SERVICE`, `CREATED`/`STOPPED` are `DOWN`. **It never touches the
  database**, so a database outage cannot restart every healthy pod.
- `mohs.queue.depth`: a gauge of the backlog visible to a claim, cluster-wide. Sampled by the
  engine's own tick every 10 s rather than on the scrape, so a metrics pipeline cannot decide the
  load it is measuring. Aggregate it across instances with `max`, never `sum`.
- `mohs.engine.idempotency-retention` (default `7d`): the deduplication window, enforced by an
  hourly prune of `mohs_idempotency`. `0s` keeps every key forever, and the unbounded table that
  comes with it. The prune carries a 5-second query timeout: it runs on the engine's loop thread,
  ahead of the firing and the claim, and every node issues the same `DELETE` — without the cap, the
  node that loses the race would sit blocked on another node's row locks while heartbeating, owning
  its shards and claiming nothing. A prune that keeps timing out shows up as
  `mohs.tick.failed{step=idempotency-prune}`.
- A CI pipeline: `./mvnw clean verify` on every push and pull request, with the frontend built.
- `CONTRIBUTING.md`, `SECURITY.md` and this file; `<scm>`, `<developers>`, a reproducible-build
  timestamp and an opt-in `release` profile (sources, Javadoc, GPG, Central Portal upload).

### Fixed

- `GET /batches/{id}` returned the host's 404: `BatchesController` was implemented, tested and
  documented, and no `@Bean` registered it. A test now asserts that every `@RestController` in
  `io.mohs.rest` has a registering bean.
- `mohs.time.mode=database` sampled the distance between two ZONES rather than between two clocks on
  SQL Server and MySQL — silently, in the one component whose job is knowing the time. Both answer
  `CURRENT_TIMESTAMP` without a zone, and reading it back as a `java.sql.Timestamp` had the driver
  interpret it in the JVM's zone: measured at +10,800,059 ms on SQL Server and +10,799,407 ms on
  MySQL from a JVM three hours away. The now-query and the crossing back are now one per-dialect
  decision — `SYSUTCDATETIME()` and `UTC_TIMESTAMP(6)` state UTC and are read as a `LocalDateTime`
  declared to be UTC, while PostgreSQL and H2 keep `CURRENT_TIMESTAMP` and are read as an
  `OffsetDateTime`. The mode is supported on all four dialects; an interim build refused it on the
  two zoneless ones.

### Removed

- **Flyway, and with it every line of DDL Mohs used to execute.** `MohsFlyway`, the
  `mohs.jdbc.migrate` property, the `mohs_schema_history` table, `JdbcDelegate#migrationLocation()`
  and the four Flyway artifacts are gone. **You now install the schema before starting the
  application** — apply `schema-<dialect>.sql`, which still ships inside `mohs-store-jdbc`'s jar, and
  apply the `V*.sql` deltas yourself when upgrading. An embedded library should not run DDL against a
  database it does not own — and in most organisations that run one at scale, to a team that does
  not accept an application changing the schema at startup, still less a third-party jar inside it.

  **What this asks of you.** Starting against a database with no schema now fails at boot with the
  driver's own message (`relation "mohs_rate_limits" does not exist` on PostgreSQL) rather than
  creating anything. Nothing records which versions a database has already seen, so folding the
  `V*.sql` files into the migration tooling you already run is the recommended path — see
  [installing and upgrading the schema](docs/06-data/migrations.md). Delete `mohs.jdbc.migrate` from
  your configuration: an unknown `mohs.*` property is ignored, so it will not fail your boot, and it
  will not do anything either.

### Changed

- `mohs.engine.node-lease-ttl` now has a validated floor of `12s`; anything shorter fails the boot
  naming the property, the floor and what to type instead. The heartbeat goes out once per tick, and
  the sleep plus the prune and the queue-depth count that follow it (`node-lease-ttl/3` + 5 s + 2 s)
  have to fit inside the promise with room to spare — at 12 s a second is left for clock skew and the
  heartbeat's own write latency. Below the floor a node loses the lease it is renewing while alive and
  working, and its peers reap what it is still running.
- `retries` defaults to `1` instead of `0`, so at-least-once delivery holds by default. Under a
  false positive of death detection this admits two **concurrent** invocations of the same
  execution, which `preventOverlap` does not stop.
- The `V1`→`V4` chain is destructive to data from the single-table era.
- **Published deltas were rewritten before 1.0, and nothing detects that any more.** A database that
  already ran the previous `V5` (PostgreSQL) or `V3` (SQL Server, whose `VARCHAR` became `NVARCHAR`)
  used to fail the boot on a checksum mismatch; with Flyway gone there is no checksum and no
  complaint — it simply runs with the wrong schema. Pre-1.0 databases are disposable: recreate from
  `schema-<dialect>.sql`. On SQL Server the old `V3` is worse than a mismatch — its
  `IF OBJECT_ID(...) IS NULL` makes the new version a no-op there, so a fresh install gets `NVARCHAR`
  and an upgraded one keeps `VARCHAR`, and only the first satisfies `V8`'s 900-byte clustered-key
  limit. The structural guard test that compares both installation paths is what catches it.
- `V5` is a maintenance window on a large database — it copies the whole history inside one
  transaction. **Drain the cluster first**: with nodes still running, the pool is exhausted, the
  heartbeat cannot get a connection, and the peers reap a live node.
- **PostgreSQL primary keys are normalised, inside that same `V5`.** `mohs_execution` is keyed on
  `(execution_id)` and `mohs_attempt` on `(execution_id, number)` — the shape the other three
  dialects always had. The time-leading keys were a partitioning artefact that served no query once
  the partitioning was gone. `idx_mohs_execution_id` and `idx_mohs_attempt_exec` disappear (they
  became the keys). It **tightens a guarantee**: `execution_id` is now unique by the schema, where
  before two rows could share an id with different `created_at` values. Nothing produced such a row,
  but `V5` checks for one before copying, so a database that somehow has it fails naming the row
  rather than reporting "duplicate key" halfway through the migration.
