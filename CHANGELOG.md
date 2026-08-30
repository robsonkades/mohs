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
- `mohs.time.mode=database` on SQL Server now fails the boot naming the alternative. There,
  `CURRENT_TIMESTAMP` is a zoneless `DATETIME` the driver reads in the JVM's zone, so the sampled
  offset was the distance between two zones rather than between two clocks — silently, in the one
  component whose job is knowing the time.

### Changed

- `retries` defaults to `1` instead of `0`, so at-least-once delivery holds by default. Under a
  false positive of death detection this admits two **concurrent** invocations of the same
  execution, which `preventOverlap` does not stop.
- The Flyway chain `V1`→`V4` is destructive to data from the single-table era.
- **Applied migrations were rewritten before 1.0, and Flyway validates on migrate.** A database that
  already ran the previous `V5` (PostgreSQL) or `V3` (SQL Server, whose `VARCHAR` became `NVARCHAR`)
  fails to boot with a checksum mismatch. Pre-1.0 databases are disposable: recreate the schema, or
  run `flyway repair` against `mohs_schema_history` after confirming the schema matches
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
