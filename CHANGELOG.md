# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with one qualification while the
version starts with `0.`: the public API in `io.mohs.core` may still change between minor versions,
and every such change is listed here under **Changed** or **Removed** with what to do about it.

The compatibility contract that will hold from `1.0.0` on — what may gain methods, what is sealed,
what the metric names and the shard hash promise — is in
[modules and publishing](docs/12-build/modules.md#versioning-policy).

## [Unreleased]

Nothing has been released yet. This section is the running record of what `0.1.0` will contain.

### Added

- `mohs.lifecycle.startup-delay` (default `0s`): a nonblocking, node-local wait from
  automatic or manual `start()` before the engine's first tick. Shutdown cancels the wait;
  work signals cannot shorten it. Component initialization and API calls remain available.
- `EngineState.STARTING` exposes the pending startup delay and maps to `OUT_OF_SERVICE`
  in the health indicator. Consumers with exhaustive engine-state switches must handle it.
  `EngineSettings` and `MohsProperties.Lifecycle` gain `startupDelay` components; the previous
  constructor signatures remain available with zero delay.
- `mohs-benchmark/scripts/api-load.ps1`: finite execution load through the existing REST API,
  with bounded request concurrency, waves, idempotency keys, CSV receipts and dry-run support.

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
- `mohs.engine.history-retention` (default `0s`, meaning forever): how long a terminal execution's
  history — its row, its attempts, and a batch none of whose members remain — survives after
  finishing. Opt-in: deleting history is the operator's decision, never the scheduler's surprise. A
  positive window is swept hourly on the tick, in bounded batches ranged by the UUIDv7 primary key,
  so no new index is paid for it. `mohs_idempotency` is not touched — it answers to
  `idempotency-retention` alone.
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

- Restore and synchronize the frontend lockfile so Maven's `npm ci` can install dependencies,
  removing a stale local self-dependency and resolving the incompatible `cn` entry.

- Refined the dashboard navigation, responsive metric strip and job search. Jobs and executions
  expose removable filters, keyboard-accessible detail actions, expandable attempt errors and
  clearer operation feedback. Activity series can be toggled, connection status is distinct from
  paused list updates, and secondary text remains readable on hover surfaces.

- Completed missing Javadoc comments and parameter, return and exception tags across the API,
  engine, JDBC, REST, Spring integration, test kit and examples. Documentation builds now pass
  with warnings treated as errors; idempotency retention and enclosing-transaction commit
  semantics are explicit in the scheduling contract.

- Updated the dashboard tooling's transitive `fast-uri` and `qs` dependencies to resolve npm audit
  findings. Fixed invalid Javadoc references in the engine and demo, and strengthened the cluster
  regression test to require completed work from both live nodes.

- REST validation inspects at most 64 exceptions in a cause chain; cyclic or deeper chains return
  the generic unreadable-request response. Dispatcher tests now distinguish an immediately visible
  retry from a waiting retry, preserving the built-in jitter's zero-delay case.
- Above 1,000 inadmissible jobs, admission losers wait one maximum poll interval before becoming
  visible again. Truncating the SQL exclusion list alone left its omitted jobs immediately
  re-claimable, consuming the budget of admissible work. The attempt number is unchanged.
- Startup rejects a Hikari connection acquisition timeout at or above the node lease, including
  pools behind Spring delegating proxies. Query timeouts do not cover waiting for a connection.
- Database clock samples taking over one second are discarded. Accepted offsets can decrease;
  monotonicity is enforced on returned instants instead, so an erroneously fast offset can recover.
- REST payload conversion errors no longer expose Jackson diagnostics, host class names or rejected
  values. Missing-resource errors do not reflect request identifiers, and only Mohs request
  constructor validations expose their original detail.

- **SQL Server: two claims could deadlock on each other's picks.** The claim's `DELETE FROM
  mohs_ready WHERE execution_id IN (…)` was compiled as a scan of `idx_mohs_ready_claim` whenever the
  table's statistics were empty or stale (a fresh install, a queue that emptied and refilled), and a
  write plan's scan takes `U` locks on every key it passes — the keys a peer's `UPDLOCK` pick had
  just taken. Measured under eight nodes on one shard: deadlock or 3 s statement timeout in four of
  nine runs; the plan is cached, so once compiled it hits every tick until a recompile. The queue
  side of the claim is now one `DELETE … OUTPUT` self-joined (`INNER LOOP JOIN`) on the
  `TOP … WITH (UPDLOCK, ROWLOCK, READPAST)` pick, driven by the same seek that takes the locks —
  zero failures in ten runs. Rows and order returned are unchanged; `READPAST` on the delete target
  keeps skipping a row a concurrent `cancel` holds, which a plain updatable CTE did not.
- A node whose heartbeat kept failing (a network partition) bumped its epoch and logged
  `node lease expired … epoch bumped` on every tick for as long as the partition lasted. The
  expiry is now consumed when first observed: one bump and one WARN per death, and the next promise
  is recorded only by a heartbeat the database actually saw.
- The dashboard answered every missing path under `/mohs-ui/**` with `index.html` and a 200, a
  stale script of a previous bundle included — the browser received HTML where it expected
  JavaScript and failed with a syntax error instead of a 404. A missing file under `assets/` is now
  a 404; only client routes fall back to the page. The bundle also went out with no
  `Cache-Control`: the content-hashed `assets/**` are now immutable for a year, and `index.html`
  with the icons are `no-cache`, so a deploy is picked up on the next load.
- A statement issued on the engine's loop thread could wait longer than the node's own lease. Only
  the queue-depth count, the prunes and the rate-limit charge carried a query timeout; the
  heartbeat, the reaper's and reconcile's reads, the cancel poll, the firing CAS, the claim and the
  requeue did not — so a lock held by a peer, or a host transaction on a definition row, could hold
  the tick past `node-lease-ttl` while the node was alive and working, and its peers would reap the
  work it was still running. Every statement the loop thread issues that can wait on a lock now
  carries a 3-second ceiling, a quarter of the validated 12-second floor — the heartbeat, the node
  and lease reads behind the reaper and the reconcile, the cancel poll, the firing CAS, the claim,
  the requeue, and the reaper's own completion as a transaction deadline over chunks of 50 reclaims
  (a mass death that could not finish inside one deadline would roll back whole and never reclaim
  anything; a chunk that times out loses only itself). A step that hits it is lost, the lease is
  not; it is counted in `mohs.tick.failed{step}` when the step runs under maintenance isolation, and
  under the new `step=tick` label when the heartbeat, the definitions read, the due-trigger read or
  the claim died. The firing sweep and the reaper's sweep now carry the claim laps' budget
  (`node-lease-ttl/4`): up to 500 CASes at 3 s each, or ten reclaim chunks each waiting out its
  deadline, would still outlast the lease when a host transaction holds a few definition rows — so
  each sweep stops at the budget, logs how many wait, and takes them up next tick. Deliberately outside the ceiling: the definition scans, whose
  cost is rows transferred rather than a lock (2.8 s measured at 1M definitions, which a ceiling
  would turn into a tick that dies every cycle), and everything host threads issue — the enqueue,
  the read model, the completion flush — because cancelling a caller's write mid-flight is a
  different decision. The one host read that shares a tick template is `GET /nodes`, one row per
  node. Waiting for a pool connection is not covered by a query timeout: keep HikariCP's
  `connection-timeout` below `node-lease-ttl`, as the tuning guide already asks.
- The due-trigger read now carries its ceiling in the SQL (`LIMIT`/`TOP`) instead of a
  `Stream.limit` in Java. The server sorted every due row regardless, and in autocommit pgjdbc and
  Connector/J also materialise the whole result before the first row, so a cluster returning from
  downtime with 1M triggers due paid an external sort of 80 MB and 2.3 s per tick to fire 500 of
  them; with the ceiling in the SQL the planner walks `idx_mohs_job_next_fire` in order and stops at
  the 500th row — 20 buffers, 0.1 ms, measured on PostgreSQL 16.
- A cron day-of-week range of width zero on Sunday — `7-7`, or `SUN-SUN` — fired every day of the
  week. The vendored parser rewrote a leading `7` to `0` so that a wrapping `7-1` means Sunday to
  Monday, and applied the rewrite to `7-7` as well, expanding it to `0..7`. A weekly Sunday job
  written that way ran seven times a week, with no warning. The rewrite now applies only to a range
  with width; the divergence from upstream is recorded in the parser's header.
- A whitespace-only job key or execution id on a REST route — `/jobs/%20`, `/executions/%20`, and
  the cancel and retry actions — was a 500 with a stack trace at ERROR, reachable by anyone: the
  value objects refused the blank and nothing mapped the refusal. It is a 422 naming the field now,
  and a blank `?jobKey=` filter on `GET /executions` is treated as no filter.
- A claimed history row the node could not turn into an `Execution` (a `priority` outside the enum,
  written by an operator or a migration) threw out of the dispatch loop and took the whole claimed
  batch and the tick with it: every sibling stayed leased with nothing dispatching it, and the same
  row was re-claimed and re-thrown on every tick. Such a row now fails alone, terminally, the way an
  unreadable payload does; its siblings dispatch.
- The 64-subscriber ceiling of `/overview/stream` was a size check before the join's four database
  reads, so concurrent joins all passed it and each paid the reads — the amplification the ceiling
  exists to contain. The seat is now a semaphore permit taken before the reads and given back when
  the client leaves, or when its initial snapshot fails.
- A completion flusher thread that died outside its flush (an `Error` between polls) left the
  results still queued with their in-transit markers set, hiding those leases from the stray-lease
  reconcile forever. The thread's last act now completes what was queued, one by one, as `close()`
  already did.
- **The completion fence now includes the attempt number.** It was `(node_id, epoch)`, and a healthy
  node's epoch only moves when its own lease expires — so after a Watchdog Bound released an
  incarnation, the same node could re-claim the retry with the same pair and the zombie's completion
  passed the fence, deleting the new incarnation's lease. Only the attempt table's primary key
  stood in the way, aborting the whole group commit with an error that blamed the wrong lease. The
  fenced `DELETE` on all four dialects now also matches `attempt_number`; no schema change, the
  column was always there. A `JdbcDelegate` of your own must add `AND attempt_number = :attemptNumber`
  to its `fencedLeaseDelete()`: the parameter is bound on every call, and a statement that does not
  consume it keeps the two-part fence with no error to tell you so.
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

- `JobContext#progress(done, total)`. It was published as "optional, dashboard-oriented, a no-op
  when nothing observes it" — and nothing ever observed it: no store column, no REST field, no
  dashboard component. A handler calling it in a loop paid a virtual call for nothing. Delete the
  call; the dashboard shows attempt timing and state, not intra-attempt progress.
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

- `Enqueued` is now published to the `ExecutionListener`s, and to `@OnExecution(ENQUEUED)`, once
  the enqueue is durable: with no transaction bound, when the terminal returns; inside a host
  transaction, after the host's commit — a rollback publishes nothing, and neither does a request
  deduplicated by its `Idempotency-Key`. Batch members publish one `Enqueued` each. The event was
  in the sealed hierarchy and accepted by `@OnExecution` but nothing ever published it. A
  `StoreTransactions` of your own now implements `inTransaction(Runnable work, Runnable onDurable)`
  and must run `onDurable` once the writes are committed — right after, or on the host's
  `afterCommit` when it joined one.
- `Started` now means "the interceptor chain, and through it the handler, is about to be invoked".
  An attempt that ended before the chain ran at all — no handler registered for the job, or a
  cancellation that arrived before the start — used to publish `Started` and then its terminal
  event; it now publishes the terminal event alone.
- `Attempt.error` is capped at 256 KB: the exception's message past that is replaced by
  `… [truncated N chars]`. A handler that echoed a response body into its exception stored that
  much per attempt, and `GET /executions/{id}` returned all of it; a stack trace never comes close.
- `JobKey` refuses a value longer than 255 characters, the width of every `job_key` column, with a
  message naming the limit — at definition time, not at the first write, where one dialect raises a
  driver error and MySQL without strict mode truncates the value and lets two keys collide. The
  idempotency key is bounded the same way, in the Java API — `ScheduleCommand.idempotencyKey`
  refuses a blank key and one longer than `ScheduleCommand.MAX_IDEMPOTENCY_KEY_LENGTH` — and the `Idempotency-Key`
  header answers a 422 above it, for the same reason: a truncated key would deduplicate a genuinely
  new schedule away.
- `POST /executions/{id}/cancel` and `/retry` resolve the actor **before** the mutation, like every
  other mutation of the API (an invalid `X-Mohs-Actor` is a 400 and nothing changes), and log the
  request at INFO with that actor — the execution keeps its original invoker, so the log line is the
  audit trail of who asked.
- The REST layer converts a request body into the job's payload type with Mohs' own `JsonMapper`,
  the one the store persists and reads with — no longer the host's context `ObjectMapper`, whose
  naming strategy or modules could accept a body the store would not read back the same way. That
  conversion is strict: an unknown property in the payload is a 422 naming it, where before a
  misspelt field became a silent `null` the job ran with. The store's own reads stay tolerant.
- `mohs.api.base-path` is validated at boot: it must start with `/` and must not end with one. It is
  concatenated into every route and into the `Location` of every 202, and an empty value mounted the
  API at the host's root, outside the `securityMatcher` the documentation recommends.
- **A new delta for MySQL, `V10__utf8mb4_table_split.sql`.** `V3` created the five tables of the
  table split without the `DEFAULT CHARACTER SET utf8mb4` that every other table declares, so on a
  server whose default is not `utf8mb4` their `job_key` columns ended up in a different character
  set from `mohs_job_definitions.job_key` and the two statements that compare them across tables
  failed with an illegal mix of collations, or coerced one side and lost the index. The installer
  and `V3` now carry the clause; `V10` converts a database that ran the old `V3` to the collation
  the `V1` tables have (a no-op where the five already share it) and keeps `payload`/`error` as
  `MEDIUMTEXT`. It is a copying rebuild that blocks writers for its whole duration — minutes per
  million rows of `mohs_execution`, measured — so it has its own section in
  `docs/06-data/migrations.md`, like `V5`.
- **SQL Server requires `READ_COMMITTED_SNAPSHOT`, and the boot refuses without it.** The
  `sqlserver` dialect used to degrade behind `WITH (NOLOCK)` hints on its idle-gate probe; those
  hints are gone and the probe is byte-identical across the four dialects. Before starting, run
  `ALTER DATABASE [<name>] SET READ_COMMITTED_SNAPSHOT ON` — the boot error carries the exact
  statement for the database it inspected.
- **A new delta to apply on upgrade, on all four dialects: `V9__due_trigger_index.sql`.** It adds
  `idx_mohs_job_next_fire` on `mohs_job_definitions (next_fire_at)`; the due-trigger scan that runs
  on every tick had no index at all and was a full scan of every definition. Measured on PostgreSQL
  at 100k definitions: 7.63 ms and 1822 buffers before, 0.229 ms and 48 buffers after. The
  installers already carry it; an existing database gets it only through the delta.
- A second engine bound to the same `MeterRegistry` now fails at construction with an
  `IllegalStateException` naming the colliding gauge (`mohs.node.inflight` or `mohs.queue.depth`) and
  the mitigation (one registry per engine, with its own exporter). Micrometer used to keep the first
  meter behind a generic WARN, so a dashboard showed one engine's saturation under the other's name.
  One Mohs per application, the auto-configured case, is unaffected.
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
