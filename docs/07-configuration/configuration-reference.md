# Configuration reference

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (`io.mohs.autoconfigure.MohsProperties`)

Every `mohs.*` property, bound as **immutable records with constructor binding** — a property is a
snapshot of the boot, not mutable state. `spring-boot-configuration-processor` is declared, so IDE
metadata is generated from the `@param` documentation.

## The complete table

| Property | Required | Default | Purpose |
| --- | --- | --- | --- |
| `mohs.enabled` | No | `true` | **Master gate.** `false` removes every Mohs bean from the context |
| `mohs.jdbc.dialect` | **YES** | *(none)* | `h2` / `postgresql` / `mysql` / `sqlserver`. Never auto-detected; an unset value **fails the boot** |
| `mohs.jdbc.migrate` | No | `true` | Run Mohs' own Flyway migrations at boot. `false` for an externally managed schema — the migrations stay in the jar as the source of truth |
| `mohs.engine.poll-interval` | No | `25ms` | The **floor** of the tick interval |
| `mohs.engine.max-poll-interval` | No | `2s` | The **ceiling** of the idle backoff. Must be `>= poll-interval` |
| `mohs.engine.batch-size` | No | `50` | Maximum executions claimed per claim statement |
| `mohs.engine.claim-rounds` | No | `1` | How many claims one tick may chain while batches come back full |
| `mohs.engine.lease-ttl` | No | `30s` | Feeds `lease_expires_at` at claim time; also the staleness cutoff for a legacy node row with no `expires_at` |
| `mohs.engine.node-lease-ttl` | No | `15s` | **The node's lease.** Each heartbeat promises `now + this` |
| `mohs.engine.watchdog-timeout` | No | *(none — off)* | The Watchdog Bound. When present it **must be greater than `node-lease-ttl`** |
| `mohs.engine.misfire-threshold` | No | `60s` | Separates a late firing from a missed one |
| `mohs.engine.idempotency-retention` | No | `7d` | **The deduplication window**: a key deduplicates for exactly as long as its row lives in `mohs_idempotency`, and the tick prunes older rows hourly, under a 5s query timeout so a prune can never hold the claim hostage. `0s` keeps every key forever, and the unbounded table that comes with it |
| `mohs.engine.dispatch-concurrency` | No | `64` | The node's ceiling on in-flight executions. **Also bounds the claim**, and sizes the built-in `io` runner |
| `mohs.engine.event-concurrency` | No | `16` | The event publisher's concurrency ceiling |
| `mohs.engine.completion-flush-on-every-result` | No | `false` | `true` turns off group commit, returning to a synchronous commit per result |
| `mohs.lifecycle.start-mode` | No | `auto` | `auto` starts the engine at boot; `manual` waits for `mohs.lifecycle().start()` |
| `mohs.lifecycle.shutdown.grace-period` | No | `30s` | How long shutdown waits for in-flight executions before interrupting them |
| `mohs.time.mode` | No | `application` | `application` uses the system clock; `database` uses `DatabaseClock` |
| `mohs.time.skew-warn-threshold` | No | `1s` | **`database` mode only.** WARN threshold for measured clock skew |
| `mohs.time.sync-interval` | No | `30s` | **`database` mode only.** How often to resample the offset |
| `mohs.registration.on-conflict` | No | `override` | `override` / `preserve` / `fail` — how definitional drift between code and store is resolved |
| `mohs.api.enabled` | No | **`false`** | Turns the operational REST API on |
| `mohs.api.base-path` | No | `/api/mohs/v1` | The prefix for every `io.mohs.rest` route |
| `mohs.runners.<name>.*` | No | — | Additional named runners; see below |
| `mohs.rate-limits.<name>.max` | **YES**, per entry | — | Firings allowed per window, cluster-wide |
| `mohs.rate-limits.<name>.window` | **YES**, per entry | — | The window (`1m`, `PT30S`) |

### Runner map entries

| Property | Applies to | Default |
| --- | --- | --- |
| `mohs.runners.<name>.mode` | Both | `io` |
| `mohs.runners.<name>.max` | **IO only** | 64 |
| `mohs.runners.<name>.core-size` | **CPU only** | `Runtime.availableProcessors()` |
| `mohs.runners.<name>.max-size` | **CPU only** | `core-size` |
| `mohs.runners.<name>.queue-capacity` | **CPU only** | `0` (direct hand-off) |
| `mohs.runners.<name>.keep-alive` | **CPU only** | `60s` |

**A field belonging to the wrong mode is a boot error, never a silent discard.** `core-size = 2` with
`mode` left at the `io` default would otherwise become a runner of 64 virtual threads for CPU-bound
work, with no warning at all — and because `core-size`'s default depends on the machine's cores, the
boot would fail only in production.

**Map-key case**: Spring's binder canonicalises a non-bracketed map key to lower case, so
`mohs.runners.myUpload.*` registers the runner as `myupload`, while `JobDefinition.runner()` is
case-sensitive. Prefer lower-case names; to preserve exact case use the bracketed form
`mohs.runners.[myUpload].max=8`.

## Minimal configuration

```yaml
mohs:
  jdbc:
    dialect: postgresql
```

That is genuinely all that is required. Everything else has a default.

## Typical production configuration

```yaml
mohs:
  jdbc:
    dialect: postgresql
    migrate: true
  engine:
    poll-interval: 50ms
    max-poll-interval: 2s
    batch-size: 500
    claim-rounds: 4
    dispatch-concurrency: 512
    event-concurrency: 64
    node-lease-ttl: 15s
    lease-ttl: 30s
    watchdog-timeout: 10m
  lifecycle:
    shutdown:
      grace-period: 30s
  registration:
    on-conflict: override
  api:
    enabled: true              # WARN at boot: no authentication
  rate-limits:
    smtp:
      max: 100
      window: 1m

spring:
  datasource:
    hikari:
      maximum-pool-size: 250   # high for virtual threads
      connection-timeout: 3000 # low
```

## Validated relationships

These are checked at boot and **fail the boot** when violated:

| Constraint | Where | Message names |
| --- | --- | --- |
| `dialect` is set | `MohsAutoConfiguration` | The four valid values |
| `batch-size > 0` | `EngineSettings` | |
| `dispatch-concurrency > 0` | `EngineSettings` | The property |
| `claim-rounds > 0` | `EngineSettings` | That `1` is the classic one-claim-per-tick shape, not zero |
| `poll-interval > 0` | `EngineSettings` | The property and the value |
| `max-poll-interval >= poll-interval` | `EngineSettings` | That it is the ceiling the backoff climbs to, not a second floor |
| `lease-ttl > 0` | `EngineSettings` | That a non-positive lease is born expired and turns the first tick into a reclaim storm |
| `node-lease-ttl > 0` | `EngineSettings` | That every peer's reaper would reclaim this node's work |
| `watchdog-timeout > node-lease-ttl` | `EngineSettings` | That the bound sits **on top of** node liveness, not as a shorter lease |
| `misfire-threshold > 0` | `EngineSettings` | That a non-positive threshold turns every normally-late fire into a misfire |
| `idempotency-retention >= 0` | `EngineSettings` | That zero is the opt-out and negative is meaningless — the message says so rather than pruning by a cutoff in the future |
| `time.mode != database` on SQL Server | `MohsAutoConfiguration` | That `CURRENT_TIMESTAMP` is zoneless there, so the sampled offset would be the JVM's zone rather than the database's clock |
| every declared `retryPolicy` has a bean | `MohsEngineLifecycle` | That a missing bean would fail executions on the built-in backoff, indistinguishable from the custom policy having chosen it (ORPHANED definitions are exempt — their annotation is gone from the code, so the bean legitimately is too) |
| A rate limit's `window >= max` nanoseconds | `RateLimit.requireRefillable` | That one token is issued every `window/max` |
| A runner field matches its mode | `MohsRunners` | The property and the mode |
| A runner or limit name declared twice | `MohsRunners` / `MohsRateLimits` | Both sources |

## Warnings emitted at boot

Not failures, but each one names a real consequence:

| Condition | Warning |
| --- | --- |
| `dialect=h2` | H2 is a test/dev backend, not supported in production |
| `api.enabled=true` | The API is served with **no authentication** and can cancel, retry, pause, drain and change rate limits. Restrict the network or put a gateway/mTLS in front |
| `mohs-ui` present but `api.enabled=false` | The dashboard will load and stay empty |
| `mohs-ui` present and `api.base-path` non-default | The dashboard will 404 on every call — it pins `/api/mohs/v1` |
| `poll-interval > node-lease-ttl / 3` | The effective cadence is capped by liveness |
| The `io` runner overridden below `dispatch-concurrency` | The excess will be rejected by the executor and sit `RUNNING` until the reaper reclaims it |
| A job's `timeout >= watchdog-timeout` | The watchdog would release ownership before the job's own deadline |
| A job declares a `retryPolicy` | It is not honoured yet |

## Interactions worth knowing

### `dispatch-concurrency` is three things at once

1. The node's ceiling on in-flight executions.
2. The claim's per-lap budget (`min(batch-size, dispatch-concurrency − inFlight)`).
3. The **size of the built-in `io` runner**.

Raising it therefore raises the claim bound, the in-flight ceiling and the default runner together.
Overriding the `io` runner separately to a *smaller* value breaks the single-source assumption and
logs a WARN.

### `poll-interval` is a floor, not a period

The actual sleep is:

```
delay      = workFound ? poll-interval : min(delay × 2, max-poll-interval)
delay      = shortened to the nearest armed trigger, floored at poll-interval
actualWait = min(delay, node-lease-ttl / 3)
```

So `poll-interval` bounds how *often* the loop can tick, `max-poll-interval` bounds how *rarely*, and
`node-lease-ttl / 3` overrides both when liveness demands it.

### `node-lease-ttl` governs more than liveness

| Derived value | Formula |
| --- | --- |
| Maximum tick cadence | `node-lease-ttl / 3` |
| The claim lap's time budget | `node-lease-ttl / 4` |
| The stop's loop-join budget | `node-lease-ttl / 4` |
| Heartbeat-row retention | `node-lease-ttl × 10` |
| Minimum `watchdog-timeout` | Strictly greater than `node-lease-ttl` |
| Crash recovery latency floor | `node-lease-ttl` |

Lowering it speeds recovery and raises the tick's minimum frequency. Raising it does the reverse.

### `lease-ttl` and the rate-limit lock

The rate-limit bucket's 2-second statement timeout assumes a generous `lease-ttl`. **A `lease-ttl`
below about 10 s calls for revisiting that internal value**, because the wait must fit comfortably
inside the TTL — otherwise the ceiling that protects the heartbeat becomes what consumes it.

### `api.base-path` and the dashboard

The bundled dashboard pins `/api/mohs/v1` in compiled JavaScript. Changing `base-path` while
shipping `mohs-ui` breaks every dashboard call. Serve the API at the default prefix, or proxy
`/api/mohs/v1` to it.

## Environment variables

Standard Spring Boot relaxed binding applies:

| Property | Environment variable |
| --- | --- |
| `mohs.jdbc.dialect` | `MOHS_JDBC_DIALECT` |
| `mohs.engine.poll-interval` | `MOHS_ENGINE_POLL_INTERVAL` |
| `mohs.api.enabled` | `MOHS_API_ENABLED` |
| `mohs.rate-limits.smtp.max` | `MOHS_RATELIMITS_SMTP_MAX` |

## Duration format

Spring Boot's duration binding: `25ms`, `2s`, `15s`, `1m`, `10m`, `PT10M`. The `?window=` query
parameter on `/overview` accepts **both** styles, parsed explicitly rather than through the host's
`ConversionService`.

## Secrets

**Mohs itself has no secret configuration.** No credential, token, password or connection string is
read from `mohs.*`. The database connection comes entirely from the host's `spring.datasource.*`,
and its secret management is the host's concern.

The one security-relevant property is `mohs.api.enabled`, and it is not a secret — it is a decision.

## Properties that do **not** exist

Searched for and absent; several appear in code comments as hypothetical:

| Absent property | Status |
| --- | --- |
| `mohs.engine.node-heartbeat-interval` | Referenced in `NodeStore`'s Javadoc as configuration "that does not exist yet". The cadence is derived from `node-lease-ttl` |
| A retention or purge window for history | **Not present.** See [data lifecycle](../06-data/data-lifecycle.md) |
| An idempotency-window property | Not present; the prune method exists but nothing schedules it |
| A stream-interval property for the SSE tick | Fixed at 2 s in code, with a comment inviting it to become a property |
| Retry base/cap/jitter properties | Internal constants (1 s / 10 min) with no property |
| `flushSize` / `flushInterval` for group commit | Fixed at 256 / 5 ms; the only knob is the opt-out |
| Anything for `ExecutionWindow` | Deliberate — predicates exist only in code |
| Authentication or authorization settings | **Not present.** See [security](../08-security/security-overview.md) |
