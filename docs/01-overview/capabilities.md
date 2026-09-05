# Capabilities

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

The complete feature inventory, each row traceable to code. Status markers are defined in the
[documentation portal](../README.md#conventions-used-in-this-documentation).

## Job definition

| Capability | Status | Evidence |
| --- | --- | --- |
| Declare a job on a Spring bean method with `@MohsJob` | Implemented | `io.mohs.core.definition.MohsJob`, `MohsJobScanner` |
| Stereotypes `@RecurringJob` / `@OnDemandJob` | Implemented | Meta-annotated over `@MohsJob` with `@AliasFor`; resolved through merged annotations |
| User-defined composed stereotypes | Implemented | `@MohsJob` targets `ANNOTATION_TYPE`; scanner uses `MergedAnnotations` |
| Programmatic definition via staged builder | Implemented | `JobDefinition.of(id, handlerType, spec -> ...)`, `JobSpec` then `PolicySpec` |
| Handler signature: optional payload + optional `JobContext`, any order | Implemented | `MohsJobs.ParameterBinding.of` |
| Typed job reference (`JobRef<T>`) for compile-time payload safety | Implemented | `io.mohs.core.job.JobRef` |
| Definition drift policy on redeploy (`override` / `preserve` / `fail`) | Implemented | `MohsProperties.Registration.OnConflict`, `MohsJobScanner#reconcile` |
| Annotated job removed from code becomes `ORPHANED`, history kept | Implemented | `MohsJobScanner#reconcileOrphans`, `JobStore#markOrphaned` |
| Explicit retirement of programmatic jobs | Implemented | `MohsImpl#remove` performs a soft retire and drains the queue |
| `@OnExecution` per-method event listener | **Implemented** | The annotated method subscribes to the engine's event stream, filtered by job and event type (`OnExecutionRegistry`); an impossible signature or filter fails the boot |
| Custom `retryPolicy` bean name | **Implemented** | The named `RetryPolicy` bean is consulted on both failure paths (`RetryPolicyRegistry`); a job naming a bean that does not exist fails the boot (`MohsEngineLifecycle#checkDeclaredPolicies`) |

## Scheduling

| Capability | Status | Evidence |
| --- | --- | --- |
| Cron, seconds-first Quartz syntax (`L`, `W`, `#`) | Implemented | `io.mohs.cron.CronExpression`, vendored from Spring |
| Cron evaluated in an explicit `ZoneId`, never the JVM default | Implemented | `CronSpec(expression, zone)`; the zone is mandatory |
| Fixed-rate interval (anchored to the scheduled time) | Implemented | `IntervalSpec(interval, afterFinish=false)` |
| Fixed-delay interval (anchored to the previous completion) | Implemented | `IntervalSpec(afterFinish=true)`; rearmed inside the completion transaction |
| On-demand (no automatic trigger) | Implemented | `OnDemandSpec` |
| Misfire policies `IGNORE` / `FIRE_NOW` / `FIRE_ALL_MISSED` | Implemented | `FiringPlanner#plan` |
| Misfire threshold separating "late" from "missed" | Implemented | `mohs.engine.misfire-threshold`, default 60s |
| Replay capped at 1,440 occurrences per job per cycle, drained not discarded | Implemented | `FiringPlanner.MAX_OCCURRENCES_PER_CYCLE` |
| DST fall-back suppression (a daily job does not fire twice) | Implemented | `NextFireCalculator#nextCronFire` |
| DST spring-forward gap compensation | **Not present**, deliberate | Documented divergence from Quartz in `NextFireCalculator` |
| Runtime reschedule | Implemented | `Mohs.reschedule`, `PATCH /jobs/{key}/schedule`; reverts on next boot under `on-conflict=override` |
| Pause / resume per job, cluster-wide | Implemented | `JobStore#pause` / `#resume`; on-demand still runs while paused |
| Born paused (`startPaused`) on first registration only | Implemented | `JobStore#upsert` initialises `paused` only at birth |
| Priority, five levels, used as claim ordering | Implemented | `io.mohs.core.execution.Priority` |
| Priority aging / starvation prevention | **Not present**, documented risk | `Priority` Javadoc records that `BACKGROUND` can starve |

## Execution

| Capability | Status | Evidence |
| --- | --- | --- |
| Sharded claim, 64 fixed shards, FNV-1a over the execution id | Implemented | `io.mohs.engine.Shards` |
| Leaderless shard assignment derived from live node ids | Implemented | `Shards.ownedBy` |
| Adaptive poll with exponential backoff to a ceiling | Implemented | `Engine#runLoop`, `Engine#nextBackoff` |
| Same-JVM wake-up on enqueue, after commit | Implemented | `Engine#signalWorkScheduled` |
| Cross-node event-driven wake-up (LISTEN/NOTIFY) | **Not present**: implemented, measured, withdrawn | `Engine` class Javadoc |
| Sleep shortened by the nearest armed trigger | Implemented | `Engine.cappedByNextFire` |
| Claim bounded by dispatch headroom | Implemented | `Engine#claimLaps` |
| Several claim rounds per tick under backlog | Implemented | `mohs.engine.claim-rounds` |
| Group commit for completions, 256 results or 5 ms | Implemented | `io.mohs.engine.CompletionBatcher` |
| Opt-out to synchronous per-result completion | Implemented | `mohs.engine.completion-flush-on-every-result` |
| Named runners: IO (virtual threads plus semaphore) and CPU (bounded pool) | Implemented | `MohsRunner`, `MohsExecutors`, `RunnerRegistry` |
| Backpressure by rejection, never an unbounded queue | Implemented | `setRejectTasksWhenLimitReached(true)`; `AbortPolicy` on CPU pools |
| Per-job concurrency cap derived from live leases | Implemented | `LeaseStore#countByJob`, `Engine.Admission` |

## Reliability

| Capability | Status | Evidence |
| --- | --- | --- |
| Node heartbeat plus node lease (`epoch`, `expires_at`) | Implemented | `NodeStore#heartbeat`, table `mohs_nodes` |
| Reaper reclaims dead nodes' leases through the retry budget | Implemented | `Engine#reapOrphanedLeases` |
| Fencing token `(node_id, epoch, attempt)` on every completion | Implemented | `LeaseStore#complete` |
| Self-diagnosed lease expiry bumps the local epoch | Implemented | `Engine#renewNodeLease` |
| Stray-lease reconcile for work lost between claim and dispatch | Implemented | `Engine#reconcileOwnStrayLeases` |
| Watchdog bound: release ownership of an over-running execution | Implemented, off by default | `mohs.engine.watchdog-timeout` |
| Retry with exponential backoff and full jitter | Implemented | `RetrySchedule`: base 1 s, cap 10 min |
| Manual retry of a `FAILED` execution, bypassing the budget | Implemented | `Mohs.retry`, `WorkQueue#rearmForManualRetry` |
| Cooperative cancellation, flag plus interrupt where applicable | Implemented | `CancellationSignal`, `JobContext#cancellationRequested()` |
| Per-attempt timeout | Implemented | `@MohsJob(timeout = "PT5M")` |
| Coordinated graceful shutdown with drain grace and escalation | Implemented | `Engine#stop`, `MohsEngineLifecycle` |
| Dead-letter queue | **Not present** | Exhausted retries end as terminal `FAILED` rows in history |

## Integration surface

| Capability | Status | Evidence |
| --- | --- | --- |
| `Mohs` Java facade: schedule, batch, define, query, control | Implemented | `io.mohs.core.Mohs` |
| `ExecutionListener` (Observer; best-effort, asynchronous) | Implemented | `ExecutionEventPublisher` |
| `ExecutionInterceptor` (Chain of Responsibility, on the attempt thread) | Implemented | `Dispatcher#runInterceptorChain` |
| `ActorResolver` SPI for attributing REST mutations | Implemented | `io.mohs.rest.ActorResolver`, default `HeaderActorResolver` |
| Operational REST API v1 | Implemented, **off by default** | `mohs.api.enabled` |
| React dashboard served at `/mohs-ui` | Implemented, **opt-in dependency** | `mohs-ui` plus `MohsUiAutoConfiguration` |
| Server-sent snapshot stream for the dashboard | Implemented | `OverviewStreamBroadcaster` |
| `GET /batches/{id}` route | Implemented | `MohsRestAutoConfiguration#mohsBatchesController` |
| Micrometer metrics under `mohs.*` | Implemented, always on | `EngineMetrics` |
| Spring Boot Actuator health indicator | **Implemented** | `MohsHealthIndicator` under the `mohs` key, contributed when the host brings the actuator (`MohsHealthAutoConfiguration`); it never touches the database |
| OpenAPI / Swagger document | **Not present** | No springdoc dependency |

## Persistence

| Capability | Status | Evidence |
| --- | --- | --- |
| PostgreSQL, MySQL 8.0+, SQL Server dialects | Implemented, production tiers | `io.mohs.store.jdbc.delegate` |
| H2 dialect | Implemented, test/dev tier only, WARN at boot | `MohsAutoConfiguration#mohsJdbcDelegate` |
| Schema installed by the operator, never by the library | By design | `schema-<dialect>.sql` and the `V*.sql` chain, both shipped in the jar |
| Idempotent baseline that adopts an existing hand-created schema | Implemented | `V1__mohs_baseline.sql` per dialect |
| Any externally managed schema | The only mode there is | Mohs executes no DDL at all |
| UUIDv7 primary keys everywhere; no sequences, no UUIDv4 | Implemented; **convention, not enforced** | `io.github.robsonkades.uuidv7.UUIDv7` at every call site |
| Automatic retention or purge of execution history | **Implemented, opt-in** | `mohs.engine.history-retention` (default `0s`, keep forever) sweeps terminal history hourly on the tick; `mohs_idempotency` is pruned hourly by `idempotency-retention` — see [data lifecycle](../06-data/data-lifecycle.md) |
