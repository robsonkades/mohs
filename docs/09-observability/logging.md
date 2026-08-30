# Logging

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

## The framework

SLF4J, with the host application's binding. Mohs configures **no** logging backend and ships no
`logback.xml` — an embedded library must not decide the host's logging configuration.

## Level distribution

Counted across production sources in `mohs-engine`, `mohs-store-jdbc`, `mohs-rest` and
`mohs-spring-boot-starter`:

| Level | Count | Character |
| --- | --- | --- |
| `WARN` | 47 | The dominant level, by design |
| `ERROR` | 12 | Reserved for "a human is needed" |
| `INFO` | 11 | Notable, correct state changes |
| `DEBUG` | 5 | Routine detail |
| `TRACE` | 0 | Not used |

The distribution is deliberate: severity reflects **operational meaning**, not the code path's
excitement. Most of what can go wrong in a distributed scheduler is recoverable and *should* be
noticed — which is exactly what WARN is for.

## Loggers

| Logger | Emits |
| --- | --- |
| `io.mohs.engine.Engine` | The tick's lifecycle: heartbeats, reaper, reconcile, drain, stop, shard warnings, clock anomalies |
| `io.mohs.engine.Dispatcher` | Attempt outcomes, retries, fence losses |
| `io.mohs.engine.CompletionBatcher` | Flush failures and fallbacks |
| `io.mohs.engine.ExecutionEventPublisher` | Listener exceptions, dropped events |
| `io.mohs.engine.BatchCompletionCallbacks` | Callback exceptions |
| `io.mohs.engine.ExecutionWindowRegistry` | Unknown window names |
| `io.mohs.engine.MohsImpl` | Manual retries |
| `io.mohs.engine.RunnerRegistry` | Runner lifecycle |
| `io.mohs.store.jdbc.MohsFlyway` | Migrations applied |
| `io.mohs.store.jdbc.DatabaseClock` | Clock skew and sync failures |
| `io.mohs.store.jdbc.JdbcRateLimitStore` | Unknown limit names |
| `io.mohs.rest.error.RestExceptionHandler` | Contention and unhandled exceptions |
| `io.mohs.rest.job.JobsController` | Runtime reschedules (audit) |
| `io.mohs.rest.ratelimit.RateLimitsController` | Runtime limit adjustments (audit) |
| `io.mohs.rest.overview.OverviewStreamBroadcaster` | SSE tick failures, client drops |
| `io.mohs.autoconfigure.*` | Boot-time warnings and definition diffs |

Recommended baseline:

```yaml
logging:
  level:
    io.mohs: INFO
    io.mohs.engine.Engine: INFO          # DEBUG adds per-trigger firing detail
    io.mohs.store.jdbc: INFO
```

Setting `io.mohs: WARN` in production is defensible, but you lose the INFO lines that bound a
shutdown's duration and record definitional drift.

## The messages that matter

### ERROR — a human is needed

| Message | Meaning and action |
| --- | --- |
| `engine loop died — this node stops claiming. Its node lease will expire and peers will reclaim the work still running here (at-least-once). Restart the instance.` | The loop thread caught a `Throwable` and is exiting. **Restart the node.** This is the message that exists because an `Error` used to escape silently, leaving `state` at `RUNNING` forever while `MohsLifecycle#state` and `GET /nodes` lied |
| `overview stream tick died with an Error — the periodic task is CANCELLED and every connected dashboard will silently freeze on stale data. Restart the application.` | `scheduleWithFixedDelay` cancels a throwing task permanently |
| `completion flush cycle failed unexpectedly — N result(s) fall to this node's stray-lease reconcile (or a peer's reaper if this node dies)` | Recovery is automatic, but the cause needs investigation |
| `dispatch of execution {} threw outside the normal failure paths` | The outcome *write* itself threw — the zombie's only trace |
| `could not record the completion of execution {} — its lease stands until a reaper reclaims it` | |
| `firing job '{}' failed — will retry next tick` | Per-job; the sweep continues |
| `engine tick failed — will retry next tick` | The whole tick threw |
| `onCompletion callback for batch {} threw` | The batch is complete regardless |

### WARN — recovery is automatic; you should know

| Message | Meaning |
| --- | --- |
| `node lease expired at {} while this node was stalled — epoch bumped to {}; peers may have reclaimed in-flight work` | **This node was dead to the cluster.** Their re-runs stand; this node's fenced completions are discarded |
| `clock moved backwards between heartbeats — the new lease promise ({}) is EARLIER than the previous one ({})… Check NTP, or mohs.time.mode/sync-interval` | Without this line, *"why did the cluster re-execute 60 jobs at 04:12?"* has no log connecting symptom to cause |
| `attempt {} of execution {} finished {} but the incarnation was no longer ours (reaper/requeue passed first) — result discarded` | The fence worked. Expected during recovery |
| `requeued N lease(s) this node was holding with no in-flight incarnation — work lost between claim and dispatch` | The stray-lease reconcile fired |
| `job '{}' missed occurrence(s) — next_fire_at was {} at tick time {}; misfire policy {} applied: N occurrence(s) materialized, next fire at {}` | A misfire |
| `drain grace period ({}) elapsed with N dispatch(es) still in flight (M still signallable) — signalling cancellation and interrupting them` | Shutdown escalated |
| `engine loop did not stop within {} — the in-flight wait below can still miss a dispatch…; work may be re-delivered` | A degraded shutdown |
| `execution {} exceeded its job timeout {}` / `exceeded mohs.engine.watchdog-timeout {} — ownership released` | |
| `this node owns no shard of 64 — it will never claim. The cluster has more RUNNING nodes than shards` | **Once per transition**, never per tick |
| `tick step '{}' failed — claim and dispatch continue this tick` | Paired with `mohs.tick.failed{step}` |
| `job references unknown execution window '{}' — treating as excluded (fail-safe) until fixed` | A typo blocks the job |
| `group completion flush of N result(s) failed — falling back to one completion per result` | |
| `event executor saturated — dropping {} for listener {}` | Best-effort delivery, by contract |
| `execution listener {} threw for event {} — ignored` | |
| `idle-gate probe failed — falling back to the full claim lap this tick` | |
| `clock skew {} exceeds threshold {}` | `database` time mode |
| `runner executor rejected execution {} — already leased, it will stand until a reaper reclaims it` | Backpressure working |
| The boot warnings | See [configuration](../07-configuration/configuration-reference.md#warnings-emitted-at-boot) |

### INFO — a notable, correct state change

| Message | Value |
| --- | --- |
| `draining: at least N dispatch(es) in flight, waiting up to {}` | **The most expensive stretch of a shutdown, and it used to have no trace at all** — operators saw the web server's graceful shutdown afterwards and blamed it for the whole time. The "at least" is literal, not modesty: with the loop still alive, a tick between the `runAsync` and the `inFlight.add` does not yet appear in the count |
| `engine stopped in {}` | Closes that interval |
| `job '{}' definition changed, code wins (on-conflict=override): <diff>` | The audit of a redeploy |
| `job '{}' rescheduled at runtime by '{}' to {}` | **The audit trail of a `PATCH`.** It logs what *this* actor asked for (the body), not the post-write snapshot — an audit trail records intent, and two concurrent `PATCH`es never swap authorship |
| `rate limit '{}' adjusted at runtime by '{}' to {}/{}` | Same |
| `execution {} manually rearmed for retry — rejoins the claim path bypassing the retries budget` | |
| `execution {} of job '{}' cancelled on attempt {} — cooperative cancellation honoured` | **INFO, not WARN, and with no stack trace**: an honoured cancellation is the system doing what the operator asked |
| `execution {} has a standing cancel request — cooperative cancellation signalled to the handler` | |
| `mohs schema migrated: N migration(s) applied up to version {}` | |
| `purged N stale node heartbeat row(s)` | |

### DEBUG

| Message | Note |
| --- | --- |
| `job '{}' fired N occurrence(s), next fire at {}` | Routine trigger detail |
| `stray-lease candidates all vanished before the requeue — completions won the race` | **DEBUG on purpose**: routine at high throughput, not a finding |
| `SSE client dropped during send` | The normal end of an SSE stream |
| `drain grace already escalated — re-signalling N in-flight attempt(s)` | |

## Conventions

| Convention | Detail |
| --- | --- |
| Parameterised messages | `log.warn("… {} …", value, exception)` — never concatenation |
| The exception is the **last** argument | So SLF4J logs the stack trace |
| A WARN or ERROR states the **consequence** | "…work may be re-delivered", "…will retry next tick", "…Restart the instance." |
| A WARN often states the **action** | "Check NTP, or mohs.time.mode", "reduce the node count or raise Shards.SHARD_COUNT", "align the two values" |
| Stack traces appear once | The WARN at a failure is the only place a handler exception's stack trace appears by default — `Attempt.error` keeps only the message, and the `Failed` event needs a registered listener |
| Persistent conditions are latched | The "owns no shard" WARN fires once per transition; at a 25 ms floor it would be ~40 lines/s per node |

## Correlation

| Available | Not available |
| --- | --- |
| `executionId` in every execution-related line | An automatically propagated request/correlation id |
| `jobKey` in claim, dispatch and completion lines | MDC populated by Mohs |
| `actor` on audit lines | Trace and span ids |
| `node_id` persisted per attempt in `mohs_attempt` | |

**Mohs populates no MDC.** The intended extension point is `ExecutionInterceptor`, which runs on the
attempt's own thread and is documented as "the place for MDC, tracing spans and context through
`ScopedValue`":

```java
@Bean
ExecutionInterceptor mdcContext() {
    return (ctx, chain) -> {
        MDC.put("executionId", ctx.executionId().value());
        MDC.put("jobKey", ctx.jobKey().value());
        MDC.put("attempt", String.valueOf(ctx.attempt()));
        try { chain.proceed(); } finally { MDC.clear(); }
    };
}
```

Note that this covers **handler execution only**. Engine-loop lines (claim, reaper, heartbeat) run on
the tick thread and carry no MDC.

## Sensitive data

| Data | In logs? |
| --- | --- |
| Job payloads | **No** — never logged |
| Exception messages | **Yes**, in the failure WARN |
| Actors | Yes, on audit lines |
| Execution and job ids | Yes |
| Database credentials | No |

**The hazard, recorded in the source**: `Execution` deliberately does not carry the payload, but a
handler exception's *message* travels from the `Failed` event into the log of anyone writing
`log.info("{}", event)`. That discipline is lost through the error-message door. **Do not put secrets
or personal data into exception messages in a job handler.**

The actor is the only fully caller-controlled string that reaches the log, and it is validated
against control characters, bidi overrides and invisibles precisely so it cannot tamper with how the
audit trail reads. See [security](../08-security/security-overview.md#input-validation).

## Structured logging

**Not configured by Mohs.** The host may add a JSON encoder, and Mohs' parameterised messages work
with one unchanged. The one Mohs-side accommodation is that U+2028/U+2029 are denied in the actor
header, because they forge a line in a JSON log consumer even where the CR/LF argument does not hold.
