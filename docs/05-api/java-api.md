# Java API

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository (`mohs-api`)

The `Mohs` facade is the entire programmatic surface. One verb per operation, always over an
**existing** definition.

## Getting the facade

```java
@Service
class Invoices {
    private final Mohs mohs;
    Invoices(Mohs mohs) { this.mohs = mohs; }   // a bean registered by MohsAutoConfiguration
}
```

## Scheduling

### The typed path (preferred)

```java
static final JobRef<SendInvoice> SEND_INVOICE = JobRef.of("send-invoice", SendInvoice.class);

Enqueued receipt = mohs.schedule(SEND_INVOICE, new SendInvoice(4711))
        .priority(Priority.HIGH)
        .as("ops@acme.com")
        .idempotencyKey("invoice-4711")
        .now();
```

A `JobRef<T>` binds the key to the payload type at compile time, so an incompatible payload is a
compilation error rather than a runtime surprise.

### The string path

```java
mohs.schedule("send-invoice", payload).now();
```

The payload type is checked **at runtime** against the definition, producing a clear error rather
than a `ClassCastException`.

### The chain

`ScheduleCommand` has three pre-terminal steps and three terminals:

| Step | Effect |
| --- | --- |
| `.priority(Priority)` | Claim ordering. Default `NORMAL` |
| `.as(String actor)` | The audit trail. Rejects blank and rejects `"scheduler"` in any casing |
| `.idempotencyKey(String)` | Deduplication scoped to `(job, key)`. Not blank, and at most `ScheduleCommand.MAX_IDEMPOTENCY_KEY_LENGTH` (255) characters, the column's width — an `IllegalArgumentException` otherwise |
| `.now()` | Terminal — due immediately |
| `.at(Instant)` | Terminal — due at an absolute time |
| `.after(Duration)` | Terminal — due after a delay |

**A chain abandoned before its terminal never touches the database.** The pre-terminal steps carry
`@CheckReturnValue`, so an abandoned chain is a compilation warning rather than runtime silence —
the classic builder-without-`.build()` bug.

The **terminals are deliberately not annotated**. The annotation exists against an abandoned chain,
and what catches that are the non-terminal steps plus `schedule`/`batch`. On a terminal it would add
nothing and would tax the single most common line in the library:
`mohs.schedule(ref, payload).now();` is a correct statement, and a framework whose hello-world
produces a warning teaches the user to suppress the inspection — at which point they also lose the
warnings that matter.

### The receipt

Every terminal returns an `Enqueued`, with the `executionId` assigned and the enqueue written in the current transaction — and the same
record reaches the `ExecutionListener`s (and `@OnExecution(ENQUEUED)`) once it is: immediately, or
after the host's commit when the call ran inside a host transaction. It is a receipt,
never a `Future` of the result.

## Batches

```java
Batch batch = mohs.batch("nightly-invoices", b -> {
    for (Customer c : customers) b.add(SEND_INVOICE, new SendInvoice(c.id()));
});
batch.onCompletion(done -> log.info("{}: {} ok, {} failed", done.name(), done.succeeded(), done.failed()));
```

All-or-nothing. Inside a host transaction, durability depends on that transaction committing;
a rollback removes the batch and its members. See [batches](../03-functional/batches.md).

## Definitions

```java
mohs.define(JobDefinition.of("tenant-42-sync", TenantSync.class,
        spec -> spec.every(Duration.ofMinutes(5)).runner("io").retries(5)));

mohs.remove(JobKey.of("tenant-42-sync"));   // PROGRAMMATIC definitions only
```

`define` is an **upsert by `JobKey`**: redefining replaces the definitional part and never touches
the operational one (`paused`, `orphaned`, `next_fire_at`).

`remove` retires: it cancels future firings and preserves history. Calling it on a `@MohsJob` job
throws — remove the annotation instead, and the scanner marks it `ORPHANED` on the next boot. An
unknown job is a no-op.

## Control operations

| Method | Semantics |
| --- | --- |
| `pause(JobKey)` / `resume(JobKey)` | Cluster-wide, per job. A pause suspends automatic firing only; manual scheduling still works. Unknown job → no-op |
| `reschedule(JobKey, Schedule)` | Rewrites the schedule and rearms the trigger in one write. Returns the new snapshot, or empty for an unknown/retired job. Throws `IllegalArgumentException` for an unrealisable schedule |
| `cancel(ExecutionId)` | Cooperative. Returns the state right after the request — **not necessarily terminal**; empty for an unknown id |
| `retry(ExecutionId)` | Rearms a `FAILED` execution, bypassing the budget. Empty for an unknown id; throws `IllegalStateException` for a wrong state, a retired job, or a batch member |
| `adjustRateLimit(String, int, Duration)` | Adjusts an **already declared** limit. Empty when the name does not exist |
| `lifecycle()` | The node's `MohsLifecycle` |

## Reads

| Method | Returns | Pagination |
| --- | --- | --- |
| `findJob(JobKey)` | `Optional<JobSnapshot>` | — |
| `jobs()` | `List<JobSnapshot>` | None — bounded cardinality |
| `findExecution(ExecutionId)` | `Optional<Execution>` **with attempts** | — |
| `executions(ExecutionQuery)` | `List<Execution>` — a **summary**: `attempts()` comes back empty | Cursor, inside `ExecutionQuery` |
| `findBatch(String)` | `Optional<BatchSnapshot>` | — |
| `nodes()` | `List<NodeSnapshot>` | None |
| `runners()` | `List<RunnerSnapshot>` — **this node only**; the one read that touches no database | None |
| `rateLimits()` | `List<RateLimitSnapshot>`, ordered by name | None |
| `overview(Duration)` | `OverviewSnapshot` | — |
| `payloadType(JobKey)` | `Optional<Class<?>>` | — |

`ExecutionQuery` is a parameter object over the six filters and controls:

```java
new ExecutionQuery(jobKey, status, from, to, cursor, limit)   // all nullable except limit >= 1
```

Results are ordered by **descending id** (UUIDv7, so most recent first); with a cursor present, only
executions with `id < cursor` are returned.

> **Read-consistency note.** `overview` performs independent reads rather than one transactional
> cut, so executions transitioning during the query may disagree between the numbers (read skew).
> That is acceptable for polling, and a serialisable cut here would be cost without benefit.

## Lifecycle

```java
MohsLifecycle lifecycle = mohs.lifecycle();
lifecycle.state();               // CREATED → RUNNING ⇄ PAUSED → DRAINING → STOPPED
lifecycle.pause();               // this NODE stops claiming; not the same as pausing a job
lifecycle.resume();
lifecycle.drain(Duration.ofSeconds(30));   // stop claiming, wait for in-flight work
lifecycle.stop(Duration.ofSeconds(30));    // drain, then shut the loop down
```

This is **node-local**, and not to be confused with pausing a job, which is cluster-wide and per job.
Under Spring, `MohsEngineLifecycle` starts the engine automatically in `auto` mode.
`mohs.lifecycle.start-mode=manual` disables automatic startup; Spring still stops a running
engine during context shutdown.

Every transition is a guarded CAS: calling `pause()` from anything but `RUNNING` throws
`IllegalStateException` naming both states.

## The handler contract

```java
@OnDemandJob("import-file")
void importFile(ImportRequest payload, JobContext ctx) throws Exception { … }
```

`JobContext` exposes:

| Method | Notes |
| --- | --- |
| `jobKey()`, `executionId()` | The execution id is **stable across retries** |
| `attempt()` | 1-based; this is what changes on a retry |
| `scheduledAt()` | When it was due |
| `firedAt()` | When **this attempt** began dispatching — not the execution's claim instant, which is tens of milliseconds earlier under load |
| `cancellationRequested()` | Cooperative cancellation from a timeout, shutdown escalation, or a manual cancel |

`JobContext` is deliberately a plain interface rather than a fluent one: it lives on the hot path,
and a DSL here would only pollute stack traces.

## Extension beans

```java
@Bean ExecutionListener    listener();      // observe — best-effort, async, never affects the job
@Bean ExecutionInterceptor interceptor();   // participate — on the attempt's thread; throwing IS a failure
@Bean MohsRunner           runner();        // a named execution capability
@Bean RateLimit            limit();         // a cluster-wide throughput cap
@Bean ExecutionWindow      window();        // a firing exclusion window (code-only)
@Bean ActorResolver        actors();        // replaces the default header-based resolver
```

See [extensibility](../04-engineering/extensibility.md).

## The compatibility contract

This is the part to read before writing any integration code.

| Type | May you implement it? |
| --- | --- |
| `Mohs`, `MohsLifecycle`, `ScheduleCommand`, `Batch`, `BatchBuilder`, `JobContext` | **No.** They are non-sealed only because the implementation lives in another module and the project does not use a JPMS `permits` clause across them. **They may gain methods in minor releases**, and implementing them breaks with `AbstractMethodError` on the first new method. To test handlers, use `io.mohs.test` |
| `ExecutionListener`, `ExecutionInterceptor`, `RetryPolicy` | **Yes.** Functional interfaces intended for application implementations |
| `io.mohs.rest.ActorResolver` | **Yes.** The REST module's SPI |
| `JobSpec`, `PolicySpec`, `Schedule`, `ExecutionEvent`, `ScheduleView` | Sealed — not implementable by consumers. Adding a permitted subtype can require changes to exhaustive switches in consuming code |

## Constructing definitions

Use `JobDefinition.of(id, handlerType, spec -> ...)` to avoid coupling application code
to constructor arity. The record currently has 15 components; the former 13-argument
compatibility constructor is no longer present. There are no deprecated API elements
in this checkout.

## The test kit

```java
MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
clock.advance(Duration.ofMinutes(5));         // no Thread.sleep anywhere

InMemoryJobStore store = new InMemoryJobStore(clock);
```

Two documented divergences of `InMemoryJobStore` from the JDBC adapter: it does not heal a disarmed
trigger (an unreachable state without a `TriggerFirer`), and `remove` is a hard delete rather than a
soft retire — so a post-remove resurrection is a *birth* there, and `startPaused` applies again.
