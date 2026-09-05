# Extensibility

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## The supported extension points

The following interfaces are intended for application implementations:

| Extension point | Type | How you register it | Runs on |
| --- | --- | --- | --- |
| `ExecutionListener` | `@FunctionalInterface` | A Spring `@Bean` | A dedicated virtual thread, asynchronously |
| `ExecutionInterceptor` | `@FunctionalInterface` | A Spring `@Bean` | **The attempt's own thread**, around the handler |
| `RetryPolicy` | `@FunctionalInterface` | A named Spring `@Bean`, referenced by `retryPolicy` | The failure path on the dispatch or engine thread |
| `ActorResolver` | Interface, `@ConditionalOnMissingBean` | A Spring `@Bean` | The request thread |

Everything else in the public API is either sealed (and therefore not implementable) or explicitly
**not** an extension point.

## `ExecutionListener` — observe

```java
@Bean
ExecutionListener alerting(AlertService alerts) {
    return event -> {
        switch (event) {
            case Failed f when f.attemptsExhausted() ->
                    alerts.page("job " + f.jobKey().value() + " exhausted its retries", f.error());
            case BatchCompleted b when b.failed() > 0 ->
                    alerts.warn(b.name() + ": " + b.failed() + " of " + b.total() + " failed");
            default -> { }
        }
    };
}
```

`ExecutionEvent` is **sealed**, so an exhaustive switch is possible and adding a variant can require updating an exhaustive switch when recompiling. A switch with
a `default` branch, like the example above, deliberately ignores events it does not handle.

### The contract, stated precisely

| Property | Value |
| --- | --- |
| Delivery | Asynchronous, best-effort |
| Concurrency | One virtual thread per publication, capped by `mohs.engine.event-concurrency` (16) |
| A throwing listener | Caught and logged; **never** affects the job, nor other listeners |
| A saturated executor | The event is **dropped** with a WARN — the observation pipeline never exerts backpressure on the control pipeline |
| Ordering | **None**, even for one execution. `RetryScheduled` may arrive before the `AttemptFailed` that causally precedes it |
| Cluster scope | Events are published by **the node that performed the transition**. A listener sees only its own node's events |
| Publication timing | Only after the completion's fence held. A result whose fence lost publishes nothing |
| Reclaim events | The reaper publishes through the same pipeline, so node-death outcomes reach listeners identically |

### Two hazards in `Failed`/`AttemptFailed`

Both are recorded in the source and both matter in practice:

1. **`equals`/`hashCode` are identity-based** on the `error` component, because `Throwable` does not
   override them. Two events describing the same failure are never `equals()`. Do not use these
   records as a deduplication key — the identity of the fact is `executionId + attempt`.
2. **The `Throwable` is shared and mutable.** Treat it as read-only: calling `addSuppressed`,
   `initCause` or `setStackTrace` corrupts what other listeners are still going to observe. There is
   no defensive copy for a `Throwable`, which is why this is a written rule rather than an enforced
   one.

### When **not** to use a listener

For anything that must happen. Delivery is best-effort and JVM-local. **Enqueue the continuation
inside the handler's own transaction** — the transactional outbox:

```java
@MohsJob(id = "settle-order")
@Transactional
void settle(OrderId id) {
    orders.settle(id);
    mohs.schedule(NOTIFY_CUSTOMER, new Notify(id)).now();   // joins THIS transaction (NESTED)
}
```

## `ExecutionInterceptor` — participate

```java
@Bean
ExecutionInterceptor mdcContext() {
    return (ctx, chain) -> {
        MDC.put("executionId", ctx.executionId().value());
        MDC.put("jobKey", ctx.jobKey().value());
        MDC.put("attempt", String.valueOf(ctx.attempt()));
        try {
            chain.proceed();
        } finally {
            MDC.clear();
        }
    };
}
```

| Property | Value |
| --- | --- |
| Thread | The attempt's own — so `ThreadLocal`/MDC works, and `ScopedValue` is the intended modern form |
| Ordering | Composed backwards, so the **first** interceptor in the injected list is the outermost |
| A throwing interceptor | **Is** a failure of the attempt and follows the normal retry flow. Whatever sits on the critical path takes part in the outcome |
| Interrupt window | An interceptor runs **inside** the interrupt window, so it can be interrupted by a timeout or by shutdown escalation |
| Typical uses | MDC, tracing spans, security context propagation, per-attempt metrics |

This is the difference from a listener in one sentence: **an interceptor is on the critical path and
therefore accountable; a listener is not on it and therefore powerless.**

## `ActorResolver` — attribute

```java
@Bean
ActorResolver authenticatedActor() {
    return request -> {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String name = auth == null ? ActorResolver.ANONYMOUS : auth.getName();
        return "scheduler".equalsIgnoreCase(name.strip()) ? "svc:" + name : name;   // see below
    };
}
```

The default `HeaderActorResolver` reads `X-Mohs-Actor` and falls back to `"anonymous"`. Replacing it
is the intended path once real authentication exists — this is exactly what the `@ConditionalOnMissingBean`
is for.

**One hard rule**: never return `Execution.SCHEDULER_ACTOR` (`"scheduler"`), in any casing. It is
the engine's reserved name; `ScheduleCommand#as` rejects it, and a rejection coming from this SPI
would surface as a generic 500 rather than the 400 the boundary gives. An authenticated principal
carrying that name — a `scheduler` service account — must be mapped to another identifier here.

The default implementation also validates length (255, the column's ceiling) and **denies a specific
character family**, which is worth understanding before writing your own:

> The actor is the only fully caller-controlled string Mohs persists and writes into the audit
> trail. Without validation, ANSI sequences and bidi characters crossed into the terminal of whoever
> reads that trail at 3 a.m.

The deny-list is a *threat* list rather than an allow-list of shape, and the reason is recorded:
`\p{Print}` in Java is pure US-ASCII, so the first version rejected "José" in NFD, the typographic
em dash, and Arabic-Indic digits — a person's name turning into a 400. It denies C0/C1 controls,
line/paragraph separators, **all** bidi controls (including U+061C, which falls outside the
200E/202x/206x ranges), zero-width invisibles, and the Tags block. ZWJ/ZWNJ are deliberately left
in, being legitimate in Persian and Indic scripts.

## Resource beans

Not extension points in the SPI sense, but the way you extend Mohs' vocabulary:

| Bean type | Effect |
| --- | --- |
| `@Bean MohsRunner` | Adds or overrides a named runner. A name declared in **both** `mohs.runners.*` and a bean is a boot error |
| `@Bean RateLimit` | Declares a cluster-wide limit. Same duplicate rule |
| `@Bean ExecutionWindow` | Declares an exclusion window. **Code-only** — there is no property form |
| `@Bean` with `@MohsJob` methods | Declares jobs |

## What you may **not** extend

| Interface | Reason |
| --- | --- |
| `Mohs`, `MohsLifecycle`, `ScheduleCommand`, `Batch`, `BatchBuilder`, `JobContext` | Non-sealed only because the implementation lives in another module and the project does not use a JPMS `permits` clause across them. **They may gain methods in minor releases.** Implementing them breaks with `AbstractMethodError` on the first new method |
| `JobStore`, `WorkQueue`, `LeaseStore`, `HistoryStore`, `NodeStore`, `BatchStore`, `RateLimitStore`, `TriggerFirer`, `StoreTransactions` | Internal ports in `io.mohs.engine`. Not published as an SPI, and the auto-configuration registers concrete implementations with no `@ConditionalOnMissingBean` |
| `JdbcDelegate` | Internal, but a bean of your own replaces it: `mohsJdbcDelegate` is `@ConditionalOnMissingBean`. Adding a *supported* database is still a change to the library. The named parameters are the contract: `fencedLeaseDelete()` must match `execution_id`, `node_id`, `epoch` **and** `attempt_number` — a statement that ignores a bound parameter runs without error and keeps a weaker fence |
| Any sealed type (`Schedule`, `ExecutionEvent`, `JobSpec`, `PolicySpec`, `ScheduleView`) | Not implementable by consumers; additions to a sealed hierarchy may require updating exhaustive switches |

To test handlers, use `io.mohs.test` (`MutableClock`, `InMemoryJobStore`) rather than implementing
`Mohs` yourself.

## `@OnExecution` and `RetryPolicy`

Both were names without behaviour in an earlier build; both are delivered now.

| Name | Status | Behaviour |
| --- | --- | --- |
| `@OnExecution(job = …, event = …)` | **Implemented** | The annotated method becomes a subscriber of the engine's event stream, filtered by job and event type, with the same asynchronous best-effort contract as `ExecutionListener`. An impossible signature or filter fails the boot |
| `retryPolicy` (a bean name on a job) | **Implemented** | The named `RetryPolicy` bean decides the delay on both failure paths — a handler that threw and a lease reclaimed from a dead node — and replaces the `retries` budget while it returns one. A bean that does not exist fails the boot |

## How to add a new database dialect

Not a consumer extension, but the shape is worth recording because the design supports it cleanly:

1. Implement `JdbcDelegate`. **Every statement method is abstract**: write out all 66, even the ones
   that will read the same as PostgreSQL's. That includes the clock — `nowQuery()` and `readNow()`
   are a pair, and answering one without the other is the bug they exist to prevent: state whether
   your server's `now` carries a zone, and if it does not, ask it for UTC outright.
   `selectReadyCandidates` and `claimReady` carry defaults, but those are the claim *algorithm*,
   not SQL.
2. Add `src/main/resources/schema-<dialect>.sql` — the installer, idempotent, so re-running it against
   an existing database is a no-op.
3. Add `src/main/resources/io/mohs/store/jdbc/migration/<dialect>/V1..Vn` — the delta chain for an
   operator upgrading a database that already has an older schema. Also idempotent.
4. Add the enum value to `MohsProperties.Jdbc.Dialect` and the branch in
   `MohsAutoConfiguration#mohsJdbcDelegate`.
5. Add a Testcontainers `*TestSupport`, a `Schema<Dialect>RoundTripTest` (the stores against a real
   database) and a `JdbcWorkQueue<Dialect>Test`. If the database gets a delta chain, add the
   structural guardian too — `SchemaPostgresChainMatchesInstallerTest` is the model: it builds one
   database from the installer and one from the chain and compares the structure, which is the only
   thing keeping the two copies of the schema honest.

The declared constraint is that **the claim's contract is identical in all dialects**: the returned
list comes back in `(priority, visible_at)` order, and the queue-removal plus ownership-insert are
atomic.
