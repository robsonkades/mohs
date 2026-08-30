# Rate limits and execution windows

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

Two named resources that gate whether a job may be claimed at all. Both are referenced **by name**
from `JobDefinition`, and for both an unknown name **blocks the job** — fail-safe.

## Rate limits

A `RateLimit(name, max, window)` is a **cluster-wide** cap on a job's firing rate, enforced by a
token bucket in `mohs_rate_limits`.

| Property | Value |
| --- | --- |
| Capacity | `max` tokens |
| Refill | One token every `window / max`, applied continuously |
| Scope | Cluster-wide, not per node |
| Bounds rate, not concurrency | For concurrency use `maxConcurrentExecutions` |
| A freshly declared limit | Is born with a **full** bucket — it has no consumption history to charge for, and starting empty would make the first job wait a whole window for a limit that was never exceeded |
| Reading the balance | Consumes no token |

### Declaring one

Two sources, both assembled at boot by `MohsRateLimits`:

```java
@Bean
RateLimit smtp() { return new RateLimit("smtp", 100, Duration.ofMinutes(1)); }
```

```yaml
mohs:
  rate-limits:
    smtp:
      max: 100
      window: 1m
```

Both fields are mandatory in the property form: a half-specified limit has no defensible default —
`max` without `window` is not a rate.

Then reference it: `@OnDemandJob(id = "send-invoice", rateLimit = "smtp")`.

### The refillability rule

`RateLimit.requireRefillable(max, window)` is a **static method shared by the record's constructor
and the REST `PATCH` body**, deliberately not two copies that would diverge by the third edit:

| Check | Reason |
| --- | --- |
| `max >= 1` | |
| `window` positive | |
| `window >= max` nanoseconds | The refill interval `window / max` must be representable. A truncation to zero would make `dividedBy` produce `Duration.ZERO`, and a division by it inside the claim would bring down the **entire claim round** — including jobs with no rate limit at all |

### Two-phase consumption

The claim reads and charges in two separate steps, and that separation is the whole performance
story:

```mermaid
sequenceDiagram
    participant Loop as Engine
    participant RL as RateLimitStore
    participant DB as mohs_rate_limits

    Note over Loop: phase 1 — before the claim, per job
    Loop->>RL: available(name, now)
    RL->>DB: pure SELECT, no lock, no write
    DB-->>RL: balance with refill applied in memory
    RL-->>Loop: n tokens

    Note over Loop: the claim runs; executions are now owned

    Note over Loop: phase 2 — after the CAS, at the transaction's tail
    Loop->>RL: charge(name, granted, now)
    RL->>DB: guarded UPDATE (up to 3 CAS attempts)
    DB-->>RL: true / false
    alt false — another node took the balance between the phases
        Loop->>Loop: undo the round: requeue everything of this job
    end
```

| Phase | Cost | Why it is shaped this way |
| --- | --- | --- |
| `available` | A pure read: no lock, no write, no serialisation cost | It allows deciding the batch's admission without holding the row for the whole claim |
| `charge` | A guarded `UPDATE` at the **end** of the claim transaction | The row lock is born here and dies at the commit, so the serialisation window is the transaction's *tail* rather than the whole transaction. Measured improvement: 2.3× at 4 clients, 3.5× at 8 |

**It charges what was claimed, not what was admitted**, so a token does not burn on a candidate that
lost the job's mutex.

**A `false` from `charge` must undo the round.** The executions have already been claimed, and
delivering them without a token would be over-delivery — the one unacceptable violation of the
contract.

### Isolation requirement

`charge` requires **`READ COMMITTED`**. The implementation re-reads the row between CAS attempts;
under `REPEATABLE READ` the re-read would return the same snapshot, the attempts would fail
identically, and the retry would become an expensive no-op. `JdbcWorkQueue` guarantees this
explicitly with `REQUIRES_NEW` + explicit isolation. Any other caller inheriting a `@Transactional`
transaction from the host must guarantee the same.

Three CAS attempts, because the cost is asymmetric: each attempt costs two round trips, while giving
up costs the whole claim round.

### The lock timeout

`BUCKET_LOCK_TIMEOUT = 2s` is the **only unconditional wait on the claim path** — the candidate
selection's `SKIP LOCKED`/`READPAST` skips what is locked and never waits.

The ceiling exists because one stuck node holding the row would delay *other* nodes' ticks — and the
tick is what carries the heartbeat that renews the node lease. Contention would become a **false
positive of death**, and the reaper would reclaim executions that are still running: work duplicated
by the very mechanism meant to protect the external resource.

On expiry a `QueryTimeoutException` surfaces (measured: H2 SQLState 50200 at 2,013 ms; PostgreSQL 18
57014 at 2,022 ms) — **not** a `CannotAcquireLockException`, because Spring's translator sends
statement timeout and deadlock to sibling branches of the hierarchy. Both are caught explicitly: the
round is lost, never the heartbeat.

Two seconds assumes a generous `lease-ttl` (30 s default). **A `lease-ttl` below about 10 s calls
for revisiting this value.**

### Runtime adjustment

`Mohs.adjustRateLimit(name, max, window)` / `PATCH /rate-limits/{name}`:

| Property | Behaviour |
| --- | --- |
| Creates a missing limit? | **No** — declaring is an act of boot, not of emergency. Returns empty (HTTP 404) with a message naming the property to set |
| The bucket | **Survives** the adjustment, its balance clamped to the new ceiling. Lowering the limit cuts future throughput; it does not give back what was already consumed |
| Durability | Holds until the next boot under the default `on-conflict: override` |
| Concurrency | Two operators adjusting the same limit at the same instant is last-write-wins, which is what a `PATCH` promises |

The boot-time upsert writes **the spec and only the spec**. Resetting the bucket there would make
every node coming up in a rolling deploy hand back a full bucket, turning a deploy into a burst.
Same reasoning as `paused` in `JobStore#upsert`: boot configuration governs the spec, never the
current state.

### Reading a limit

`available` in a snapshot means **tokens available now**, not "used". Whoever opens the dashboard
wants to know how much still fits, and "used" is not even a quantity a bucket has — refill is
continuous, with no window boundary at which to reset a counter.

The refill is applied **in memory at read time**. The row stores only the balance as of the last
charge, and showing that raw number would display an empty bucket long after it had refilled.
Writing at read time would be worse: it would turn a monitoring read into contention for the claim
hot path's lock.

### Cross-job cost

A claim round that fails its CAS is undone **entirely**, and that round may contain executions of
jobs with **no limit at all**. This is measured deliberately by `RateLimitCeilingScenario`, whose
second question is exactly "does the unlimited job pay for its limited neighbour?".

The ceiling criterion asserted there is the **token bucket's**, not a fixed window's: the legitimate
envelope of the k-th delivery is `t_k >= (k − max) × window/max`. Demanding "never more than `max`
in any sliding window" would be demanding a mechanism that was deliberately not chosen.

## Execution windows

An `ExecutionWindow(name, exclusions)` is a set of predicates over `Instant`. A job whose scheduled
time falls inside **any** exclusion does not fire.

```java
@Bean
ExecutionWindow businessHours() {
    return ExecutionWindow.named("business-hours")
            .excludeWeekends()
            .excludeDaily(LocalTime.of(22, 0), LocalTime.of(6, 0))   // crosses midnight
            .excludeDates(List.of(LocalDate.of(2026, 12, 25)))
            .build();
}
```

Then: `@RecurringJob(id = "report", cron = "...", zone = "...", window = "business-hours")`.

| Builder method | Semantics |
| --- | --- |
| `excludeWeekends()` | Saturday and Sunday, evaluated in **UTC** |
| `excludeDaily(from, to)` | The half-open interval `[from, to)`, in UTC. **Supports crossing midnight**: when `from` is after `to`, it reads as `[from, 24:00) ∪ [00:00, to)` rather than silently becoming a no-op. `from == to` is empty by definition, as any `[t, t)` is |
| `excludeDates(dates)` | Whole UTC days |
| `exclude(predicate)` | Anything else |

### Deliberate limitations

| Limitation | Detail |
| --- | --- |
| **Code only** | There is no property-based equivalent. Only a `@Bean ExecutionWindow` feeds the registry |
| **UTC only** | This version's predicates evaluate the `Instant` in UTC. Whether an exclusion should respect the *job's* zone is a decision for the engine consuming the window, not for the contract |
| **Equality is effectively identity** | `exclusions` is a list of lambdas, so two windows built from identical calls are never `equals()`. True value semantics would require modelling exclusions as sealed data — out of scope while nothing depends on equality |
| **Fail-safe on an unknown name** | A job referencing a window that does not exist is treated as **excluded**, with a WARN on every occurrence. A typo must not let a job slip past the intended exclusion |
| No lifecycle | Unlike a runner, a window owns no resource, so `ExecutionWindowRegistry` has no lifecycle at all |
| Duplicate names | A boot error |

## Where each guard applies

Both are evaluated **at claim time**, not at enqueue time and not at handler entry:

| Guard | Pre-claim (the inadmissible list) | Post-claim (`admit`) |
| --- | --- | --- |
| Window closed | Job excluded from the round entirely | Second line of defence — the whole job's share is rejected |
| Rate limit exhausted | Job excluded from the round | `min(allowed, available)` then an all-or-nothing charge |
| Concurrency cap full | Job excluded from the round | Trimmed to the remaining headroom |

The post-claim pass is the **authority**; the pre-claim filter is a churn optimisation that may be
truncated. A newborn job, or a round whose filter was truncated at the parameter ceiling, arrives at
`admit` with that as the only barrier.
