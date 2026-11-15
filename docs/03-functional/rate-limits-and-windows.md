# Rate limits and execution windows

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

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
| Exact microseconds representable as BIGINT | Sub-microsecond or overflowing durations are rejected. Fractional credit is retained; refill does not divide Duration into an unrepresentable period. |

### Two-phase consumption

Before claim, WorkAdmission checks each named resource once per phase. After claim it groups work sharing a rate name, reads availability and charges the group's permitted share. Excess members return to READY with the same attempt. Positive concurrency caps have already been arbitrated inside the claim's job locks.

Charging and configuration each own a short READ_COMMITTED transaction with a two-second deadline. A charge retries up to three complete full-state CAS attempts. Refill uses fresh database time, checked arithmetic and retained fractional credit. API callers cannot supply a refill clock. A failed creation rolls back before resolving the winning bucket.

The availability read is advisory. Failed or unknown charge never authorizes dispatch. A thrown infrastructure failure leaves grants for fenced reconciliation. Tokens charged before a crash/dispatch failure are consumed and refill normally; there is no speculative refund. Claim and rate charging remain separate transactions.

### Runtime adjustment

`Mohs.adjustRateLimit(name, max, window)` and `PATCH /rate-limits/{name}` adjust an existing
declaration. An unknown name returns empty (HTTP 404). The bucket survives and its balance
is clamped to the new capacity. At the next application boot, declared rate-limit settings
are upserted again; `registration.on-conflict` also governs conflicts between declared and stored rate-limit specifications.

### Reading a limit

`available` means tokens available now. Reads apply refill in memory without consuming tokens
or updating the bucket. Token buckets permit bursts up to their capacity; they do not promise
at most `max` deliveries in every sliding window.

## Execution windows

An `ExecutionWindow(name, exclusions)` is a set of predicates over `Instant`. The engine tests the claim-time instant against these predicates. While **any** exclusion
matches, queued executions wait for admission; they are not cancelled or discarded.

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
| Concurrency cap full | Authoritative job lock prevents excess grants | No post-claim over-admission allowance |

The post-claim pass is the **authority**; the pre-claim filter is a churn optimisation that may be
truncated. A newborn job, or a round whose filter was truncated at the parameter ceiling, arrives at
`admit` with that as the only barrier.
