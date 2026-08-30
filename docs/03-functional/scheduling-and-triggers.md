# Scheduling and triggers

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

## The three schedule kinds

`Schedule` is a sealed interface with exactly three variants, so the engine switches exhaustively
and a fourth kind would be a compilation error at every use site.

| Variant | Semantics | `next_fire_at` behaviour |
| --- | --- | --- |
| `CronSpec(expression, zone)` | Quartz-style, **seconds-first**, evaluated in the given zone | Always armed while the job is active |
| `IntervalSpec(interval, afterFinish=false)` | **Fixed rate** — the next firing is anchored to the *scheduled* time, so the series never drifts | Always armed |
| `IntervalSpec(interval, afterFinish=true)` | **Fixed delay** — anchored to the *end* of the previous execution | Disarmed (`NULL`) while an occurrence is in flight; rearmed by the completion |
| `OnDemandSpec()` | No automatic trigger | Always `NULL` |

`OnDemandSpec` is explicit rather than "no schedule defined", so that a job without a cron or an
interval is a deliberate choice rather than an omission.

## Cron

Parsing and next-occurrence computation live in `io.mohs.cron`, vendored from
`org.springframework.scheduling.support`. It supports the Quartz extensions `L`, `W` and `#`.

**The zone is mandatory.** `CronSpec` requires a `ZoneId`, and the annotation validates that `zone`
is present whenever `cron` is. A cron expression is never evaluated in the JVM's default zone.

### Two rules the expression alone does not guarantee

`NextFireCalculator#nextCronFire` adds both:

**1. Strict progress.** `CronExpression.next()` promises an instant strictly after the seed, and for
day-of-month `L-n` that promise has been broken before. Its consumer, `FiringPlanner.planSeries`,
*iterates* over the result: without progress it would materialise the same occurrence up to the cap
of 1,440, return `next_fire_at` unchanged, leave the trigger due forever, and re-execute the job on
every tick — with nothing in the log. The root cause is fixed in `QuartzCronField`; the guard exists
because an invariant consumed by a loop cannot depend on the producer merely behaving. It throws a
message naming the expression, which the firing path routes as a per-job error without taking down
the sweep of the others.

**2. DST fall-back suppression.** At the end of daylight saving the same wall-clock time happens
twice with different offsets, and the cron matches both — a "daily 02:00" job would run twice on the
transition day. The repetition is suppressed: **a loss is worse than a delay, and duplicating a
daily close is the worst possible outcome.**

The suppression is careful about a subtle case. The repetition is only real work when the series is
uniformly at least as dense as the shift, on **both** sides of the ambiguous slot. Looking only
forward confuses "hourly cron" with "twice a day in adjacent hours": `0 0 2,3 * * *` has a 1 h step
*after* 02:00 and no occurrence *before* it, and would have duplicated the close.

**The spring-forward gap is deliberately not compensated.** A time that does not exist does not
fire, and the next occurrence is the following day. This is an explicit divergence from Quartz.

### Expression caching

Parsed expressions are cached in a `ConcurrentHashMap` with a 10,000-entry ceiling. The ceiling
exists because the key is operator-controlled — `PATCH /jobs/{key}/schedule` accepts a new
expression — so a loop of reschedules with distinct expressions would grow the map forever. Clearing
everything on overflow is acceptable: a miss costs a parse, not a query.

## Intervals

| | Fixed rate (`afterFinish = false`) | Fixed delay (`afterFinish = true`) |
| --- | --- | --- |
| Anchor | The scheduled firing time | The end of the previous execution |
| Drift | None — the series stays on its original anchor even after a misfire skip | Accumulates by design |
| While an occurrence runs | The next one is already armed and may overlap | The trigger is `NULL`; nothing new is materialised |
| Rearmed by | The firing itself | The **completion transaction** (`CompletionResult.rearmNextFireAt`) |

### The fixed-delay chain, and its three cures

Because a fixed-delay trigger is disarmed while an occurrence is alive, the chain dies if that
occurrence ever reaches a terminal state without going through the completion path. Three cures
exist, each for a different way that can happen:

| Cure | Where | Covers |
| --- | --- | --- |
| Rearm inside the completion transaction | `Dispatcher#rearmNextFireAt` → `LeaseStore#complete` | The normal end of an occurrence, on success, failure or cancel |
| Rearm on reclaim | `Engine#rearmFor` | A node died mid-occurrence; a zombie's "end" is unknown, so the reaper's observation instant anchors the next firing |
| Rearm after cancelling a queued occurrence | `MohsImpl#rearmAfterFinishChain` | The occurrence was cancelled while still in the queue and never ran |
| Heal a disarmed trigger at upsert | `JobStore#upsert` | Anything the three above missed — the boot or a `define` rearms a recurring schedule whose trigger is `NULL` |

All of them are guarded: `armNextFire` writes only when the column `IS NULL`, so a cure can never
clobber a series that is already live. And all of them apply **only to scheduler occurrences** — a
cancelled *manual* execution is not the chain.

One residual window is declared: a crash between `cancelQueued` and the rearm leaves the chain
disarmed until the next upsert-time cure. Making it transactional would require leaking the storage
boundary up into the facade.

## Firing a due trigger

```mermaid
sequenceDiagram
    participant Loop as Engine tick
    participant Store as JobStore
    participant Planner as FiringPlanner
    participant Firer as TriggerFirer
    participant DB as Database

    Loop->>Store: findDueRecurring(now, limit 500)
    Note over Store: next_fire_at <= now,<br/>excluding paused, orphaned, retired;<br/>oldest first
    Store-->>Loop: due jobs
    loop each due job
        Loop->>Planner: plan(schedule, misfire, observed next_fire_at, now)
        Planner-->>Loop: occurrences + new next_fire_at + misfired flag
        Loop->>Firer: fire(key, observed, new, occurrences, payload, now)
        Firer->>DB: UPDATE ... WHERE next_fire_at = :observed AND retired = false
        alt CAS won
            Firer->>DB: INSERT mohs_execution (one per occurrence)
            Firer->>DB: INSERT mohs_ready (visible_at = scheduledAt)
            Firer-->>Loop: true
        else CAS lost — another node fired it
            Firer-->>Loop: false (routine, not an error)
        end
    end
```

Key properties:

- **The CAS is the cluster-wide mutual exclusion.** Only the node that advances `next_fire_at`
  inserts occurrences, and because the advance and the insert are atomic, a crash between them can
  neither lose nor duplicate an occurrence. This is why occurrences carry **no `Idempotency-Key`**:
  that key would be subject to a retention window, while atomicity is not.
- **The CAS compares a value read *from the column*.** Never an instant computed in the JVM that
  never went through the database — temporal precision does not round-trip identically across four
  dialects.
- **`retired` is in the CAS predicate.** A `Mohs.remove` between the sweep and the CAS has already
  cancelled what was queued; inserting occurrences after that would leave zombies until a possible
  resurrection.
- **Firing runs *before* the claim on the same tick**, so a freshly materialised occurrence is
  claimable without waiting for another poll.
- **`FIRE_LIMIT = 500` triggers per tick.** A boot after a long downtime must not become an
  unbounded sweep; the surplus stays due and drains, oldest first.
- **One job's failure does not stop the sweep**, and does not stop the tick's claim.

`created_at` on a materialised occurrence is `now`, not `scheduledAt`: it is the instant the row is
*born*, and in a `FIRE_ALL_MISSED` replay `scheduledAt` is in the past — history would otherwise
record a birth that did not happen then. `visible_at` in the queue is `scheduledAt`, so the
occurrence enters the queue already due.

## Misfire

A **missed** occurrence is one older than `mohs.engine.misfire-threshold` (default 60 s). An
occurrence due *within* the threshold fires late under any policy — a delay of up to one poll
interval is normal operation, not a failure. Only the batch of genuinely missed occurrences answers
to the policy.

| Policy | Series schedule (cron / fixed rate) | Fixed delay |
| --- | --- | --- |
| `IGNORE` (default) | Skip forward to the first non-missed occurrence, then materialise from there | Materialise nothing; rearm at `now + interval` |
| `FIRE_NOW` | Skip forward, plus **one** compensating occurrence at `now` | One occurrence at `now` |
| `FIRE_ALL_MISSED` | Replay every missed occurrence from `next_fire_at` onward, capped | Same as `FIRE_NOW` — only one occurrence can be missed in an end-to-start chain, so the two policies coincide |

Details that matter:

- **Skipping never walks the missed occurrences one by one.** Cron recomputes directly from the
  threshold boundary; fixed rate jumps by integer division, **preserving the series anchor** — the
  next regular occurrence stays on the original series and is never re-anchored to the tick's
  instant.
- **The cap is 1,440 occurrences per job per cycle** (`FiringPlanner.MAX_OCCURRENCES_PER_CYCLE`),
  applied to *every* materialisation and not just to replay: a pathological schedule (a millisecond
  interval) must not turn one tick into an unbounded insert. When capped, `next_fire_at` stays due
  and the surplus **drains over following ticks — never discarded**.
- **A compensation reserves its own slot in the cap**, and is skipped when the series already placed
  an occurrence exactly at `now` — otherwise two executions would share a `scheduled_at` and nothing
  in the schema would stop both from running.
- **A misfire logs a WARN** naming the job, the observed `next_fire_at`, the tick instant, the
  policy applied, how many occurrences were materialised and the new `next_fire_at`.

### A 1 ms slack, and why

`FiringPlanner#plan` rejects a trigger that is not due — but allows `now + 1ms`. That slack is the
**column's resolution**, not clock tolerance: `DATETIME2` (100 ns) and `DATETIME(6)` (microseconds)
round the nanosecond-precision value the calculation produced, and the `SELECT` may return a row
whose read value lands marginally after the raw `now`. Without the slack, a non-event becomes a
`log.error` — and a benign ERROR is what erodes trust in the log at 3 a.m.

## Pause, resume and reschedule

| Operation | Scope | Effect |
| --- | --- | --- |
| `Mohs.pause(jobKey)` / `POST /jobs/{k}/pause` | Cluster-wide, per job | Suspends automatic firing. **Manual scheduling still works.** A no-op for an unknown job |
| `Mohs.resume(jobKey)` / `POST /jobs/{k}/resume` | Cluster-wide, per job | Re-enables firing |
| `Mohs.reschedule(jobKey, schedule)` / `PATCH /jobs/{k}/schedule` | Cluster-wide, per job | Rewrites the schedule **and** rearms the trigger from the clock in the same write |

`reschedule` writes `next_fire_at` unconditionally, on purpose: *an explicit reconfiguration beats a
concurrent firing.* It is guarded by `retired` — a retired job is invisible to the whole API. An
unrealisable schedule (a syntactically valid cron that never fires) raises `IllegalArgumentException`,
which the REST layer turns into a 422 that teaches.

**A rescheduled job reverts on the next boot** under the default `on-conflict=override` — the
scanner restores the code's version with a logged diff. This is an *emergency* lever, and every
`PATCH` response says so in a `notice` field.

## Resume produces a burst — by design

When a paused job is resumed, `FiringPlanner` materialises every occurrence due within the misfire
threshold at once. That is **not** misfire: an occurrence due within the threshold fires late under
any policy, `IGNORE` included. The operational consequence is a burst proportional to the pause
length, and `RecurringTriggerScenario` in `mohs-benchmark` asserts that behaviour explicitly rather
than pretending resume merely picks the cadence back up.
