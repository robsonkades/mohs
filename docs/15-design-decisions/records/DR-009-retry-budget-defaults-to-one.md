# DR-009: A job is born with a retry budget of one

## Status

Accepted

## Context

`retries` counts attempts **beyond the first**. The obvious default is 0 — "run it once, and if the
handler fails, that is the answer" — and that is what the API originally did.

It interacts badly with recovery. When a node dies, the reaper reclaims its leases and resolves each
one through the retry budget. With a budget of zero there is **nowhere to reschedule**, so the
reclaim becomes a terminal `FAILED`.

That is silently lost work, **in exactly the event the product promises to survive**.

## Decision

`retries` defaults to **1**, on `@MohsJob`, on both stereotypes, and in `PolicySpec`.

The annotation's own Javadoc states the reasoning:

> The delivery contract is only at-least-once when there is budget: without it, reclaiming an
> execution whose ownership was lost (a dead node, an expired lease, the shutdown window) has nowhere
> to reschedule and becomes a terminal `FAILED` — silently lost work, in exactly the event the product
> promises to survive. Anyone preferring at most one invocation per execution declares `retries = 0`
> deliberately, and accepts the loss under node failure.

## Consequences

### Positive

- **The advertised guarantee is the default behaviour.** At-least-once holds out of the box, rather
  than only for jobs whose author happened to set a budget.
- **The three ownership-loss paths all have somewhere to go**: a dead node's reclaim, a watchdog
  release, and a shutdown-grace escalation each produce a synthetic failure that reschedules.
- The opt-out is explicit and its cost is documented at the point of declaration.
- The reaper's metric distinguishes the outcomes — `attempts_exhausted` versus `job_retired` versus
  `retry` — so an operator can see when a zero budget is losing work.

### Negative

- **A handler with a side effect that must not repeat now runs twice by default.** The mitigation is
  the one this system requires regardless: handlers must be idempotent. Mohs deduplicates the *request
  to schedule*, never the side effect.
- **A deterministically failing job now costs two attempts**, plus a backoff delay before the second.
- Anyone who genuinely wants at-most-once must know to ask for it, and must accept losing work under
  node failure.
- The default interacts with `RollingUpdateScenario`'s first case: a missing handler goes through the
  budget on purpose, so a node running the newer version may claim the retry — which only works
  because there *is* a budget.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Keep `retries = 0` | Makes the advertised guarantee false by default |
| A higher default (3, 5) | More opinionated than necessary. One retry is enough to make recovery work; anything beyond that is a policy the job's author should choose |
| Give the *reaper* a budget of its own, separate from the job's | Two policies for one question; they would diverge on the first change. `RetrySchedule` is deliberately shared by the dispatcher and the reaper for exactly this reason |
| Treat a reclaim as "not an attempt" and reschedule regardless of budget | Would make an infinite loop possible for a job that kills the node it runs on |

## Evidence

- `mohs-api/src/main/java/io/mohs/core/definition/MohsJob.java` — the default and its reasoning.
- `mohs-engine/src/main/java/io/mohs/engine/Engine.java` — `decideReclaim`, where a zero budget
  produces a terminal `FAILED`.
- `mohs-engine/src/main/java/io/mohs/engine/RetrySchedule.java` — one policy, shared by both failure
  paths.
- `mohs-engine/src/main/java/io/mohs/engine/EngineMetrics.java` — `leaseReclaimed`, whose
  `attempts_exhausted` label exists so an operator can see this happening.
