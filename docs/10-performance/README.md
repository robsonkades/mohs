# 10. Performance

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [performance-characteristics.md](performance-characteristics.md) | Structural properties that transfer, measured throughput and latency, idle cost, write amplification, the cost model, known bottlenecks |
| [tuning.md](tuning.md) | The knobs ordered by impact, per-workload starting points, database-side settings, how to diagnose where the ceiling is |
| [benchmarks.md](benchmarks.md) | The nine scenarios, the three scripts, prerequisites, and how to write a new one |

## The caveat, stated once

Every number in this section was measured on a **local development machine** against **Docker
Desktop containers** — not production hardware, not CI. They compare dialects and arms **against each
other within one round**, and detect relative regression in later rounds on the same machine. **They
are not capacity-planning inputs for your hardware.**

## The three facts worth carrying away

1. **The defaults are latency-oriented.** `poll=5s`, `batch=50` caps arithmetically at 10
   executions/s. If you have a backlog and have not tuned, you are measuring your configuration.
2. **History size does not affect claim cost** — the claim references only `mohs_ready` and
   `mohs_lease`. What does cost is the *backlog*: `GET /overview`'s `COUNT(*)` over the queue, and
   the idle probe when a large non-visible backlog exists.
3. **Cross-node dispatch latency is bounded by `max-poll-interval`.** The local hand-off wakes only
   the JVM that enqueued. A LISTEN/NOTIFY tier was built to close that gap and **withdrawn** after
   measurement — it serialised the ingest, taking a 10 k REST load from ~15 s to ~29 s and average
   POST latency from ~8 ms to ~1,300 ms.

## The measurement rule the codebase applies to itself

> Without a number, it is not an optimisation.

Four plausible optimisations were implemented, measured and **reverted**: the NOTIFY wake-up tier, an
`INCLUDE` on the claim index, a `UNION ALL` claim rewrite for MySQL, and weekly time partitioning on
PostgreSQL. Apply the same standard to anything you change.
