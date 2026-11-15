# 10. Performance

Status: Active · Last Reviewed: 2026-09-10 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [tuning.md](tuning.md) | Configuration controls, workload starting points and database diagnosis |
| [benchmarks.md](benchmarks.md) | Current scenarios and scripts, prerequisites, methodology and limits |
| [data model](../06-data/data-model.md) | Current table layout and authoritative state |
| [indexes](../06-data/indexes.md) | Access paths and the operations they support |

The removed experimental reports are not shipped here. The harnesses describe how to collect
current evidence; their presence alone does not establish throughput or validate this working tree.

## The caveat, stated once

Historical benchmark numbers in this section came from a **local development machine** against **Docker
containers** — not production hardware, not CI. They compare dialects and arms **against each
other within one round**, and detect relative regression in later rounds on the same machine. **They
are not capacity-planning inputs for your hardware.**

## The three facts worth carrying away

1. **The defaults are latency-oriented.** The 25 ms poll floor favours prompt dispatch, while
   adaptive backoff reduces idle traffic. Backlog throughput still depends on batch size, claim
   rounds, dispatch capacity, handler duration and database capacity.
2. **Active work has its own indexes**, separate from retained snapshots. Shared database resources and cleanup still affect observed cost; the old archive-size experiment is historical evidence.
3. **The poll ceiling bounds sleep, not dispatch latency.** Candidate pages, resource admission, backlog and database waits add delay. Local wakeup only reaches the submitting process.

## The measurement rule the codebase applies to itself

> Without a number, it is not an optimisation.

Four plausible optimisations were implemented, measured and **reverted**: the NOTIFY wake-up tier, an
`INCLUDE` on the claim index, a `UNION ALL` claim rewrite for MySQL, and weekly time partitioning on
PostgreSQL. Apply the same standard to anything you change.
