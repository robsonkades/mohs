# DR-008: An adaptive poll with local hand-off; the NOTIFY tier was withdrawn

## Status

Accepted

## Context

A polling scheduler faces a direct trade: a short poll gives low dispatch latency and high idle query
cost; a long poll gives the reverse. With sharding, a lap is up to 64 statements, so an idle
four-node cluster was measured at **108.8 queries/s** — for a system doing nothing.

The obvious fix is event-driven wake-up. PostgreSQL offers `LISTEN`/`NOTIFY`, and it was implemented.

## Decision

**Three tiers, of which only two survived.**

| Tier | Mechanism | Status |
| --- | --- | --- |
| Local hand-off | An enqueue from **this JVM** that is already due wakes the loop after commit | **Kept** |
| Adaptive poll | The interval starts at `poll-interval`, doubles on every empty tick to `max-poll-interval`, and returns to the floor on the first tick that finds work | **Kept** |
| Cross-node NOTIFY | `pg_notify` per enqueue, with peers listening | **Implemented, measured, withdrawn** |

Two caps shorten the sleep without touching the backoff's state:

- **The nearest armed trigger.** The backoff is blind to deadlines, and a recurring job has a known
  one. Floored at `poll-interval`, so N dense triggers cannot redefine the loop's cadence.
- **`node-lease-ttl / 3`.** The heartbeat rides on the tick, so the tick's cadence *is* the liveness
  promise's cadence.

And an **idle gate**: while the previous complete lap came back empty, a single existence probe
answers for the whole lap.

## Consequences

### Positive

- **Idle cost fell 24×** on a single node (96.0 → 4.02 queries/s) and 6.8× on a four-node cluster
  (108.8 → 16.0), measured A/B on the same binary.
- Nothing else regressed in that A/B: dispatch latency p50 41.1 → 35.3 ms, drain throughput unchanged.
- **The local hand-off gives near-zero added latency for the common case**: an application that
  schedules and runs the job in the same JVM.
- A trigger's maximum delay is bounded by `poll-interval` (25 ms by default), not by the backoff
  ceiling — measured on the demo, a `PT1S` job had been firing in pairs of ~0.4 s and ~1.6 s and never
  at 1.0 s before the trigger cap.

### Negative

- **Cross-node dispatch latency is bounded by `max-poll-interval`, and that is the accepted cost.**
  Measured on an idle four-node cluster: the node that received the POST dispatched at p50 25 ms; the
  other three at 504 / 612 / 844 ms.
- The mitigation is configuration, not mechanism: lower `max-poll-interval` if cross-node latency
  matters more than idle cost.
- **The idle gate has a weak case**: with a large *non-visible* backlog the probe becomes a sequential
  scan — measured 5.53 ms at 200 k and 27.5 ms at 1 M non-visible entries, and far worse with dead
  tuples present (10.45 ms / 38,607 buffers with 50 k dead tuples). The aggressive autovacuum settings
  on `mohs_ready` exist to keep the dead-tuple case from arising.
- The gate must be **fail-open**: a probe that throws returns `true` and falls back to the lap.
  Without that, a persistent probe failure would leave a node alive, heartbeating, owning 1/n of the
  shards and never claiming — and nobody would reap it, because it is not dead.
- **Only a *complete, empty* sweep may arm the gate.** Zero claimed because dispatch saturated or the
  budget ran out is not an empty queue; arming on those would make the gate pay for a probe per tick
  on the hot path, the opposite of its purpose.

## Why NOTIFY was withdrawn

It worked, and the ingest cost was decisive:

| Metric | Before | With `pg_notify` per enqueue | With global conflation |
| --- | --- | --- | --- |
| REST load of 10 k, wall time | 14.3 / 15.3 s | **29.0 / 29.2 s** | 15.9 / 15.1 / 14.8 s |
| Average POST latency | 7.7 / 18.7 ms | **1,281 / 1,514 ms** | 9 / 10 / 17.9 ms |
| Drain throughput | 12.0–12.8 k/s | 12.9–13.1 k/s | 11.1–12.2 k/s |

The cause: **the notifying transaction does not take part in group commit**, so it serialised the
ingest. A conflated variant recovered the ingest but did not justify keeping a mechanism with a
dialect-specific dependency and a listening connection per node — for a gap the poll already bounds.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| A fixed short poll | 96 queries/s per idle node |
| A fixed long poll | Multi-second dispatch latency even on the local node |
| `LISTEN`/`NOTIFY` | Measured above. Also PostgreSQL-only, in a project with four dialects |
| A message broker for wake-up | A second datastore, contradicting the library's premise |
| Making the local hand-off cross-node over HTTP | Node-to-node networking, which the design deliberately does not have |

## Evidence

- `mohs-engine/src/main/java/io/mohs/engine/Engine.java` — `runLoop`, `nextBackoff`,
  `cappedByNextFire`, `awaitWork`, `signalWorkScheduled`, `claimAndDispatch`, `probeSaysThereIsWork`,
  and the class Javadoc recording the withdrawal.
- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/JdbcWorkQueue.java` — `hasVisibleWork`.
- Idle-gate and cluster-latency measurements recorded 2026-08-22 and 2026-08-23.
