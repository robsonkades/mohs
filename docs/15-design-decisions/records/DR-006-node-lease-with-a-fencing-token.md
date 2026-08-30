# DR-006: Liveness is a node lease; every owned write carries a fencing token

## Status

Accepted

## Context

An earlier design gave **each execution** a lease, renewed on every tick while it ran. Measured under
sustained in-flight work, that cost roughly **five updates per execution** on the hottest table in the
system — the same table the claim scanned.

Independently, a scheduler must answer the hardest question in distributed systems honestly: a node
that stops responding might be dead, or might be stalled and about to come back holding stale state.
Any failure detector that is not perfect — and none is — must be paired with a mechanism that makes a
false suspicion **safe**.

## Decision

Two coupled decisions.

**1. Liveness is per node, not per execution.** Each tick, before anything else, a node writes:

```
mohs_nodes(node_id, state, last_heartbeat_at = now, epoch, expires_at = now + node-lease-ttl)
```

`expires_at` is a promise: *"I am alive until here."* A peer's reaper treats any lease whose owner is
absent from the live set as orphaned. **Death is never written** — a crash produces no record, so
alive-versus-dead is derived from the promise's age at read time.

**2. Every write over owned work carries `(node_id, epoch)`** — a fencing token. The completion is a
fenced `DELETE`, and **the row count is the verdict**.

The epoch starts at 1 and rises **only when the node itself observes that its own lease had expired**.

## Consequences

### Positive

- **One write per node per tick**, replacing roughly five per execution.
- **A false suspicion is safe.** A stalled node that resumes carries an old epoch, so *every* write
  from that incarnation loses. Verified by the `SUSPEND` chaos scenario: 50,000 executions terminal,
  244 reclaimed while the node was frozen, **zero executions with more than one `SUCCEEDED` attempt**.
- **A clean stop is distinguishable from a crash** in the database, because the final heartbeat of a
  graceful shutdown writes `STOPPED`.
- **A discarded result is detected, never silently lost**: the completion logs a WARN naming the
  incarnation that lost.
- Self-diagnosis is symmetric with a peer's judgement — `renewNodeLease` treats `now >= expires_at` as
  expiry using `!isBefore`, because a peer considers the node dead already at equality.

### Negative

- **Recovery latency has a floor of `node-lease-ttl`** (15 s by default). A measured `kill -9` run
  showed the reclaim wave at 15.4 s and full recovery at 19.6 s.
- **The tick's cadence is coupled to liveness.** The heartbeat rides on the tick, so the sleep is
  capped at `node-lease-ttl / 3`, the claim lap budget is `node-lease-ttl / 4`, and a `poll-interval`
  above the cap logs a WARN naming the effective cadence.
- **The shutdown grace is only partly covered.** `renewNodeLease` runs on the tick, which stops during
  the post-loop wait — with a 15 s TTL and a 30 s grace, **more than half the wait is uncovered** and a
  peer may reclaim work still running here.
- **A wall-clock regression is dangerous**, because the promise is compared across nodes. It is
  detected and logged with its consequence, naming NTP and `mohs.time.mode` as what to check.
- A node absent from `mohs_nodes` is dead **by definition**, which is why the stale purge derives its
  cutoff from `node-lease-ttl` rather than `lease-ttl`: using the execution TTL let an operator who
  lowered it delete the row of a node whose promise was still alive.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Per-execution lease renewal | The design being replaced. About five updates per execution on the hottest table |
| A heartbeat with no promise, using staleness alone | Kept as a fallback for rows written by an older jar with no `expires_at`, but a promise is better: it lets the node itself state how long it expects to be alive |
| A monotonically increasing global token from the database | A sequence — which the project's key strategy bans elsewhere, and which would be an extra round trip per claim |
| Trusting the failure detector to be accurate | It cannot be. The fence is what makes inaccuracy safe |

## Evidence

- `mohs-engine/src/main/java/io/mohs/engine/Engine.java` — `renewNodeLease`, `aliveNodeIds`,
  `reapOrphanedLeases`, `purgeStaleNodeRows`.
- `mohs-engine/src/main/java/io/mohs/engine/LeaseStore.java` — the fence's contract.
- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/JdbcLeaseStore.java` — the row count as verdict,
  including the refusal to guess when a driver returns `SUCCESS_NO_INFO`.
- `mohs-benchmark/scripts/chaos-recovery.ps1` — the `S6` and `SUSPEND` scenarios.
