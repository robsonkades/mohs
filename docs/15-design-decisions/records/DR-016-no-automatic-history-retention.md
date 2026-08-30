# DR-016: History has no automatic retention — deferred, not decided against

## Status

Accepted, with a known cost

## Context

`mohs_execution` and `mohs_attempt` are append-only records of every execution and every attempt the
system has ever run, and they include the **payload** of each execution, stored as plaintext JSON.
`mohs_idempotency` grows with every idempotent enqueue. `mohs_batches` grows with every batch.

All four grow forever.

The table split ([DR-003](DR-003-split-the-hot-path-by-write-profile.md)) was designed with retention
in mind — its migration header says as much, describing "retention becomes a `DROP` of a partition"
as a later phase. PostgreSQL time partitioning was even adopted to make that cheap, and then
**removed**, on the grounds that it was the only structural divergence between dialects, cost a
production class with its own failure mode, and bought a benefit that did not yet exist.

## Decision

**No automatic retention is implemented.** Exactly one purge exists, and it is not history's:
`Engine#purgeStaleNodeRows` removes `mohs_nodes` rows older than `node-lease-ttl × 10`, riding along
on the tick.

A prune method for the idempotency table **does** exist —
`HistoryStore#pruneIdempotencyBefore(cutoff)`, described in its own Javadoc as "called by
housekeeping, never on the hot path", and properly indexed. **No production code calls it.**

Retention is therefore the operator's responsibility, and the documentation says so prominently.

## Consequences

### Positive

- **Complete forensic history.** Every attempt, its node, its exception class and message are
  preserved indefinitely — which is exactly what an operator wants at 3 a.m.
- **No risk of deleting something still live.** A wrong retention policy is worse than none, and the
  correct predicates are subtle: only terminal rows with a past `finished_at`, and a `PENDING` row with
  no queue entry and no lease is a completion flush in progress, not a candidate.
- **The cost is bounded to storage**, not to the hot path: the claim references only `mohs_ready` and
  `mohs_lease`, and the `GET /overview` throughput count is a range scan over the requested window.
  Growing history does **not** slow the system down.
- The schema is already shaped for retention: `idx_mohs_attempt_throughput (finished_at, outcome)`
  makes a time-bounded delete an index range, and `V7` added `idx_mohs_idempotency_created` for exactly
  this purpose — measured at 83.2 ms → 0.97 ms on 2 M rows.

### Negative

- **Storage grows without bound**, proportional to every execution ever run.
- **Payloads are retained forever**, which may be a data-protection obligation rather than a
  preference.
- **The idempotency window is effectively unbounded.** `ScheduleCommand#idempotencyKey`'s contract says
  the key deduplicates "for as long as the execution exists — the window is that of execution
  retention, which is unbounded while no retention policy exists". So reusing an old key returns the
  old execution, indefinitely. Keys must be designed to be unique per intended unit of work.
- **An operator must implement retention themselves**, and the correct `DELETE … LIMIT` form differs
  per dialect — which is one honest reason a portable implementation inside the library is real work
  rather than a small addition.
- The absence is easy to miss until the table is large.

## Alternatives considered

| Alternative | Why not (yet) |
| --- | --- |
| Time partitioning with retention by partition drop | Adopted on PostgreSQL and removed: the only structural divergence between dialects, a production class with its own failure mode, for a benefit that did not exist yet |
| A recurring internal job doing batched deletes | The obvious implementation, and the one recommended to operators. Not shipped, and it needs per-dialect batched-delete syntax plus a window policy that separates successes from failures |
| A retention property with a default | A default that deletes data is dangerous; a default that does not is a property nobody sets |
| A hard cap on row count | Deletes by age are comprehensible; deletes by count are not |

## What an operator must do

Documented in [data lifecycle](../../06-data/data-lifecycle.md), including:

- Delete **in batches**, never one unbounded statement — a long transaction holds back PostgreSQL's
  xmin horizon and bloats the hot tables the autovacuum settings were tuned to protect.
- Order: `mohs_attempt`, then `mohs_execution`, then `mohs_batches`, then `mohs_idempotency`.
- Only terminal rows with a past `finished_at`.
- Prune `mohs_idempotency` by its **own** window, which is typically far shorter than history's.

The natural implementation is a Mohs job of the operator's own — the tool schedules its own
housekeeping perfectly well.

## Evidence

- `mohs-engine/src/main/java/io/mohs/engine/HistoryStore.java` — `pruneIdempotencyBefore` and its
  "called by housekeeping" note.
- A repository-wide search: **no production caller and no scheduled invocation** of that method.
- `mohs-engine/src/main/java/io/mohs/engine/Engine.java` — `purgeStaleNodeRows`, the only purge that
  runs.
- `V3__table_split.sql` and `V5__drop_partitioning.sql` — retention named as a later phase, and the
  partitioning that would have served it removed.
- `V7__idempotency_retention_index.sql` — the index measurement.
