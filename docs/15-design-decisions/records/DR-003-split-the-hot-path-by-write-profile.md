# DR-003: Split the hot path into four tables by write profile

## Status

Accepted

## Context

The original design held executions in one table: enqueue inserted, claim updated, the lease was
renewed, completion updated again, and history accumulated in the same rows. Measured on PostgreSQL,
that cost **3.9 commits and about 4 tuple versions per execution**, with 6.7–7.0 updates on the
executions table under sustained in-flight work.

Worse than the cost was the coupling: the table the claim scanned was the same table history grew in,
so claim cost tracked history size — a scheduler that gets slower the longer it has been running.

## Decision

Split the hot path into **four tables, one per shape of write**:

| Table | Write profile | Size proportional to |
| --- | --- | --- |
| `mohs_ready` | INSERT on enqueue/retry/requeue, DELETE on claim | The backlog |
| `mohs_lease` | INSERT on claim, DELETE on completion | Work executing across the cluster |
| `mohs_execution` | One INSERT at birth, one UPDATE at terminal | Grows forever |
| `mohs_attempt` | Append-only | Grows forever |

Plus `mohs_idempotency`, separated so the deduplication uniqueness is independent of history's
physical layout.

## Consequences

### Positive

- **Claim cost no longer depends on history size.** The claim statement references only `mohs_ready`
  and `mohs_lease`. Verified as a release gate: throughput flat between roughly 0 and 2 M history
  rows, in a clean A/B on the same binary and the same session.
- **The ownership table is bounded** by `nodes × dispatch-concurrency` — thousands of rows, never
  millions. That is what makes the derived per-job concurrency cap an always-cached index scan, and it
  is why a hot `running_execution_count` counter could be deleted outright.
- **Two tuple versions per execution in history** (one INSERT, one advisory UPDATE), down from about
  four.
- **The claim index has one job.** `(shard, priority, visible_at)` on a table that *is* the backlog —
  no partial-index trickery needed to exclude terminal rows.
- Retention becomes a bulk delete on tables nothing on the hot path reads.

### Negative

- **An execution's state is spread across four tables**, and only a transaction holds it together.
  Every transition that spans them must be one transaction, and `LeaseStore.CompletionResult`'s
  compact constructor enforces the "terminal **or** retry, never both, never neither" invariant
  structurally because of it.
- **`mohs_execution.state` became advisory**, which required the derived read model
  ([DR-004](DR-004-derive-state-rather-than-trust-a-column.md)).
- **Migration cost.** The expand/contract took two versions (`V3` creating the new tables alongside,
  `V4` dropping the old), and on PostgreSQL a third (`V5`) to undo partitioning that had been
  introduced with them.
- **No foreign keys** between the split tables — an FK from `mohs_ready` to `mohs_execution` would add
  index maintenance and a lock dependency to the hottest insert path.
- The queue table takes real churn, so PostgreSQL needs explicit `fillfactor` and autovacuum tuning.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Keep one table, add partial indexes | Tried in the single-table era and it worked (95.2% index-size reduction on PostgreSQL). But it did not decouple claim cost from history growth, and it carried a trap: a partial index is only eligible when the query's predicate **implies** the index's, so when retry became claimable the plan silently degraded to a sequential scan plus sort |
| One table with time partitioning | Adopted on PostgreSQL and then **removed**: it was the only structural divergence between dialects, cost a production class with its own failure mode, and bought a benefit — retention by partition drop — that did not yet exist |
| Move history to a separate database | Loses the single-transaction guarantee that makes the enqueue unit and the completion atomic |

## Evidence

- `mohs-store-jdbc/src/main/resources/io/mohs/store/jdbc/migration/postgresql/V3__table_split.sql` —
  the four write profiles are named in its header.
- `mohs-engine/src/main/java/io/mohs/engine/LeaseStore.java` — the completion transaction's contract.
- `mohs-engine/src/main/java/io/mohs/engine/WorkQueue.java` — the single visibility rule.
- Write-amplification and flat-versus-history measurements recorded 2026-08-22.
