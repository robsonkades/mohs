# DR-002 — Opt-in history retention, swept in bounded batches on the tick

Status: Accepted · Date: 2026-09-01 · Closes: TD-02

## Context

Nothing pruned execution history: `mohs_execution`, `mohs_attempt` and `mohs_batches` grew forever
(`mohs_idempotency` already had its own window — the deduplication contract). Unbounded growth is a
storage problem, a data-protection liability (payloads retained indefinitely), and eventually a
performance one. The documented answer was the operator's obligation
([data lifecycle](../../06-data/data-lifecycle.md)); TD-02 asked for the policy itself.

## Decision

1. **One property, opt-in, no default window**: `mohs.engine.history-retention` (Duration). `0s` —
   the default — keeps everything forever. An embedded scheduler that deletes history by default
   surprises exactly the deployments with audit obligations; the property is the tool for the
   documented obligation, not a surprise. Validation mirrors its sibling `idempotency-retention`
   (non-negative; zero = off).
2. **Scope: history only.** Terminal executions finished before the cutoff, attempts whose execution
   no longer exists, and batches with no remaining member. `mohs_idempotency` is untouched: its
   window IS the dedup contract, so a key row may outlive its execution's history — reusing the key
   still deduplicates, which is correct, and the receipt id it returns may point at pruned history,
   which is documented.
3. **Where it runs**: a `history-prune` step of the engine tick's `runMaintenance`, hourly (the same
   cadence and isolation as the idempotency prune — every node issues the same sweep; the loser of a
   row race deletes nothing). Per pass, each statement is bounded to 1,000 rows; passes repeat only
   while a table keeps coming back full and a 2s monotonic budget lasts — the claim-rounds shape.
   Statement query timeout is 1s (a sweep statement that waits longer is lock-bound, and the tick
   carries the heartbeat). A months-deep backlog drains across hourly slots, invisibly.
4. **No new index.** The candidate read ranges the PRIMARY KEY: ids are UUIDv7 — time-ordered and
   lexicographically sortable — so the store synthesizes the smallest UUIDv7 an id born at the
   cutoff could be and predicates `execution_id < :cutoffId`. This is ADR-0040's "keyset-able if
   ever needed" promise, cashed. `finished_at < :cutoff` refines (a long retry that finished
   recently survives until its FINISH leaves the window), and the terminal-state predicate sits in
   the DELETE's own `WHERE` — doubled in the subquery shapes (PostgreSQL/H2), because a predicate
   inside a subquery evaluates against a snapshot and serialises nothing; only the outer predicate
   is re-evaluated under the row lock, and that is what makes the race against a manual retry's
   rearm CAS resolve safely in either order. Attempts use the existing throughput index (`finished_at` leads) plus orphanhood; batches
   range their own UUIDv7 PK and are spared while any member remains — which spares open batches by
   construction (a pending member is never pruned).
5. **No cross-statement transaction.** Executions delete first (guarded), then orphaned attempts,
   then member-less batches: each statement is safe alone, and a crash between them leaves rows the
   next sweep's own predicates collect.

## Consequences

- Storage becomes bounded where the operator says so; the FAILED rows a manual retry could rearm
  are gone once outside the window — inherent to retention, stated in the docs.
- The tick's worst case gains, hourly and only when enabled, up to ~5s: the 2s budget is checked
  BETWEEN passes, and a pass is up to three 1s-capped statements, so the last pass can start just
  under the budget and still run whole. The `node-lease-ttl` floor Javadoc lists it beside the
  always-on ceilings; the floor itself stays derived from the always-on steps.
- Three new statements per delegate join the limit-position divergence family (`TOP` vs `LIMIT` vs
  `IN (subquery LIMIT)`), each exercised against its real database by the schema round-trip tests.

## Verified by execution plan (2026-09-01)

Seeded ~225k executions / 267k attempts / 5k batches on real PostgreSQL 16, SQL Server 2025 and
MySQL 8.0, real UUIDv7 ids, 7-day cutoff:

- **Executions**: the PK-range claim holds on all three — clustered/PK seek below `:cutoffId`
  (SQL Server 9 ms, MySQL 20–40 ms, PostgreSQL 2.1 ms custom plan / 23.5 ms generic, both within
  budget). **No `finished_at` index needed, confirmed with numbers.**
- **Batches**: 1–2 ms everywhere; PostgreSQL uses the PARTIAL `idx_mohs_execution_corr` for the
  member probe (the planner proves the equality implies `IS NOT NULL`).
- **Attempts**: the anti-join is O(total tables), not O(limit), on PostgreSQL and SQL Server —
  ~80 ms per pass at ~0.5M total rows (MySQL probes the parent PK per row and is O(limit) already).
  Correct and comfortably inside the 1s statement timeout at that volume.

## Deferred, with its trigger

**O(limit) attempts pruning driven by the deleted ids** — the executions delete returns what it
removed (`RETURNING` / `OUTPUT`), the attempts delete probes those ids on its composite PK, and the
orphanhood predicate stays as the rare crash backstop. Deferred because today's cost is 80 ms
against a 1s ceiling, and the redesign adds a per-dialect statement shape plus drift-test churn.
**Trigger**: when `mohs_execution` + `mohs_attempt` total ~2M rows, measure one attempts pass on
PostgreSQL; at ~1s — the statement timeout, at which point the sweep would fail every slot and stop
pruning attempts and batches — implement it. (Companion pattern: RATE-LIMIT-EVOLUTION.)

Two operational notes from the same measurement: the 1,000-row batch is also a LOCKING property on
SQL Server (≥5,000 row locks escalate to a table lock against completion writes — do not raise it
past that without re-measuring), and a massive MANUAL delete on MySQL leaves the next hourly pass
paying InnoDB purge lag (measured 6.2s once, self-healing) — the sweep's own cadence never piles
that up.

## Alternatives considered

- **A default window (e.g. 90 days)** — rejected: silent deletion is the worse surprise; the
  unbounded table at least fails loudly in a dashboard.
- **An index on `finished_at`** — rejected without measurement being needed: the UUIDv7 PK range
  gives the sweep its seek for free, and a new index on the hottest history table is paid by every
  completion write.
- **Partitioning with `DROP PARTITION`** — rejected: `V5` removed partitioning deliberately; a
  retention feature must not re-import that complexity.
- **A separate scheduler thread for the sweep** — rejected: the tick already owns maintenance,
  isolation and metrics (`mohs.tick.failed{step="history-prune"}`); a second loop is a second
  lifecycle to shut down.
- **Deleting the idempotency row with its execution** — rejected: that re-opens the dedup window
  early, turning retention into a correctness change.
