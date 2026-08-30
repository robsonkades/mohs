# DR-005: Coordinate without a leader, through three database primitives

## Status

Accepted

## Context

A clustered scheduler must answer three contended questions: who fires a due trigger, who runs a
queued execution, and whose completion counts. The conventional answers are a leader election (one
node decides) or a distributed lock service.

Both add a component with its own failure modes, its own operational surface, and its own split-brain
story — for a library whose entire premise is that it adds no operational surface
([DR-001](DR-001-embedded-library-not-a-server.md)).

## Decision

**No leader, no consensus protocol, no lock service.** Every contended decision is arbitrated by the
database, with exactly three primitives:

| Decision | Mechanism | The loser |
| --- | --- | --- |
| Who fires a due trigger | `UPDATE mohs_job_definitions SET next_fire_at = :new WHERE next_fire_at = :observed AND retired = false` | Gets 0 rows and moves on. **Routine, not an error** |
| Who runs a queued execution | `SELECT … FOR UPDATE SKIP LOCKED` (or `WITH (UPDLOCK, ROWLOCK, READPAST)`) inside the claim transaction | Never sees the row |
| Whose completion counts | `DELETE mohs_lease WHERE execution_id = ? AND node_id = ? AND epoch = ?` | Row count 0, result discarded with a WARN |

Nodes discover each other only through `mohs_nodes`. There is no gossip, no registry, no election.

## Consequences

### Positive

- **No additional infrastructure.** The database an application already has is the only dependency.
- **No split brain to reason about.** The database is a single authority by construction.
- **A losing node's behaviour is trivially correct**: it does nothing. There is no state to reconcile,
  no term number, no fencing negotiation between peers.
- **The trigger CAS makes occurrences exactly-once *at materialisation***: because the advance and the
  insert are atomic, a crash between them can neither lose nor duplicate an occurrence. That is why
  occurrences carry no idempotency key — a key would be subject to a retention window, atomicity is
  not.
- Adding or removing a node requires no coordination at all.

### Negative

- **Every decision is a round trip.** The claim is the hottest statement in the system, and its shape
  is therefore the performance story.
- **The database is a single point of failure**, and there is no degraded mode without it.
- **Contention is real and must be engineered around**: the claim's `SKIP LOCKED` never waits, but the
  rate-limit bucket does — which is why it carries the only unconditional wait on the claim path, with
  a 2-second ceiling, because one stuck node holding that row would delay other nodes' heartbeats and
  turn contention into a false positive of death.
- **The isolation level cannot be left to the database's default.** MySQL defaults to
  `REPEATABLE READ`, so claim, completion and trigger firing all set `READ COMMITTED` explicitly with
  `REQUIRES_NEW` — otherwise an outer transaction would impose its own and the guarantee would be
  silently lost.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Leader election (Raft, ZooKeeper, a lease-based leader) | Adds a component and a failure mode for a problem three SQL statements already solve. Also concentrates firing on one node, which the CAS does not |
| A distributed lock service (Redis, etcd) | A second datastore to operate, and a second consistency model to reason about |
| Advisory locks in the database | Dialect-specific and stateful across a connection; the CAS is portable and stateless |
| Optimistic concurrency with a version column | That is exactly what the CAS is, expressed on the column that already carries meaning |

## Evidence

- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/JdbcTriggerFirer.java` — the CAS, and why it
  compares a value read *from the column*.
- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/dialect/JdbcDialect.java` — the claim, per dialect.
- `mohs-store-jdbc/src/main/java/io/mohs/store/jdbc/JdbcLeaseStore.java` — the fenced completion.
- `RecurringTriggerScenario` — asserts that three nodes firing the same trigger produce one occurrence.
