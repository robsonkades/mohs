# 15. Design decisions

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

Decision records **reconstructed from code evidence**. Each one documents a decision that is
observable in the source, with the reasoning the code itself records.

> **Provenance.** These records were written by reading the implementation, its comments, its tests
> and its measurements. Where the original reasoning could not be determined from the repository, the
> record says so explicitly rather than inventing one. They are numbered `DR-nnn` in their own
> sequence.

## Index

| # | Decision | Status |
| --- | --- | --- |
| [DR-001](records/DR-001-embedded-library-not-a-server.md) | Mohs is an embedded library, not a standalone server | Accepted |
| [DR-002](records/DR-002-ports-and-adapters-with-executable-boundaries.md) | Ports and adapters, with boundaries made executable twice over | Accepted |
| [DR-003](records/DR-003-split-the-hot-path-by-write-profile.md) | Split the hot path into four tables by write profile | Accepted |
| [DR-004](records/DR-004-derive-state-rather-than-trust-a-column.md) | Derive execution state from the queue and the lease | Accepted |
| [DR-005](records/DR-005-leaderless-coordination-through-the-database.md) | Coordinate without a leader, through three database primitives | Accepted |
| [DR-006](records/DR-006-node-lease-with-a-fencing-token.md) | Liveness is a node lease; every owned write carries a fencing token | Accepted |
| [DR-007](records/DR-007-fixed-64-shards-derived-assignment.md) | Sixty-four fixed shards, with a derived assignment | Accepted |
| [DR-008](records/DR-008-adaptive-poll-and-the-withdrawn-notify-tier.md) | An adaptive poll with local hand-off; the NOTIFY tier was withdrawn | Accepted |
| [DR-009](records/DR-009-retry-budget-defaults-to-one.md) | A job is born with a retry budget of one | Accepted |
| [DR-010](records/DR-010-group-commit-for-completions.md) | Group commit for completions, with the durability window declared | Accepted |
| [DR-011](records/DR-011-library-owned-flyway-migrations.md) | The library owns its schema, in its own Flyway history table | Accepted |
| [DR-012](records/DR-012-explicit-dialect-never-detected.md) | The SQL dialect is an explicit choice, never auto-detected | Accepted |
| [DR-013](records/DR-013-rest-api-off-by-default-no-auth.md) | The REST API ships unauthenticated and off by default | Accepted |
| [DR-014](records/DR-014-runners-are-specs-not-executors.md) | A runner is a specification; Mohs owns the threads | Accepted |
| [DR-015](records/DR-015-uuidv7-for-every-generated-key.md) | Every generated primary key is UUIDv7 | Accepted |
| [DR-016](records/DR-016-no-automatic-history-retention.md) | History has no automatic retention — deferred, not decided against | Accepted (with a known cost) |

## Decisions that were measured and reversed

Worth reading as a group, because they are the clearest evidence of the project's measurement
discipline. Each was implemented, measured, and removed:

| Reversed | Measurement that killed it |
| --- | --- |
| Cross-node LISTEN/NOTIFY wake-up | The notifying transaction does not take part in group commit and serialised the ingest: a 10 k REST load went from ~15 s to ~29 s, and average POST latency from ~8 ms to ~1,300 ms. See [DR-008](records/DR-008-adaptive-poll-and-the-withdrawn-notify-tier.md) |
| An `INCLUDE` clause on the claim index | Identical plan and buffers, 2.7× the index size, +43% WAL. Pure write amplification |
| A `UNION ALL` claim rewrite for MySQL | p99 improved, throughput fell 32% |
| Weekly time partitioning on PostgreSQL | A production class with its own failure mode, buying a benefit (retention by partition drop) that did not yet exist |
| A hot `running_execution_count` counter per job | Replaced by counting live leases, which is bounded by `nodes × dispatch-concurrency` and therefore always cached |
| A per-execution lease renewed each tick | ~5 updates per execution on the hottest table; replaced by one node heartbeat per tick |

## The template

```markdown
# DR-nnn: <decision>

## Status
Accepted

## Context
<the forces, the constraint, the failure mode>

## Decision
<what was decided>

## Consequences
### Positive
### Negative

## Alternatives considered
<each, with why it was not chosen>

## Evidence
<files, tests, measurements>
```
