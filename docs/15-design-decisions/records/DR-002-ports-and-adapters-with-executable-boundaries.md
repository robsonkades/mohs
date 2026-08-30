# DR-002: Ports and adapters, with boundaries made executable twice over

## Status

Accepted

## Context

A scheduler's correctness lives in its execution logic — claim, admission, retry, recovery — and its
performance lives in its SQL. Mixing the two produces code where a change to a query can break a
delivery guarantee, and where the logic cannot be tested without a database.

Architectural rules stated only in prose decay. The question was not whether to draw a boundary but
how to make one that cannot be crossed by accident.

## Decision

Ports and adapters, with the boundary enforced by **two independent mechanisms**:

1. **The Maven reactor.** `mohs-api` has no `mohs-engine` on its compile classpath; `mohs-engine` has
   no `mohs-store-jdbc`. A violation does not compile.
2. **ArchUnit**, in `mohs-demo` — the one module that sees every other on a single classpath, which is
   what makes a whole-system rule checkable at all.

Ten ports live in `io.mohs.engine` as interfaces the engine defines and does not implement:
`JobStore`, `WorkQueue`, `LeaseStore`, `HistoryStore`, `NodeStore`, `BatchStore`, `RateLimitStore`,
`TriggerFirer`, `StoreTransactions`, `SyncableClock`.

The ports are cut by **concept** (queue, ownership, history, control), not by entity.

## Consequences

### Positive

- **`InMemoryJobStore` exists with no database at all** — which is simultaneously a test kit and the
  proof that the port leaked nothing JDBC-specific.
- **The engine's tests are fast and deterministic**: `ShardsTest`, `RetryScheduleTest`,
  `FiringPlannerTest`, `EngineSleepTest` need no I/O.
- **A `ResultSet` in a port signature fails the build** (`engine_is_free_of_jdbc`), so the leak is
  caught by type rather than by review.
- **Adding a dialect is a bounded change** with a documented five-step recipe.

### Negative

- **Ten interfaces with one production implementation each.** That is indirection, and the project's
  own principles are suspicious of it. It is accepted because the second implementation already
  exists (in-memory) and because the boundary is what makes the whole test strategy possible.
- **`LeaseStore#complete` writes history**, which reads as a leak until the rationale is known: the
  ports follow *concepts*, and a completion is a concept of ownership that touches history. Splitting
  it would split one transaction across two ports.
- **Two ports deliberately do not open transactions** (`HistoryStore#record`, `WorkQueue#offer`), and
  calling either alone is not a supported mode. That is a contract a reader must know.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| One `MohsRepository` port | The four write profiles have genuinely different lifecycles; one interface would be a God object |
| An ORM (JPA/Hibernate) | The claim query's exact shape **is** the performance story, and an ORM hides it. Also, `SKIP LOCKED` plus a dialect-specific single-statement CTE is not something to express through a mapper |
| Prose-only architecture rules | They decay. Every rule here is a test |
| Ports cut per entity | Would have put queue writes and history writes in the same port, which is exactly what the table split separated |

## Evidence

- `mohs-demo/src/test/java/io/mohs/ArchitectureTest.java` — twelve rules, each with its rationale.
- `mohs-engine/src/main/java/io/mohs/engine/package-info.java` — the port inventory.
- `mohs-test/src/main/java/io/mohs/test/InMemoryJobStore.java` — the no-database implementation,
  including its two declared divergences.
- Every module `pom.xml` — the dependency direction.
