# Architecture overview

Mohs is an embedded Java scheduling library coordinated by a relational database. The process runs handlers; the database arbitrates publication, acceptance, acquisition and fenced state transitions. No scheduler leader is required. Database availability and its own failover guarantees remain dependencies.

## Responsibilities and dependencies

| Module / class | Responsibility |
|---|---|
| mohs-api | Typed job references and inputs, immutable definitions/options, scheduling and lifecycle contracts |
| MohsImpl | Validated API operations, payload snapshot creation and local wakeup after acceptance |
| Engine | Session lifecycle and bounded coordinator stages; dispatches acquired work |
| WorkAdmission | Page-scoped window checks and grouped shared-rate admission |
| FiringPlanner | Pure occurrence/misfire planning from a database-bounded observation |
| Dispatcher | Resolve local handler/runner, decode snapshot, invoke user code and construct a completion |
| CompletionBatcher | Bounded delivery of completion groups, retry after uncertain database outcomes |
| mohs-store-jdbc | Complete database operations with explicit isolation, lock order and row-count verdicts |
| mohs-spring-boot-starter | Composition, normalized registration/catalog publication and lifecycle wiring |
| mohs-rest / mohs-ui | HTTP contracts and administration; no separate scheduler rules |
| mohs-test / mohs-benchmark | Deterministic doubles, real protocol fixtures and opt-in measurements |

Public contracts do not depend on JDBC. Engine ports describe complete operations, not table CRUD. JdbcSubmissionStore owns both manual acceptance and scheduled firing. JdbcExecutionTransitions shares the actual batch-counter and fixed-delay terminal effects. JdbcSessions and JdbcAdmission keep the session/job locking protocol consistent among writers. They are internal helpers, not extra public extension layers.

## Persistence and authoritative state

Eight tables include the executable catalog. Work is one READY/LEASED row in mohs_work. The retained mohs_execution row contains the immutable snapshot, PENDING or a terminal state, and only the latest completed attempt. Idempotency receipts expire independently. See the [inventory](../06-data/data-model.md) and [index contracts](../06-data/indexes.md).

| Committed representation | Public execution state |
|---|---|
| PENDING + LEASED work | RUNNING |
| PENDING + READY, attempt > 1, future visibility | RETRY_WAITING |
| PENDING + other READY work | ENQUEUED |
| Terminal history + absent work | SUCCEEDED, FAILED or CANCELLED |
| PENDING without work, or contradictory terminal/work shape | Invariant failure |

There is no permitted completion-flush gap with PENDING history and absent work. Until the completion transaction commits, the old grant remains; afterwards the full transition is visible. Local pending completion is a delivery concern, not a second durable state machine.

## Coordinator and progress

Each tick creates one monotonic budget of node TTL / 4 before heartbeat. A rotating stage cursor visits recovery, firing, claim, cancellation, stray reconciliation, cleanup and metrics. It advances even if a stage exhausts the budget. Statement deadlines limit individual database calls; a call already blocked in a pool or driver can exceed the remaining round budget.

Claims read at most 128 visible job identities per shard page, advance a durable-in-process key cursor, bulk-load only those policies and apply the positive eligible set. The cursor also advances through closed-window jobs. SQL DISTINCT with a bounded result can still inspect many duplicate/future entries, so bounded returned metadata is not an O(page-size) database-work guarantee.

The authoritative per-job concurrency check is serialized by job locks, and counts active grants. Window/rate admission after claim may return the same attempt to READY. Shared rate names are evaluated and charged in groups; tokens are not refunded after ambiguous outcomes. Runner permits and dispatch slots remain process-local limits. None bounds how many external operations a paused stale process may still perform.

Cancellation and watchdog inspections keep continuation iterators; own-session stray grants use composite keyset pages. Timeouts interrupt cooperatively. A handler ignoring interruption may continue after losing authority; its stale result cannot change scheduler state.

## Distributed contract

One process session has a UUIDv7, database expiry and irreversible revocation. A fresh token identifies each acquisition. Complete operations lock session, ordered jobs, work/history and ordered batches; catalog publication adds its own leading arbitration lock. See [transactions](../04-engineering/transactions.md) and [failure sequences](clustering-and-liveness.md).

Retries are finite recovery attempts, not an unconditional eventual-delivery guarantee. An external effect may finish before a crash hides its completion. A later authorized attempt can repeat that effect. Users need business idempotency or destination-enforced fencing when duplicates matter. Priority is local to selected work and there is no global execution/completion ordering.

Database time decides sessions, rate refill, occurrence/claim eligibility and receipt expiry. Configured Clock values still serve application schedule construction and local execution timestamps; monotonic time measures local waits. Technical keys compare exactly. Generated identities have canonical UUIDv7 form, and payloads use an explicit format contract independent of Java class names.

## Verification

Correctness tests cover real lock races, rollback after actual writes, response loss after commit, revocation versus renewal, stale completion, cancellation/retry and catalog mismatches across four databases. Architecture scans enforce selected dependency and SQL-writing boundaries. Read their actual assertions rather than assuming all design rules are mechanically enforced.

The schema, APIs, tests and installers change together before the first release. There are no deprecated facade adapters, dual writes or migration compatibility branches for unpublished versions.
