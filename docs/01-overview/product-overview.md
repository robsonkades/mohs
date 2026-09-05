# Product overview

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## What Mohs is

Mohs is a **job scheduling and execution library** for Java 25 and Spring Boot 4, distributed as a
set of Maven artifacts under the `io.github.robsonkades` group. An application adds
`io.github.robsonkades:mohs-spring-boot-starter` to its build, points a `DataSource` at a relational database,
annotates a method, and gets durable, cluster-safe, at-least-once job execution.

The name comes from the Mohs hardness scale; the project description in the root `pom.xml` is
*"Job scheduling for Java/Spring Boot"*, maintained at `github.com/robsonkades/mohs` and licensed Apache 2.0
(`LICENSE`, `NOTICE`).

## The problem it solves

Applications need work to happen **later**, **repeatedly**, or **out of band** — nightly invoices, a
retryable e-mail send, an import triggered from a UI. Doing this correctly in a clustered deployment
requires answers that are hard to get right:

| Question | Mohs' answer | Where |
| --- | --- | --- |
| Two nodes see the same due trigger. Who fires? | A compare-and-set on `mohs_job_definitions.next_fire_at`; the loser does nothing | `io.mohs.store.jdbc.JdbcTriggerFirer` |
| Two nodes see the same queued execution. Who runs it? | `SELECT … FOR UPDATE SKIP LOCKED` (or `READPAST` on SQL Server) inside one transaction that also writes ownership | `io.mohs.store.jdbc.delegate.JdbcDelegate` |
| A node dies mid-execution. What happens to the work? | Its node lease expires; a peer's reaper reclaims the ownership row and reschedules through the retry budget | `Engine#reapOrphanedLeases` |
| The dead node comes back and finishes the job. | Every write over owned work is fenced by `(node_id, epoch, attempt)`; the zombie's result is discarded | `io.mohs.engine.LeaseStore` |
| The clock jumps backwards. | All "when" values come from an injected `Clock`; all durations use `System.nanoTime()`; a backwards jump is logged with its operational consequence | `Engine#renewNodeLease` |
| The same request is submitted twice. | `Idempotency-Key` deduplication via a primary-key conflict on `mohs_idempotency` | `io.mohs.store.jdbc.JdbcHistoryStore` |

## Delivery guarantee

**At-least-once, conditional on retry budget.** Stated precisely, from `Engine`'s class Javadoc:

- Under node failure the guarantee is **at-least-once when `retries > 0`** — which is the default
  (`@MohsJob(retries = 1)`).
- With `retries = 0` a reclaimed orphan has nowhere to reschedule, and the guarantee degrades to
  **at-most-once** for that job. This is a deliberate, documented opt-in.

Exactly-once is **not** offered and is not achievable across a handler that performs side effects
outside the transaction. The recommended pattern for a guaranteed reaction is the transactional
outbox: the handler enqueues the continuation inside its own transaction, rather than relying on a
best-effort listener.

## Boundaries — what Mohs is not

| Not | Why |
| --- | --- |
| A standalone scheduler server | There is no server artifact, no Dockerfile, no deployment manifest. `mohs-demo` is a development application, explicitly never published. |
| A message broker | Persistence is a relational database via JDBC. There is no AMQP/Kafka/JMS dependency anywhere in the reactor. |
| A workflow engine | There are no DAGs, no step dependencies, no compensation. A batch is a flat set of independent members with a shared counter. |
| A distributed lock service | The only cluster-wide coordination is the trigger CAS, the claim's row lock, and the node lease. There is no leader election and no consensus protocol. |
| A security product | The REST API ships with **no authentication**; the host application's security configuration is the only protection. See [security overview](../08-security/security-overview.md). |

## Design posture, as evidenced by the code

Four commitments show up repeatedly in the source and are worth stating up front, because they
explain otherwise-surprising choices:

1. **Operability is a feature.** Warning logs name the property to change and the consequence of not
   changing it (`MohsEngineLifecycle#checkDeclaredPolicies`, `Engine#renewNodeLease`,
   `MohsUiAutoConfiguration#warnIfDashboardHasNoApiToRead`). Metric label values are treated as
   contract (`EngineMetrics`).
2. **Failure modes are designed first.** Nearly every class Javadoc names the failure it exists to
   prevent, and several name a *measured* incident (the stray-lease reconcile grace, the SQL Server
   index predicates, the completion lock ordering).
3. **Degradation is monotonic.** When a guard cannot be fully applied it is *truncated*, never
   switched off — see the inadmissible-job filter cap in `Engine#claimLaps`.
4. **Measurement outranks intuition.** Optimisations in the tree carry before/after numbers in the
   source comments, and several candidate optimisations were implemented, measured, and reverted.

## Current maturity

- Version `0.0.1-SNAPSHOT`; the repository contains CI and a manual release workflow; see [build and publishing](../12-build/modules.md).
- Database tiers, from `MohsAutoConfiguration#mohsJdbcDelegate`: PostgreSQL, MySQL 8.0+ and SQL
  Server are production dialects; **H2 is explicitly a test/dev tier** and logs a WARN at boot when
  selected.
- The REST API and the dashboard are **off by default** and must be switched on deliberately.
