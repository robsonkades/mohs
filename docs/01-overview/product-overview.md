# Product overview

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## What Mohs is

Mohs is a **job scheduling and execution library** for Java 25 and Spring Boot 4, distributed as a
set of Maven artifacts under the `io.github.robsonkades` group. An application adds
`io.github.robsonkades:mohs-spring-boot-starter` to its build, points a `DataSource` at a relational database,
registers a typed handler or annotates a method, and gets durable scheduling with clustered acquisition and bounded recovery.

The name comes from the Mohs hardness scale; the project description in the root `pom.xml` is
*"Job scheduling for Java/Spring Boot"*, maintained at `github.com/robsonkades/mohs` and licensed Apache 2.0
(`LICENSE`, `NOTICE`).

## The problem it solves

Applications need work to happen **later**, **repeatedly**, or **out of band** — nightly invoices, a
retryable e-mail send, an import triggered from a UI. Doing this correctly in a clustered deployment
requires answers that are hard to get right:

| Question | Mohs' answer | Where |
| --- | --- | --- |
| Two nodes see the same due trigger. Who fires? | A compare-and-set on `mohs_job_definitions.next_fire_at`; the loser does nothing | `io.mohs.store.jdbc.JdbcSubmissionStore` |
| Two nodes see the same queued execution. Who runs it? | `SELECT … FOR UPDATE SKIP LOCKED` (or `READPAST` on SQL Server) inside one transaction that also writes ownership | `io.mohs.store.jdbc.delegate.JdbcDelegate` |
| A node dies mid-execution. What happens to the work? | Its node lease expires; a peer's reaper reclaims the ownership row and reschedules through the retry budget | `Engine#reapOrphanedLeases` |
| The dead node comes back and finishes the job. | Every write over owned work is fenced by `(execution_id, node_id, claim_token, attempt)`; the zombie's result is discarded | `io.mohs.engine.LeaseStore` |
| The clock jumps backwards. | Database time arbitrates sessions, refill, expiry and eligibility; local deadlines are monotonic. Clock skew may delay progress and is observable. | JdbcDatabaseTime / Engine |
| The same request is submitted twice. | `Idempotency-Key` deduplication via a primary-key conflict on `mohs_idempotency` | `io.mohs.store.jdbc.JdbcSubmissionStore` |

## Delivery and external effects

Retries authorize a finite number of further attempts. A crash can exhaust that budget before a handler runs successfully. Zero retries suppresses automatic recovery attempts; it does not establish exactly-once external effects or forbid a new manual submission.

An external effect may complete before the scheduler persists its result, and a later attempt can repeat it. Scheduler fencing protects scheduler state. Business idempotency, a destination-enforced token or a suitable application transaction must protect effects that cannot repeat. Listeners are best effort and do not provide durable continuation delivery.

## Boundaries — what Mohs is not

| Not | Why |
| --- | --- |
| A standalone scheduler server | There is no server artifact, no Dockerfile, no deployment manifest. `mohs-demo` is a development application, explicitly never published. |
| A message broker | Persistence is a relational database via JDBC. There is no AMQP/Kafka/JMS dependency anywhere in the reactor. |
| A workflow engine | There are no DAGs, no step dependencies, no compensation. A batch is a flat set of independent members with a shared counter. |
| A distributed lock service | Coordination protects the scheduler catalog, triggers, sessions, admission and results. There is no leader election and no consensus protocol. |
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
3. **Progress is bounded and rotated.** Candidate and maintenance cursors continue across rounds, and only a positively selected eligible set reaches claim.
4. **Measurement outranks intuition.** Optimisations in the tree carry before/after numbers in the
   source comments, and several candidate optimisations were implemented, measured, and reverted.

## Current maturity

- Version `0.0.1-SNAPSHOT`; the repository contains CI and a manual release workflow; see [build and publishing](../12-build/modules.md).
- Database tiers, from `MohsAutoConfiguration#mohsJdbcDelegate`: PostgreSQL, MySQL 8.0+ and SQL
  Server are production dialects; **H2 is explicitly a test/dev tier** and logs a WARN at boot when
  selected.
- The REST API and the dashboard are **off by default** and must be switched on deliberately.
