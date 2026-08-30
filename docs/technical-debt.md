# Technical debt

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

> **Resolved on 2026-08-29**, in the release-readiness pass: TD-01, TD-03, TD-04, TD-05, TD-07,
> TD-08, TD-10, TD-13, TD-14 (through a boot guard), TD-17 and TD-20. Each entry below carries what
> happened. Nothing was closed by lowering a bar — the two items that changed shape rather than
> disappearing (TD-14, TD-17) say so explicitly.

Every item was found by reading the source, the tests and the build. Each has evidence, an impact and
a recommended fix. Nothing here is speculative.

## A note on what is *not* here

A repository-wide scan for `TODO`, `FIXME`, `HACK` and `XXX` across Java, SQL and TypeScript found
**zero genuine markers**. There are four hits: three are the Portuguese word "todo" (meaning "every")
inside SQL comments, and the fourth is a Javadoc sentence *warning against* a TODO disguised as a
test.

There is exactly **one** `@Deprecated` element in the whole tree.

Most of what follows is therefore **deliberate deferral**, recorded in the code itself, rather than
neglect. Where the code already names a gap, this document quotes it.

## Severity summary

| Severity | Count | Items |
| --- | --- | --- |
| **Critical** | 0 | — (TD-01 resolved) |
| **High** | 2 | TD-02, TD-06 (TD-03, TD-04, TD-05 resolved) |
| **Medium** | 4 | TD-09, TD-11, TD-12, TD-14 (reduced to a boot guard) |
| **Low** | 4 | TD-15, TD-16, TD-18, TD-19 |

---

## Resolved

### TD-01 — `BatchesController` is implemented and tested, but never registered — **RESOLVED**

`MohsRestAutoConfiguration` now declares `mohsBatchesController`, and the stale Javadoc claiming
batches "remains a contract with no implementation" is gone. The recurrence guard is
`MohsRestAutoConfigurationTest#everyRestControllerInThePackageIsRegisteredWhenTheApiIsOn`: it scans
`io.mohs.rest` for `@RestController` and asserts a bean for each — the class of defect a slice test
cannot catch, because a slice builds the controller itself.

### TD-03 — the idempotency prune is never called — **RESOLVED**

`mohs.engine.idempotency-retention` (default `7d`) IS the deduplication window, and the engine's
tick prunes on an hourly cadence. `0s` opts out and keeps every key forever, which is the previous
behaviour, now chosen rather than inherited.

### TD-04 — no health indicator — **RESOLVED**

`MohsHealthIndicator`, contributed under the `mohs` key when the host brings the actuator
(`spring-boot-health` is an `optional` dependency of the starter). `RUNNING` → `UP`,
`PAUSED`/`DRAINING` → `OUT_OF_SERVICE`, `CREATED`/`STOPPED` → `DOWN`. **It never touches the
database.**

### TD-05 — no CI, no coverage, no static analysis — **RESOLVED**

`.github/workflows/maven.yml` runs `mvn -B -ntp verify` — with the frontend — on every push and pull
request; `codeql.yml` runs static analysis on every push and weekly; `dependabot.yml` keeps the
three ecosystems current. JaCoCo produces a report per module. **No coverage threshold**, on
purpose: a limit chosen before the first measurement is a number invented to be met.

### TD-07 — `@OnExecution` is public API with no implementation — **RESOLVED**

Implemented. An annotated method becomes a subscriber of the engine's event stream through
`OnExecutionRegistry`, with `ExecutionListener`'s contract (asynchronous, best-effort, unordered).
`job()` now defaults to empty, meaning every job, which is also what makes `BATCH_COMPLETED`
observable. A signature that cannot receive its declared event still fails the boot.

### TD-08 — `retryPolicy` is accepted, persisted, and ignored — **RESOLVED**

`io.mohs.core.execution.RetryPolicy` is the SPI. It replaces the budget while it returns a delay, is
consulted on both failure paths (a handler that threw and a lease reclaimed from a dead node — the
latter with a `null` error), and a job naming a bean that does not exist fails the boot instead of
falling back silently. A policy that throws or returns a negative delay falls back to the built-in
backoff, logged: a bug in someone's Strategy must not strand an execution in `RUNNING`.

### TD-10 — no queue-depth metric — **RESOLVED**

`mohs.queue.depth`, sampled by the engine's tick every 10 s and published as a gauge — never queried
on the scrape, so a metrics pipeline cannot decide the load it is measuring. The number is
cluster-wide: aggregate with `max`, never `sum`.

### TD-13 — a user-facing string in Portuguese — **RESOLVED**

`RuntimePatchResponse.BOOT_REVERSION_NOTICE` is English.

### TD-17 — PostgreSQL primary keys lead with a time column — **RESOLVED, verified against a real database**

`mohs_execution` is keyed on `(execution_id)` and `mohs_attempt` on `(execution_id, number)`, the
same shape the other three dialects always had. The normalisation rides inside `V5`, which already
recreates and copies both tables — doing it later would have cost a second full copy, with the same
outage window, on somebody's production database. `idx_mohs_execution_id` and `idx_mohs_attempt_exec`
are gone (they became the keys), the two terminal-`UPDATE` constants collapsed into one, and
`executionCreatedAt` no longer travels from the claim to the completion. It **changes a guarantee**:
`execution_id` is now unique by the schema, and `V5` checks for a pre-existing duplicate before
copying so the failure names the row instead of saying "duplicate key".

Verified 2026-08-30 against real PostgreSQL, MySQL and SQL Server (Testcontainers): the whole suite
is green — 758 tests, 0 failures, 0 errors — including the structural guard that compares the Flyway
path against `schema-postgresql.sql`, which is what proves the two installation paths still converge
after the key changed, and the two new tests that pin the uniqueness the schema now enforces.

### TD-20 — no release process — **RESOLVED**

`<scm>`, `<developers>`, `<issueManagement>`, `distributionManagement` (Central Portal — OSSRH was
retired in June 2025), `project.build.outputTimestamp`, sources and Javadoc jars on every build, GPG
signing behind `gpg.skip` (default `true`, overridden by the release workflow), and
`central-publishing-maven-plugin` with `autoPublish=false`. `mohs-demo` and `mohs-benchmark` are
excluded through `maven.deploy.skip` **and** `skipPublishing`. `.github/workflows/release.yml` is the
manual pipeline. `CHANGELOG.md` exists. Two prerequisites remain outside the repository: the
`io.mohs` namespace verified in the Portal against the DNS of mohs.io, and a published GPG key.

---

## High

### TD-02 — Nothing prunes execution history

| | |
| --- | --- |
| **Problem** | `mohs_execution`, `mohs_attempt`, `mohs_batches` and `mohs_idempotency` grow forever. There is no policy, no scheduled task and no property |
| **Evidence** | The only purge in the tree is `Engine#purgeStaleNodeRows`, for `mohs_nodes`. `V3__table_split.sql`'s header names retention as a later phase; `V5__drop_partitioning.sql` removed the partitioning that would have served it |
| **Impact** | Unbounded storage growth; payloads retained indefinitely, which may be a data-protection obligation |
| **Fix** | Ship a retention policy, or document the operator's obligation prominently. **The second is done** — see [data lifecycle](06-data/data-lifecycle.md) |
| **Decision record** | [DR-016](15-design-decisions/records/DR-016-no-automatic-history-retention.md) |

### TD-06 — `GET /overview`'s counts lost the lock-free read hint on SQL Server

| | |
| --- | --- |
| **Problem** | `countActiveByState` and `countTerminalOutcomesSince` were rewritten over the split tables and **no longer use `JdbcDialect#lockFreeReadHint`**. On SQL Server without RCSI they take shared locks on all three hot tables |
| **Evidence** | The only caller of `lockFreeReadHint` is `JdbcWorkQueue#hasVisibleWork`. `OverviewStreamBroadcaster`'s Javadoc records the regression explicitly |
| **Impact** | On SQL Server without `READ_COMMITTED_SNAPSHOT`, dashboard polling contends with the claim and completion hot path — and the SSE stream polls every 2 s |
| **Fix** | Either restore the hint on those counts, or document `READ_COMMITTED_SNAPSHOT ON` as a **requirement** for SQL Server rather than a recommendation |

---

## Medium

### TD-09 — `JobContext#progress` is a no-op

Published API, documented as "optional, dashboard-oriented; a no-op if nothing observes it". Nothing
observes it. A handler calling it in a loop pays a virtual call for nothing and gets no dashboard
signal.

### TD-11 — `mohs.node.*` gauges have no `node` label

Recorded in `EngineMetrics#bindNodeGauges`: with two engines in the same registry (one Mohs per
`DataSource`), they collide on the meter id and Micrometer **silently ignores the second bind**. The
deferral is deliberate — the trigger is the first real multi-engine scenario, paying the cardinality
only then — but the failure is silent, which is the part worth revisiting.

### TD-12 — Mixed prose language in the codebase

The majority of Javadoc has been translated to English, but a minority of comments and short Javadoc
lines remain in Portuguese, sometimes within the same file (for example `Misfire`, `MohsJob`,
`RecurringJob`). `mohs-ui` is English throughout, deliberately. The project's own rule says one
bilingual subtree is worse than either language; the same argument applies to a bilingual file.
Migration is deferred with **no date**.

### TD-14 — `DatabaseClock` misreads time on SQL Server — **reduced to a boot guard**

`mohs.time.mode=database` on SQL Server now fails the boot naming `mohs.time.mode=application` as
the alternative, so the misconfiguration is no longer silently available. The clock itself is
**unchanged**: the real fix is a per-dialect now-query (`SYSUTCDATETIME()`), which alters the
clock's behaviour and needs its own decision — and could not be verified in this pass, with no
container to run SQL Server against. What follows is the original finding, still accurate about the
clock.

Recorded in `DatabaseClock#sync`: on SQL Server, `CURRENT_TIMESTAMP` is a zoneless `DATETIME`
interpreted in the JVM's zone. The comment notes the fix is sketched out and needs approval because it
changes behaviour. Until then, `mohs.time.mode=database` **should not be used on SQL Server** — and
nothing in the code prevents it.

---

## Low

### TD-15 — The UUIDv7 invariant is only half enforced

ArchUnit forbids `UUID.randomUUID` (calls **and** method references). The other half — no `IDENTITY`,
`SERIAL`, `AUTO_INCREMENT` or `SEQUENCE` in any schema — is prose, because ArchUnit does not read SQL.
The rule's own Javadoc declares the gap. A SQL scan over `schema-*.sql` and the migration folders
would close it.

### TD-16 — The `synchronized`-block gap in the architecture rules

`no_synchronized_methods_in_concurrency_critical_code` catches only the **method modifier**. A
`synchronized (lock) { … }` block is not modelled by ArchUnit, which has no instruction-level bytecode
inspection in its public API. Declared in the rule's Javadoc.

### TD-18 — The SQL Server clustered-key trade is unmeasured

`V8` moved `mohs_idempotency`'s clustered index to `created_at` because the composite `NVARCHAR` key
exceeds the 900-byte clustered limit. The change was **mandatory**, but its own header records both
sides and states that the balance has not been measured: the insert now maintains two structures, the
dedup `SELECT` becomes a seek plus key lookup, and monotonic `created_at` concentrates every node on
the last page (`PAGELATCH_EX`).

### TD-19 — The dashboard is entirely untested

`package.json` defines `dev`, `build`, `typecheck` and `preview`. There is **no `test` and no `lint`
script**, and no test framework in `devDependencies`. `tsc -b` is the only automated check on roughly
90 TypeScript files.

## Documented limitations that are *not* debt

Deliberate decisions with recorded reasoning. Listed here so nobody files them as bugs.

| Limitation | Why it is a decision |
| --- | --- |
| **Priority starvation**: `BACKGROUND` can starve under sustained higher-priority load | Documented in `Priority`'s Javadoc. Aging was not implemented |
| **64-node ceiling** on claiming nodes | `SHARD_COUNT` is fixed by decision; 64 divides cleanly into any plausible cluster |
| **Cross-node dispatch latency** bounded by `max-poll-interval` | The NOTIFY tier that would close it was measured and withdrawn ([DR-008](15-design-decisions/records/DR-008-adaptive-poll-and-the-withdrawn-notify-tier.md)) |
| **No dead-letter queue** | Exhausted retries are terminal `FAILED` rows in history, queryable and manually retryable |
| **No circuit breaker** over the database | Opening one would stop the heartbeat, which is exactly what must not stop |
| **`onCompletion` is JVM-local** | Stated in the contract; the outbox pattern is prescribed instead |
| **The REST API is unauthenticated** | [DR-013](15-design-decisions/records/DR-013-rest-api-off-by-default-no-auth.md) |
| **`ExecutionWindow` equality is identity-based** | Predicates are lambdas; true value semantics would require modelling exclusions as sealed data |
| **No ordering guarantee between execution events** | Delivery is asynchronous per listener, by contract |
| **The group-commit durability window** (up to 5 ms) | Declared; the opt-out exists |
| **The `SUSPEND` scenario's mid-claim-transaction mode** | Pre-existing; the database-side mitigation is named |
| **Engine tests living in `mohs-store-jdbc/src/test`** | They need a real store, and they live where it is. It is also why that module has no `module-info` |

## Suggested order of work

| Priority | Item | Effort |
| --- | --- | --- |
| 1 | **TD-06** — decide: restore the hint, or make RCSI a requirement on SQL Server | Small |
| 2 | **TD-02** — a retention policy | Medium; needs per-dialect batched deletes |
| 3 | **TD-14** — the per-dialect now-query, so `mohs.time.mode=database` works on SQL Server rather than being refused | Medium; changes behaviour |
| 4 | **TD-19** — frontend tests | Medium |
| 5 | **TD-11** — a `node` label on the `mohs.node.*` gauges, or a loud failure on the second bind | Small |
| 6 | **TD-09** — deliver `JobContext#progress`, or remove it | Small |
| 7 | **TD-15 / TD-16** — close the two half-enforced invariants (a SQL scan; instruction-level bytecode) | Small |
| 8 | **TD-12** — finish the prose translation | Large, mechanical |
