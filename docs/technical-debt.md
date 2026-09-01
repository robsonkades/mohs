# Technical debt

Status: Active · Last Reviewed: 2026-08-31 · Source of Truth: Repository

> **Only open items are listed.** Eleven were closed in the 2026-08-29 release-readiness pass
> (TD-01, TD-03, TD-04, TD-05, TD-07, TD-08, TD-10, TD-13, TD-16, TD-17, TD-20) and their entries
> have been removed rather than kept as an archive — what was fixed is visible in the code and the
> changelog. **Numbers are never reused**, so a gap in the sequence means an item that is gone.
>
> **Changed on 2026-08-31.** **TD-14 is closed** — the now-query and its crossing became per-dialect
> and state UTC where the server is zoneless, so `mohs.time.mode=database` is supported on all four
> dialects instead of refused on two. The measurement the previous pass could not take is in
> `DatabaseClockZoneTest`. TD-12's count was wrong and is corrected: 30 of 34 `.sql` files, not 27
> of 30.
>
> **Changed on 2026-08-30.** Removing the migration engine and deleting `mohs-demo/src/test` moved
> two items and opened one: TD-12 shrank to the SQL files alone, TD-15 stopped being half-enforced
> and became unenforced, and **TD-21 is new** — the architecture rules and the only end-to-end
> coverage went with those tests.

Every item was found by reading the source, the tests and the build. Each has evidence, an impact and
a recommended fix. Nothing here is speculative.

## A note on what is *not* here

A repository-wide scan for `TODO`, `FIXME`, `HACK` and `XXX` across Java, SQL and TypeScript found
**zero genuine markers**. There are four hits: three are the Portuguese word "todo" (meaning "every")
inside SQL comments, and the fourth is a Javadoc sentence *warning against* a TODO disguised as a
test.

There are **no** `@Deprecated` elements in the tree: the one that existed — a compatibility
constructor on `JobDefinition` — was removed, because nothing has been released yet and there is no
compatibility to keep.

Most of what follows is therefore **deliberate deferral**, recorded in the code itself, rather than
neglect. Where the code already names a gap, this document quotes it.

## Severity summary

| Severity | Count | Items |
| --- | --- | --- |
| **Critical** | 0 | — |
| **High** | 3 | TD-02, TD-06, TD-21 |
| **Medium** | 3 | TD-09, TD-11, TD-12 |
| **Low** | 3 | TD-15, TD-18, TD-19 |
| **Total open** | **9** | |

## High

### TD-02 — Nothing prunes execution history

| | |
| --- | --- |
| **Problem** | `mohs_execution`, `mohs_attempt`, `mohs_batches` and `mohs_idempotency` grow forever. There is no policy, no scheduled task and no property |
| **Evidence** | The only purge in the tree is `Engine#purgeStaleNodeRows`, for `mohs_nodes`. `V3__table_split.sql`'s header names retention as a later phase; `V5__drop_partitioning.sql` removed the partitioning that would have served it |
| **Impact** | Unbounded storage growth; payloads retained indefinitely, which may be a data-protection obligation |
| **Fix** | Ship a retention policy, or document the operator's obligation prominently. **The second is done** — see [data lifecycle](06-data/data-lifecycle.md) |

### TD-06 — `GET /overview`'s counts lost the lock-free read hint on SQL Server

| | |
| --- | --- |
| **Problem** | `countActiveByState` and `countTerminalOutcomesSince` were rewritten over the split tables and **carry no lock-free read hint**. On SQL Server without RCSI they take shared locks on all three hot tables |
| **Evidence** | `SqlServerJdbcDelegate` spells `WITH (NOLOCK)` into `visibleWorkExists` and `visibleWorkCount` — the idle gate — and nowhere else. `OverviewStreamBroadcaster`'s Javadoc records the regression explicitly |
| **Impact** | **Measured on SQL Server 2022 with RCSI off, which is the default.** With one uncommitted claim holding X locks on 1000 `mohs_ready` rows, `countActiveInQueue` did not merely contend — it **blocked to a lock timeout** (`Msg 1222`). The idle gate, which carries `WITH (NOLOCK)`, answered immediately. With `READ_COMMITTED_SNAPSHOT ON` the same count answered correctly and without blocking. The SSE stream runs this every 2 s |
| **Fix** | **Not by spreading the hint.** In the same measurement `NOLOCK` read a depth of 49,000 against 50,000 committed — it saw the uncommitted `DELETE` of a claim that then rolled back, which is a wrong number indistinguishable from a right one on an operational dashboard. RCSI fixes both sides and would retire the existing `NOLOCK` too. The open decision is whether to make it a **documented requirement** of the SQL Server dialect, refused loudly at boot rather than degraded silently |

### TD-21 — There is no end-to-end test, and no executable architecture rule

| | |
| --- | --- |
| **Problem** | `mohs-demo/src/test` was deleted on 2026-08-30. It held the ArchUnit suite (12 rules) and the only tests that loaded the assembled application: a full context boot with the whole example catalogue, the dashboard integration and the runners endpoint |
| **Evidence** | `mohs-demo` has `src/main` only, and its `pom.xml` no longer declares a test dependency. The reactor went from 778 to 753 tests |
| **Impact** | Two distinct holes. **Wiring**: every bean is tested in isolation and nothing asserts they assemble — a defect in the auto-configuration path a real consumer takes now reaches the first application that tries the starter, not CI. This is exactly the class of bug that shipped once already: a controller implemented, contract-tested, and never registered, so the route answered the host's 404. **Invariants**: six rules that used to fail the build are now convention — the engine reading the wall clock directly, `UUID.randomUUID`, `ThreadLocal` and `synchronized` methods on concurrent paths, missing `@NullMarked`, and package cycles. Each compiles cleanly and passes every remaining test |
| **Why it happened** | Deliberate: the demo is an example application, and tests were not wanted there. The debt is the coverage, not the decision |
| **Fix** | Both holes need a module that sees every other on one classpath, which is what `mohs-demo` was. Either restore a test source set there, or add a `mohs-integration-tests` module that is never published. The cheaper partial fix for the invariants alone is a source scan inside each module, next to `TerminalStateWriteScanTest` — see TD-15 |

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

### TD-12 — The SQL comments are still in Portuguese

**The Java is done**: no `.java` file in the reactor carries Portuguese prose, and `mohs-ui` is
English throughout. What remains is the SQL — **30 of the 34 `.sql` files**: the four
`schema-*.sql` installers and 26 of the 30 files in the per-database delta chain. The four already
in English are the `V9__due_trigger_index.sql` series, written in English from the start.

That subtree matters more than it did. With no migration engine, those files are what an **operator**
opens and applies by hand, so their comments are user-facing documentation now, not internal notes.

Migration is deferred with **no date**, and the reason is unchanged: that prose is the record of why
a migration is shaped the way it is, and a careless pass would lose the argument.


---

## Low

### TD-15 — The UUIDv7 invariant is not enforced at all

It used to be half enforced: ArchUnit forbade `UUID.randomUUID` (calls **and** method references),
while the other half — no `IDENTITY`, `SERIAL`, `AUTO_INCREMENT` or `SEQUENCE` in any schema — was
prose, because ArchUnit does not read SQL. The ArchUnit half went with `mohs-demo/src/test`, so today
**neither half is checked**.

Both are closable by a source scan in `mohs-store-jdbc`, next to the ones that already run there
(`TerminalStateWriteScanTest`, `SqlServerUnicodeScanTest`): one pass over `src/main/java` for
`randomUUID`, one over the `.sql` files for the four forbidden keywords. That is the cheapest way to
get the invariant back without restoring a whole-classpath module.

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

It got worse on 2026-08-30: `MohsUiIntegrationTest` — which at least asserted that the bundle was
served and that the hashed assets the index references actually resolve — went with
`mohs-demo/src/test`. Nothing now catches a dashboard that ships broken. See TD-21.

## Documented limitations that are *not* debt

Deliberate decisions with recorded reasoning. Listed here so nobody files them as bugs.

| Limitation | Why it is a decision |
| --- | --- |
| **Priority starvation**: `BACKGROUND` can starve under sustained higher-priority load | Documented in `Priority`'s Javadoc. Aging was not implemented |
| **64-node ceiling** on claiming nodes | `SHARD_COUNT` is fixed by decision; 64 divides cleanly into any plausible cluster |
| **Cross-node dispatch latency** bounded by `max-poll-interval` | The NOTIFY tier that would close it was measured and withdrawn |
| **No dead-letter queue** | Exhausted retries are terminal `FAILED` rows in history, queryable and manually retryable |
| **No circuit breaker** over the database | Opening one would stop the heartbeat, which is exactly what must not stop |
| **`onCompletion` is JVM-local** | Stated in the contract; the outbox pattern is prescribed instead |
| **The REST API is unauthenticated** | Off by default, and turning it on WARNs at boot |
| **`ExecutionWindow` equality is identity-based** | Predicates are lambdas; true value semantics would require modelling exclusions as sealed data |
| **No ordering guarantee between execution events** | Delivery is asynchronous per listener, by contract |
| **The group-commit durability window** (up to 5 ms) | Declared; the opt-out exists |
| **The `SUSPEND` scenario's mid-claim-transaction mode** | Pre-existing; the database-side mitigation is named |
| **Engine tests living in `mohs-store-jdbc/src/test`** | They need a real store, and they live where it is. It is also why that module has no `module-info` |

## Suggested order of work

| Priority | Item | Effort |
| --- | --- | --- |
| 1 | **TD-21** — a home for the end-to-end tests and the architecture rules | Medium; the rules themselves already exist in git history |
| 2 | **TD-06** — decide: restore the hint, or make RCSI a requirement on SQL Server | Small |
| 3 | **TD-02** — a retention policy | Medium; needs per-dialect batched deletes |
| 4 | **TD-15** — a source scan for `randomUUID` and for `IDENTITY`/`SERIAL`/`AUTO_INCREMENT`/`SEQUENCE`, inside `mohs-store-jdbc` | Small, and it does not wait on TD-21 |
| 5 | **TD-19** — frontend tests | Medium |
| 6 | **TD-11** — a `node` label on the `mohs.node.*` gauges, or a loud failure on the second bind | Small |
| 7 | **TD-09** — deliver `JobContext#progress`, or remove it | Small |
| 8 | **TD-12** — finish the prose translation, now that the SQL is what an operator reads | Large, mechanical |
