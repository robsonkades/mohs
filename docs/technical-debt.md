# Technical debt

Status: Active · Last Reviewed: 2026-09-04 · Source of Truth: Repository

> **Only open items are listed.** Eleven were closed in the 2026-08-29 release-readiness pass
> (TD-01, TD-03, TD-04, TD-05, TD-07, TD-08, TD-10, TD-13, TD-16, TD-17, TD-20) and their entries
> have been removed rather than kept as an archive — what was fixed is visible in the code and the
> changelog. **Numbers are never reused**, so a gap in the sequence means an item that is gone.
>
> **Changed on 2026-09-04 (later).** **TD-09 is closed by removal** — `JobContext#progress` is gone
> from the public API. Nothing ever observed it (no store column, no REST field, no dashboard
> component), and a published method that promises a dashboard signal it cannot deliver is worse
> than no method. Nothing has been released, so there is no compatibility to keep. Its Javadoc was
> Portuguese, and so were seven more comments in `OnDemandJob`, `RecurringJob`, `Engine` and
> `MohsImpl` that the same day's review found and translated — the claim below that the Java is
> done became exact on 2026-09-04, not before. No Medium item remains except the SQL prose.
>
> **Changed on 2026-09-04.** **TD-11 is closed** — not with the `node` label, whose deferral stands,
> but by making the failure loud: `EngineMetrics#bindNodeGauges` and `bindQueueDepthGauge` now throw
> at construction when their gauge is already bound in the registry, naming the mitigation (one
> `MeterRegistry` per engine, with its own exporter). Micrometer's own WARN on the collision named
> neither the engine nor the fix; a dashboard showing one engine's saturation under the other's name
> was the part worth refusing.
>
> **Changed on 2026-09-01 (later).** **TD-02 is closed** — history retention shipped as
> `mohs.engine.history-retention` (DR-002): opt-in with no default window, swept hourly on the tick
> in bounded batches, seeking by the UUIDv7 primary-key range instead of a new index.
> `mohs_idempotency` stays on its own window by design. No High item remains.
>
> **Changed on 2026-09-01.** **TD-15 is closed** — `KeyGenerationScanTest` in `mohs-store-jdbc`
> now checks both halves of the UUIDv7 invariant: no `randomUUID` outside the UUIDv7 library in
> `src/main/java`, and no `IDENTITY`/`SERIAL`/`AUTO_INCREMENT`/`SEQUENCE` in any `.sql`. Store
> scope only — the reactor-wide ArchUnit reach it replaces is not restored. Also: **TD-06 is closed** — `READ_COMMITTED_SNAPSHOT` became a boot
> requirement of the SQL Server dialect (DR-001): `SqlServerRcsiRequirement` refuses to start
> without it, naming the exact `ALTER DATABASE` to run, and the two `WITH (NOLOCK)` hints on the
> idle-gate probe were retired — the probes are now byte-identical across the four dialects. Also:
> **TD-21 is removed by decision**: `mohs-demo` is an example
> application, and missing coverage there is not debt — the premise the item was built on was
> rejected. The end-to-end and architecture-rule coverage it described is not tracked here anymore;
> if a home for it ever exists, it will not be the demo. TD-19 loses its reference to it.
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
| **High** | 0 | — |
| **Medium** | 1 | TD-12 |
| **Low** | 2 | TD-18, TD-19 |
| **Total open** | **3** | |

## Medium

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

### TD-18 — The SQL Server clustered-key trade is unmeasured

`V8` moved `mohs_idempotency`'s clustered index to `created_at` because the composite `NVARCHAR` key
exceeds the 900-byte clustered limit. The change was **mandatory**, but its own header records both
sides and states that the balance has not been measured: the insert now maintains two structures, the
dedup `SELECT` becomes a seek plus key lookup, and monotonic `created_at` concentrates every node on
the last page (`PAGELATCH_EX`).

### TD-19 — The dashboard is entirely untested

`package.json` defines `dev`, `build`, `typecheck` and `preview`. There is **no `test` and no `lint`
script**, and no test framework in `devDependencies`. `tsc -b` is the only automated check on roughly
90 TypeScript files. Nothing catches a dashboard that ships broken.

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
| 1 | **TD-19** — frontend tests | Medium |
| 2 | **TD-12** — finish the prose translation, now that the SQL is what an operator reads | Large, mechanical |
