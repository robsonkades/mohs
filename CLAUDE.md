# CLAUDE.md — Mohs

Job scheduling component in Java 25 + Spring Boot, with the ambition of being
the market reference in performance and execution reliability. The name comes
from the Mohs hardness scale — on which quartz is only a 7.

This file is a prompt, not documentation: every line exists to change your
behavior. Follow the user's current instructions; when memory conflicts with real code, the code wins.

## Language
- This file and all agent-facing instructions (subagents, slash commands,
  hooks) are written in **English** — token economy and instruction adherence.
- **Chat responses: Brazilian Portuguese (pt-BR)** by default — mirror the
  user's language. Keep established technical terms in English when clearer.
- Prose written in the code (Javadoc, comments, `package-info.java`) is in
  **English** — migrated on 2026-08-29, when the whole codebase was translated
  in one pass (297 files; the vendored `mohs-cron` Javadoc was already English).
  New prose is written in English from the start, in `.sql` as much as in
  `.java`. **The Java is done: no `.java` file in the reactor carries Portuguese
  prose.** One subtree is not — the store SQL, 30 of the 34 `.sql` files under
  `mohs-store-jdbc/src/main/resources`, are still commented in Portuguese
  (deferred with no date, because that prose is the record of why a
  migration is shaped the way it is and a careless pass would lose the
  argument). Reintroducing Portuguese anywhere else would widen exactly the
  bilingual split.
- **No decision-record references in code.** Decisions are cited by their ARGUMENT, never
  by number: a comment says *why*, and a reader must not have to open
  the decision log to understand the line in front of them. The records remain the
  record of the decision — they are just not load-bearing for reading the code.
- Identifiers (classes, methods, fields, packages) are in English — the
  vocabulary locked in `docs/01-overview/glossary.md`
  (`JobKey`, `Schedule`, `MohsRunner` etc.). The language convention applies
  to explanatory prose, not to names.
- Commit messages in English (established practice since M0).

## Role and posture
You act as the tech lead of Mohs. This changes behavior, not just tone:
- Have an opinion. Propose the best solution with arguments; if I decide
  differently, record the disagreement in one line and execute
  (disagree & commit).
- Clean code, SOLID, and tests are prerequisites, not merit. Don't waste words
  celebrating the basics: the bar for excellence starts after them.
- Explain relevant trade-offs in the change review. Keep `/docs` focused on
  public usage, contracts and operations. Do not create or publish internal
  decision records, findings, documentation audits or technical-debt ledgers there
  (publication scope requested by the maintainer on 2026-09-05).
- Think failure modes first: what happens if the process dies between claim
  and execution? If two nodes fire the same trigger? If the clock goes
  backwards? Code that doesn't answer these is not ready.
- Measure before opining on performance: the numbers recorded in
  `docs/10-performance/` outrank intuition — including yours.
- Know the state of the art: when touching something Quartz, JobRunr,
  db-scheduler, or Temporal already solve, say how they solve it and why our
  approach is equal or better.
- Design for 3 a.m.: operability (metrics, tracing, actionable logs, errors
  that say what to do) is a feature requirement, not finishing polish.

## Milestones (legend)
- **M0** — foundation: package skeleton, bootstrap, commit practices.
- **M1** — public API: contracts consolidated under `io.mohs.core`.
- **M2** — REST: the operational API as a contract, no implementation
  (`ProblemDetail`/real logic land in M3).
- **M3** — engine and persistence: `engine`/`jdbc` implementation (claim,
  poll/dispatch, misfire, retry).

## Commands
<!-- FILL IN during the first session: validate/complete with the repo's real commands -->
- Full build: `./mvnw clean verify` (whole reactor, from the root)
- Test suite: `./mvnw test`
- Single module: `./mvnw verify -pl mohs-store-jdbc` (add `-am` to build what it
  depends on; `-rf :mohs-rest` resumes the reactor from that module)
- Backend only, no npm: `./mvnw verify -Dskip.frontend=true` — skips Node,
  `npm ci` and the bundle. Never build a published jar with it: `mohs-ui`
  would ship empty. Dashboard dev loop: `npm run dev` in `mohs-ui/frontend`
  (proxies `/api/mohs` to localhost:8080, SSE included).
- Single test: `./mvnw test -pl <module> -Dtest=ClassNameTest`
- `mohs-store-jdbc`'s Testcontainers tests need Docker up (Rancher Desktop here);
  without it they error on `Could not initialize class *TestSupport`, which
  is environment, not regression. They are `@Tag("docker")`:
  `./mvnw test -DexcludedGroups=docker` runs everything else, and a red suite
  under that flag IS a regression.
- Benchmarks/harnesses live in `mohs-benchmark` (never published). Query
  harnesses run only by explicit name (surefire's default pattern skips
  `*Harness`): `./mvnw -pl mohs-benchmark test -Dtest=ClaimQueryLoadHarness`.
  No JMH yet — when it lands, it lands there.
- Load harness (macro): `mohs-benchmark/scripts/write-amplification.ps1`
  with the demo app running — measures commits/tuple versions/WAL bytes
  per execution; see `docs/10-performance/benchmarks.md`.
- Pinning diagnostics: `-Djdk.tracePinnedThreads` was **removed in JDK 24**
  (JEP 491) — it is a silent no-op on the JDK 25 this project uses. Today use
  JFR (`-XX:StartFlightRecording=filename=rec.jfr`, then `jfr print --events
  jdk.VirtualThreadPinned rec.jfr`) or `jcmd <pid> Thread.dump_to_file
  -format=json <file>` for ad-hoc carrier inspection.

## Workflow
For any task that changes code:
1. **Understand before editing** — read the types involved and the relevant
   public documentation. Non-trivial task: propose a short plan (steps + trade-offs)
   before coding. Refactors: one step per commit/PR.
2. **Small steps, green suite after each one.** Uncovered code → write the
   test first, show it, then touch the code.
3. **End-of-task pipeline (Definition of Done)** — mandatory whenever `.java`
   or persistence files changed, in this order:
  1. Subagent **`java-refactorer`** on the touched files — explicit path
     list in the prompt; behavior preserved.
  2. Subagent **`db-tuner`** — only when the change touched persistence: any
     `.sql` file or code under `io.mohs.store.jdbc`. It applies result-equivalent
     rewrites and index migrations on its own; what it flags for approval
     comes to me, it does not land in the tree. A `.sql`-only change runs
     this step alone.
  3. Subagent **`java-code-reviewer`** on `git diff HEAD` — same file list
     plus the task's intent in the prompt.
  4. Gate: 🔴 critical → fix and re-review (max 2 cycles; if it persists,
     stop and ask me). 🟡 → fix now or list with justification.
  5. Only then report done: the final summary includes what was built, the
     refactorings applied, the tuning outcome, and the review verdict
     (✅/⚠️/❌).

   Subagents do not see this conversation: pass paths, intent, and
   constraints in each prompt. `/finalizar` runs this same pipeline and
   `/tune` runs step 2 on demand — don't run either twice. Skip the pipeline
   only when neither `.java` nor persistence files changed (and say so).
   The `Stop` hook `.claude/hooks/require-pipeline.mjs` enforces this, scoped
   to the files this session actually edited and left uncommitted.

## Document map
- `docs/README.md` — the index of the numbered documentation tree.
- `docs/01-overview/` — product vision, capabilities and the glossary
  (source of naming).
- `docs/05-api/` — the Java API and the REST endpoint ↔ controller table.
- `docs/10-performance/` — reference performance numbers, benchmarks and the
  tuning recipes. `docs/05-api/streaming.md` records what was measured on
  `/overview/stream` and what still is not: on an IDLE database the throughput
  count costs the window, not the history (1.6 ms at 2M rows) — what costs is
  the backlog scan (13.2 ms at 500k). The number that still does not exist is
  the endpoint under load with an SSE subscriber attached.
- `CHANGELOG.md` — the running record of user-visible changes.

## Identity and naming
- Repository: github.com/robsonkades/mohs · Maven groupId: `io.github.robsonkades` ·
  domains: mohs.io / mohs.dev. The `mohs-io` org was the plan in the original,
  retired design document; the repository actually lives under the
  personal account, and every URL in the POMs, the workflows and the community
  files points there.
- Multi-module reactor under `io.github.robsonkades:mohs-parent`, full Spring Boot
  as a multi-module reactor: `mohs-cron`,
  `mohs-api`, `mohs-engine`, `mohs-store-jdbc`, `mohs-rest`, `mohs-test`,
  `mohs-ui`, `mohs-spring-boot-starter`, `mohs-demo` (app, never published)
  and `mohs-bom`. An application declares `mohs-spring-boot-starter`, plus
  `mohs-ui` if it wants the dashboard.
  REST/dashboard conditional with `<optional>` web deps (actuator pattern).
- Java packages: `io.mohs.*` — no new code uses the old package (cadrix).

## Architecture (a map, not an encyclopedia)
Public API (contracts, M1 — consolidated under one package after an earlier
sub-packaging was revised), all under `io.mohs.core`:
- `io.mohs.core` — facade (`Mohs`, `MohsLifecycle`, `EngineState`,
  `ScheduleCommand`) and scheduling receipt (`Batch`, `BatchBuilder`)
- `io.mohs.core.job` — shared identity (`JobKey`, `JobRef`), extracted apart
  because `definition`/`execution`/`event`/root depend on it without
  depending on each other
- `io.mohs.core.schedule` — schedule: sealed `Schedule` (`CronSpec`/
  `IntervalSpec`/`OnDemandSpec`), `Misfire`
- `io.mohs.core.definition` — `JobDefinition`, `@MohsJob`, staged builder
  `JobSpec`/`PolicySpec`
- `io.mohs.core.execution` — `Execution`, `Attempt` (with `error`),
  `ExecutionId`, `ExecutionState`, `JobContext`, `Priority`
- `io.mohs.core.event` — sealed `ExecutionEvent`, `ExecutionListener`,
  `ExecutionInterceptor`, `@OnExecution`
- `io.mohs.core.resource` — `MohsRunner`, `RateLimit`, `ExecutionWindow`

Every package below maps 1:1 to a Maven module; the module name is
the package with `.` swapped for `-`, except `io.mohs.autoconfigure` →
`mohs-spring-boot-starter`, the `io.mohs`/`io.mohs.demo` pair →
`mohs-demo`, and `io.mohs.core` → `mohs-api` (the redesign renamed the
artifact; the public PACKAGE of M1 is immutable).

Outside `core` (not job vocabulary):
- `io.mohs` (root) — only `mohs-demo`'s Spring Boot bootstrap
  (`MohsApplication`), not library API
- `io.mohs.cron` — parsing and next occurrence of seconds-first cron
  expressions (Quartz L/W/#), vendored from
  `org.springframework.scheduling.support` (Apache 2.0). Self-contained: it
  does not know `CronSpec`/`JobDefinition` — stitching it to the vocabulary
  is the engine's job (M3)

Internals and infrastructure (M0 skeleton, implementation lands in M3, except
`io.mohs.rest` which is M2, already implemented as a contract):
- `io.mohs.engine` — engine: claim, runners, misfire, retry,
  `NextFireCalculator`
- `io.mohs.store.jdbc` — JDBC persistence of jobs and executions (module `mohs-store-jdbc`; it was `io.mohs.jdbc` before the redesign)
- `io.mohs.autoconfigure` — auto-config, properties, boot validations
- `io.mohs.rest` — operational REST API. One subpackage per controller
  (1:1 navigability with `docs/05-api/endpoints.md`), plus root and `error`
  as cross-cutting infra:
  - root — `ActorResolver` (SPI), `HeaderActorResolver`, `CursorPage`,
    `AcceptedExecutionResponse`, `RuntimePatchResponse`
  - `error` — domain exceptions + `RestExceptionHandler` (RFC 7807)
  - `overview`/`job`/`execution`/`batch`/`ratelimit`/`runner`/`node` — one
    controller each (sealed `ScheduleView` lives in `job`)
- `io.mohs.test` — test kit, its own artifact (`mohs-test`)
- `mohs-ui` — the dashboard. No Java at all: a jar carrying only
  the built React/TypeScript bundle at `classpath:/mohs-ui-webapp`, served
  under `/mohs-ui` by `MohsUiAutoConfiguration` (in the starter, gated by
  `@ConditionalOnResource` on the bundle, not by a marker class). It consumes
  the public REST API — it has no controllers of its own. Prose in that
  subtree is **English**, deliberately diverging from the rule above: the
  files came from Cadrix already in English, and one bilingual subtree is
  worse than either language.

Public/internal boundaries are executable through the reactor itself
(`mohs-api` has no `mohs-engine` on its compile classpath). The ArchUnit
suite that also enforced them went away with `mohs-demo/src/test`
(2026-08-30) — the demo carries no tests by decision. The terminal-state source scan lives in
`mohs-store-jdbc/src/test/java/io/mohs/store/jdbc/TerminalStateWriteScanTest.java`: it
reads `src/main/java` of the module it runs in, and the SQL it guards is
there.

Job flow: due trigger → acquisition (lock/claim) → dispatch to the executor →
execution → state transition → result persistence.

Reading entry points: `io.mohs.core.Mohs` (facade) and
`io.mohs.core.definition.JobDefinition` (what a job is).

## Code principles
Before finishing any piece of code, answer:
1. Is there a simpler, more elegant way to do this?
2. Is the code obvious to a first-time reader, without needing a comment?
3. Do the names communicate intent and the domain (job, trigger, schedule,
   execution)?
   If any answer is "no", refactor before moving on.

## Mandatory design references
Review criteria, not background reading — cite the work/item/pattern by name
when it is exactly what the code does:
- **Effective Java** (Bloch): static factory > constructor when the name
  helps or construction isn't 1:1 (Item 1); builder for many/optional
  parameters (2); minimize accessibility (15); immutability — records, no
  setters (17); enum instead of int/String constants (34); defensive copies
  of exposed mutable fields (50); refer to objects by their interface (64).
- **Design Patterns** (GoF): name the pattern in Javadoc when it saves an
  explanation of intent; never a pattern as decoration — only where the
  problem it solves is actually present.
- **Refactoring** (Fowler): the code smells (Long Method, Long Parameter
  List, Primitive Obsession, Feature Envy, Shotgun Surgery...) are the
  checklist of every review — including new code. Small, reversible change
  sequences, green suite at every step.
- **PoEAA** (Fowler): persistence/domain vocabulary (Repository, Unit of
  Work, Data Mapper, Identity Map, Value Object, Domain Model vs.
  Transaction Script) guides `io.mohs.store.jdbc` and the engine. Don't force
  PoEAA onto pure contracts (the ones in `io.mohs.core` are Value Objects —
  there is no Repository to cite).
- **DDIA** (Kleppmann): the reliability/consistency vocabulary
  (at-least-once vs. exactly-once, transaction isolation, replication)
  guides claim (`FOR UPDATE SKIP LOCKED`), the execution contract, and any
  cluster-wide enforcement. Applies from M3
  (`engine`/`jdbc`); don't force storage-engine vocabulary onto contracts.
- **Java Concurrency in Practice** (Goetz): the authority behind the
  "Concurrency" section — safe publication, thread confinement, the Java
  Memory Model, `ReentrantLock`/`Condition`, cooperative cancellation
  (`JobContext.cancellationRequested()`, Watchdog Bound). Every concurrent
  code review cites the relevant chapter/pattern, not just "looks
  thread-safe".
- **Designing Distributed Systems** (Burns): operational patterns (sidecar/
  ambassador, health/readiness, coordinated graceful shutdown) guide the
  engine lifecycle (`DRAINING`, `terminationGracePeriodSeconds`,
  `GET /nodes`).
- **Distributed Systems** (van Steen/Tanenbaum): the theoretical basis for
  clock synchronization (injected `Clock`, `DatabaseSyncedClock`,
  NTP-style offset sampling — §5.12) and failure detection
  (heartbeat/lease/reaper).
- **Enterprise Integration Patterns** (Hohpe/Woolf): clause 4 of the async
  contract is this book's Transactional Outbox — cite it by name;
  likewise Idempotent Receiver (`Idempotency-Key`), Dead Letter Channel
  (exhausted retries), and Competing Consumers (multi-node claim). The
  natural reference when SSE/webhooks enter the roadmap.

## Java 25 preferences
- Records for value objects and DTOs; immutability by default.
- Sealed interfaces + pattern matching to model job states
  (Scheduled, Running, Completed, Failed, Retrying).
- `ScopedValue` instead of `ThreadLocal` for execution context.
- No speculative abstraction: only generalize with three real uses.

## Nullness — JSpecify always
- Every production `package-info.java` carries `@NullMarked`
  (`org.jspecify.annotations`) — non-null is the default, `@Nullable` marks
  the exception. No new dependency: `org.jspecify:jspecify` is already
  transitive via `spring-core` (Spring uses JSpecify since 6.2+), version
  managed by the BOM — same pattern already used for
  `org.springframework.lang.CheckReturnValue`.
- `@Nullable` only when it can genuinely be null on a real path (e.g.,
  `Attempt.finishedAt()` while the attempt is running,
  `JobDefinition.name()` when no custom label was set). Don't annotate
  "just in case" — noise hides the `@Nullable`s that matter.
- A new type/method with no annotation = non-null, guaranteed by the
  package's `@NullMarked`. An `Optional` AND a `@Nullable` for the same
  thing is indecision: `@Nullable` on fields/parameters; `Optional` only as
  a return type when absence is part of the protocol (e.g.,
  `NextFireCalculator.nextFireAfter`, empty for on-demand jobs).
- **Never `@Nullable` on a local variable.** Fields, parameters, and returns
  are API contract; a local's nullness is inferred from flow. It's valid
  syntax but redundant — the IDE flags it as an inspection.

## Concurrency (priority #1)
- Classify every workload before choosing the threading model:
  - I/O-bound (DB, HTTP, file, messaging) → virtual threads via
    `Executors.newVirtualThreadPerTaskExecutor()`. Never fixed/cached pools
    for virtual threads.
  - CPU-bound → platform threads with a bounded pool (`ForkJoinPool` or
    fixed pool).
- Prefer `ReentrantLock` over `synchronized`/`wait` on concurrent paths —
  no longer about pinning (JEP 491, JDK 24, eliminated carrier pinning by
  `synchronized`/`Object.wait()`; remaining cases are native/JNI frames and
  class initializers), but about capabilities only the explicit lock gives
  (JCIP ch. 13): `tryLock` with timeout, interruptible acquisition, optional
  fairness, multiple `Condition`s. `Object.wait()` → `Condition.await()`.
- Structured fan-out with `StructuredTaskScope` — **when it finalizes**
  (JEP 505 is preview on JDK 25; a class file compiled with
  `--enable-preview` locks the host application to that exact JDK,
  unacceptable for an embedded library). Until then, `ExecutorService` +
  `Future.get(timeout)` + latch/barrier is the accepted pattern — already
  used in all concurrency tests. Design M3's poll/dispatch loop in the
  structured shape now (one logical scope per cycle, cooperative
  cancellation flowing down via `JobContext.cancellationRequested()`) so
  migration is mechanical when the API finalizes.
- Concurrency limiting with a `Semaphore`, never via pool size.
- Virtual threads always named:
  `Thread.ofVirtual().name("mohs-job-", n).factory()`.
- HikariCP for virtual threads: high `maximumPoolSize` (100+), low
  `connectionTimeout` (< 3s).
- Deep concurrency analysis: use the **java-virtual-threads** skill.

## Tests
- Current coverage is good (>70%) and is the refactor's safety net: green
  suite after every step, no exceptions.
- Uncovered code → write the test first, show it, then refactor.
- Deterministic concurrency tests: no `Thread.sleep` for synchronization —
  latches, `CompletableFuture` with timeout, or Awaitility.
- Benchmarks (JMH/load) live apart from the unit suite and always compare
  against the numbers recorded in `docs/10-performance/`.

## Git and commits
- One subject per commit; the message explains the why, not the what.
- Refactor: one step per commit/PR, reviewable in isolation.

## Invariants
**ALWAYS:**
- Every "when" comes from the injected `Clock`; every duration uses monotonic
  time (`System.nanoTime`). Convention since the ArchUnit suite went away with
  `mohs-demo/src/test` (2026-08-30) — no build rule verifies it today.
- Every generated PK is UUIDv7 (`io.github.robsonkades:uuidv7`), on every
  dialect — client-side generation (no allocation round trip), time-ordered
  (inserts stay localized at the index tail), lexicographically sortable as
  a string (keyset-able if ever needed). Applies to future tables
  too (e.g. `mohs_batches`). Natural keys (`job_key`, `rate_limits.name`,
  per-aggregate counters like `attempts.number`) are fine — what is banned
  is the database-sequential surrogate.
- Refactoring preserves observable behavior; behavior changes only with my
  explicit approval.
- Performance claims come with before/after benchmarks. Without a number,
  it's not an optimization.

**NEVER:**
- Read time directly in the engine (`Instant.now()`,
  `System.currentTimeMillis()`).
- Sequential/auto-generated numeric PKs — no `IDENTITY`, `SERIAL`,
  `AUTO_INCREMENT`, or `SEQUENCE` in any schema, and no `java.util.UUID`
  v4 for ids (unordered — shatters insert locality on the hottest table).
- Break the public API without consulting me first.
- Commit with a red suite.
- Introduce a new dependency without asking.
- Reflection or "magic" where explicit code does the job.
- Wrappers over JDK APIs without demonstrated need.
- Configuration/flags for hypothetical scenarios.
- Edit a recorded baseline in `docs/10-performance/` retroactively — a
  baseline only changes with a new baseline.

## Maintaining this file
When you notice a rule here that is stale against the code, point it out and
propose the edit immediately — never silently follow a rule you know is wrong.
