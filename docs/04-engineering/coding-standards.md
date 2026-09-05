# Coding standards

Status: Active · Last Reviewed: 2026-09-04 · Source of Truth: Repository

This document separates **current practice** (what the code does, observably) from **recommended
practice** (what a contributor should do going forward). Where they coincide, that is said.

## Language and tooling

| Item | Value |
| --- | --- |
| Java release | **25** (`maven.compiler.release=25`) |
| Spring Boot | 4.1.1, imported as a BOM — **not** inherited via `spring-boot-starter-parent` |
| Preview features | **None.** `--enable-preview` would lock the host application to one exact JDK |
| Compiler flags | `-parameters` (Spring MVC resolves `@RequestParam`/`@PathVariable` names reflectively) |
| Formatter / linter | **Not present.** No Checkstyle, Spotless, PMD, SpotBugs or ErrorProne in the build |
| Static analysis | None beyond the three source-scan tests in `mohs-store-jdbc` |

## Naming

Current practice, consistently applied:

| Element | Convention | Examples |
| --- | --- | --- |
| Identifiers | **English**, domain vocabulary | `JobKey`, `Schedule`, `MohsRunner`, `ExecutionState` |
| Interfaces | The role, no `I` prefix, no `Impl` suffix on the interface | `JobStore`, `WorkQueue`, `LeaseStore` |
| Adapters | `Jdbc` + the port's name | `JdbcJobStore` implements `JobStore` |
| The one `Impl` | `MohsImpl`, `JobSpecImpl` | Used only where the interface name is the domain noun and no better adapter name exists |
| Read models | `*Snapshot` | `JobSnapshot`, `RunnerSnapshot`, `OverviewSnapshot` |
| Wire DTOs | `*Response` by default; `*View` for a sealed-type mirror or a computed projection | `JobResponse`, `ScheduleView`, `ThroughputView` |
| Requests | `*Request` | `ScheduleJobRequest`, `RateLimitPatchRequest` |
| Threads | `mohs-<resource>-N` | `mohs-engine-loop`, `mohs-runner-io-3` |
| Metrics | `mohs.<area>.<measure>` | `mohs.claim.latency`, `mohs.execution.total` |
| Metric label values | lower case, `snake_case` | `succeeded`, `attempts_exhausted` |
| Tables | `mohs_` prefix on **every** table | Mohs shares the host's database and schema |
| Test methods | Descriptive sentences, no `test` prefix | |

## Prose language

A deliberate, documented split:

| Where | Language |
| --- | --- |
| Identifiers (classes, methods, fields, packages) | **English** |
| Commit messages | **English** |
| Javadoc, comments, `package-info.java` | Historically **Portuguese**; the majority of the tree has been translated to English, and a minority of shorter comments remain in Portuguese |
| `mohs-ui` subtree | **English**, a deliberate divergence — the files arrived in English, and one bilingual subtree is worse than either language |

**Recommended practice**: write new prose in English. A migration of the remaining Portuguese
comments is deferred with no date; do not mix languages within one file.

## Comments

Current practice, and it is unusually strong:

- Comments explain the **why** the code cannot show. There is essentially no narration of *what*.
- Many comments name the **incident** that produced the rule — a measured regression, a benchmark
  number, a reproduced failure. Example: the 2-second stray-lease grace floor cites "10.7k lost
  fences in one cold round".
- Several comments are **prohibitions with reasons**: *"Do not 'tidy' it to `true` — the value
  changes nothing and the 'only arm with proof' rule stops being legible."*
- Alternatives considered and **rejected** are recorded, with the cost of each.

**Recommended practice**: keep it. A comment that says "increment the counter" should be deleted; a
comment that says "counting a retry would close the batch early" must exist.

## Immutability

| Rule | Current practice |
| --- | --- |
| Records for value objects and DTOs | Universal. Every public API type is a record, a sealed interface, an enum or a plain interface |
| No setters | Universal in the domain |
| Defensive copies of exposed mutable fields | `Execution.attempts` (`List.copyOf`), `ExecutionWindow.exclusions`, `FiringPlanner.Plan.occurrences`, `HistoryStore.PayloadBatch` |
| Documented exception to `copyOf` | `ScheduleJobRequest.payload` uses `new LinkedHashMap<>(...)` wrapped in `unmodifiableMap`, because `Map.copyOf` rejects null values and JSON legitimately has them |
| Mutable classes | Only where mutation is the point, and each says so: `Admission` is "a class, not a record: the lap **mutates** the counts, and a record communicates an immutable value"; `InFlightAttempt` is a class "deliberately without `equals`: equality **is** identity" |

## Validation

**Validate at construction, in the compact constructor.** An invalid domain object cannot exist.

```java
public record RateLimitSnapshot(RateLimit rateLimit, int available) {
    public RateLimitSnapshot {
        Objects.requireNonNull(rateLimit, "rateLimit");
        if (available < 0) {
            throw new IllegalArgumentException("available must not be negative, got " + available);
        }
        if (available > rateLimit.max()) {
            throw new IllegalArgumentException(
                "available must not exceed the bucket capacity of " + rateLimit.max() + ", got " + available);
        }
    }
}
```

Three rules visible throughout:

1. **The message names the field and shows the value.** `"got " + value` appears in nearly every
   message.
2. **The message often names the property to change**, not just the constraint:
   `"mohs.engine.max-poll-interval (…) must be >= mohs.engine.poll-interval (…) — it is the ceiling
   the idle backoff climbs to, not a second floor"`.
3. **Validate where the data enters the type.** `WorkQueue.ReadyEntry` validates the shard range,
   because a row outside `[0, 64)` would never be claimed and would rot silently.

## Nullness — JSpecify

| Rule | Detail |
| --- | --- |
| Every production `package-info.java` carries `@NullMarked` | Convention; nothing in the build verifies it |
| Non-null is the default | `@Nullable` marks the exception |
| Use `@Nullable` only on a genuinely nullable path | `Attempt.finishedAt()` while running; `JobDefinition.name()` with no label. Not "just in case" — noise hides the ones that matter |
| `Optional` **only** as a return type, when absence is part of the protocol | `NextFireCalculator.nextFireAfter` (empty for on-demand), `Mohs.findJob` |
| Never both for the same thing | `@Nullable` on fields and parameters; `Optional` on returns |
| **Never `@Nullable` on a local variable** | Valid syntax, but redundant: a local's nullness is inferred from flow |

The dependency is `org.jspecify:jspecify`, declared once in the parent for every module.

## Effective Java, applied

Items cited by name in the source, with real call sites:

| Item | Where |
| --- | --- |
| **1** — static factory over constructor when the name helps | `JobKey.of`, `ExecutionId.of`, `JobDefinition.of`, `Execution.enqueued`, `OverviewStreamBroadcaster.start`, `CursorPage.of` |
| **2** — builder for many/optional parameters | `MohsRunner.IoBuilder`/`CpuBuilder`, `ExecutionWindow.Builder`, the staged `JobSpec`/`PolicySpec` |
| **15** — minimise accessibility | `MohsJobs`, `MohsRunners`, `MohsRateLimits`, `JobSpecImpl`, `ExecutionEventPublisher`, `CancellationSignal` are all package-private |
| **17** — immutability | Records everywhere; the exceptions are named and justified |
| **34** — enum with an instance field, never `ordinal()` | `Priority.value()` |
| **49** — validate parameters | Compact constructors throughout |
| **50** — defensive copies | `Execution.attempts`, `ExecutionWindow.exclusions` |
| **64** — refer to objects by their interface | Applied; the documented exception is `RunnerRegistry.LiveRunner`, which holds the concrete `CountingExecutor` precisely for the decorator's extra capability |

`Execution.enqueued(...)` is worth calling out as a concrete Item 1 application: it **replaced two
overloads with an ambiguous tail**. The removed overloads ended in a row of `@Nullable String`, so
`(priority, null, nodeId)` compiled while writing `nodeId` into `batchId` — an execution declaring
itself a member of a batch that does not exist, with nothing to flag it.

## Modern Java usage

| Feature | Usage |
| --- | --- |
| Records | The default for every value object and DTO |
| Sealed interfaces | `Schedule`, `ExecutionEvent`, `ScheduleView`, `JobSpec`, `PolicySpec` — so a new variant is a compilation error at every use site |
| Pattern matching in `switch` | `FiringPlanner#plan`, `NextFireCalculator#nextFireAfter`, `ScheduleView#from`/`toSchedule`, `RunnerRegistry#build` |
| Record deconstruction patterns | `MohsImpl#rearmAfterFinishChain`: `instanceof IntervalSpec(Duration interval, boolean afterFinish)` |
| Unnamed variables (`_`) | Used consistently in lambdas and switch arms that ignore their binding |
| Text blocks | Every multi-line SQL string |
| Virtual threads | All I/O-bound execution |
| `ScopedValue` | **Not used yet.** `ThreadLocal` is banned by convention; the interceptor chain is the documented place for context propagation |
| `StructuredTaskScope` | **Not used** (preview). The shape is prepared — see [concurrency](concurrency.md) |

## Exception discipline

| Rule | Practice |
| --- | --- |
| Never swallow an exception | Every `catch` either handles, logs with the cause, or rethrows. The two deliberate swallows (`Engine#awaitWork`'s `InterruptedException`, `CompletionBatcher`'s flush-loop interrupt) carry a paragraph explaining the interruption policy |
| Preserve the original cause | `MohsJobs#adaptHandler` unwraps `InvocationTargetException` so `Attempt.error` records the real message |
| Restore the interrupt status — **at the right moment** | `CompletionBatcher#submit` completes the fallback *before* re-arming the flag, because with it raised the JDBC acquire would throw and the fallback would defeat itself |
| Fail fast on an unsupported construct | `@OnExecution` fails the boot rather than being silently ignored |
| A message that teaches | Every domain exception names what to do about it |

## Logging

See [logging](../09-observability/logging.md) for the full treatment. The standards in one line:
**SLF4J with parameterised messages, never string concatenation; the level matches the operational
meaning, not the code path's excitement; a WARN or ERROR states the consequence and the action.**

## Design references cited in the code

These are review criteria in this project, and the code cites them by name where they apply:

| Work | Applied to |
| --- | --- |
| *Effective Java* (Bloch) | The item list above |
| *Design Patterns* (GoF) | Named in Javadoc only where the problem the pattern solves is actually present — see [design patterns](design-patterns.md) |
| *Refactoring* (Fowler) | Code smells as the review checklist; `ExecutionQuery` is an explicit "Introduce Parameter Object", `ThroughputReading` an explicit Data Clump fix |
| *PoEAA* (Fowler) | Repository / Data Mapper / Unit of Work vocabulary for the store; deliberately **not** forced onto the pure contracts in `io.mohs.core`, which are Value Objects |
| *DDIA* (Kleppmann) | Fencing tokens (ch. 8), read skew (ch. 7), at-least-once semantics |
| *Java Concurrency in Practice* (Goetz) | Cited by chapter at nearly every concurrent site — 3.1 safe publication, 3.2 this-escape, 3.3 thread confinement, 5.5.2 launderThrowable, 6.3.2 changing a caller's error contract, 7.1.3 interruption policy, 7.1.5 two-phase cancellation, 7.3 service-thread failure, ch. 10 lock ordering, ch. 13 explicit locks, 14.2 condition predicates |
| *Enterprise Integration Patterns* (Hohpe/Woolf) | Idempotent Receiver, Transactional Outbox, Competing Consumers |
| *Designing Distributed Systems* (Burns) | Graceful shutdown, health/readiness posture |

**Recommended practice**: cite by name only when the code genuinely *is* the thing. A pattern named
as decoration is worse than no name at all.

## Recommended additions

Not present today; each would close a concrete gap.

| Recommendation | Gap |
| --- | --- |
| A formatter (Spotless with a fixed style) | Formatting is currently consistent by convention alone |
| `maven-enforcer-plugin` with dependency convergence | No enforcement of a single version per transitive dependency |
| JaCoCo aggregation and a per-module threshold | The per-module report exists; nothing merges it across modules or gates the build on it |
| A `.editorconfig` | Nothing pins indentation or line endings beyond `.gitattributes` |
| A CONTRIBUTING-level statement of the prose-language migration date | Currently "deferred, date undefined" |
