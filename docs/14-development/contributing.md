# Contributing

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

Start with [CONTRIBUTING.md](../../CONTRIBUTING.md) for contribution scope and bug reports.
This guide explains the development conventions and documentation expectations.

## The bar

Four things this codebase treats as prerequisites rather than achievements: clean code, SOLID,
tests, and a green suite. The interesting standard starts after them:

| Standard | What it means here |
| --- | --- |
| **Every decision carries its trade-offs** | Alternatives considered, why this one, what is being paid. Several classes in the tree record a rejected alternative and its cost |
| **Failure modes first** | What happens if the process dies between claim and execution? If two nodes fire the same trigger? If the clock goes backwards? Code that does not answer these is not ready |
| **Measure before opining on performance** | Without a number, it is not an optimisation. Four plausible optimisations in this codebase's history were implemented, measured and **reverted** |
| **Design for 3 a.m.** | Metrics, actionable logs, and errors that say what to do are feature requirements, not finishing polish |
| **Know the state of the art** | When touching something Quartz, JobRunr, db-scheduler or Temporal already solve, say how they solve it and why this approach is equal or better. The dialect design cites Quartz's delegates and Hibernate's `LimitHandler` by name for exactly this reason |

## Workflow for a code change

1. **Understand before editing.** Read the types involved. Non-trivial change: propose a short plan
   (steps and trade-offs) before coding.
2. **Small steps, green suite after each one.** Uncovered code → write the test first, show it, then
   touch the code.
3. **One logical change per commit**, reviewable in isolation.
4. **Run the suite and report the real result**, including failures.

## Commit conventions

| Convention | Detail |
| --- | --- |
| Language | **English** |
| Subject | One subject per commit; the message explains the **why**, not the what |
| Style | Observable from `git log`: short, declarative subjects — *"Close the shutdown window that handed running work to the reaper"*, *"A job is born with a retry budget (retries default 0 → 1)"* |
| Scope | One logical change |
| Trailers | **No Claude/Anthropic author or co-author trailer** |

## Code conventions

The full treatment is in [coding standards](../04-engineering/coding-standards.md). The short form:

| Rule | Detail |
| --- | --- |
| Identifiers | English, using the domain vocabulary |
| Prose in code | English for documentation, Java and frontend prose; preserve historical SQL comments when unrelated to the change |
| Comments | The **why** the code cannot show. Never narrate the what, and never narrate the change itself |
| Records for value objects; validation in the compact constructor | An invalid object must not be constructible |
| `@NullMarked` on every production package | Convention; nothing in the build verifies it |
| No new dependency without discussion | The runtime footprint is deliberately small |
| Prefer editing an existing file to creating one | Especially documentation |
| No configuration flag for a hypothetical scenario | |
| No reflection or "magic" where explicit code does the job | |
| No wrapper over a JDK API without demonstrated need | `JdbcTimestamps` exists because the alternative had a reproduced DST bug |

## Testing expectations

| Expectation | Detail |
| --- | --- |
| Run the existing tests after **any** change, and report the real result | |
| **Never** weaken, `@Disabled` or delete a test to make it pass | If the test is wrong, say so and ask |
| A new test covers the requested behaviour | Do not invent extra scenarios |
| Deterministic concurrency tests | Latches, `CompletableFuture` with a timeout, `MutableClock`. **No `Thread.sleep` for synchronisation** |
| Benchmarks live apart from the unit suite | And always compare against a recorded baseline |
| Make assertions falsifiable | The model is `BatchCompletionScenario`'s `retries(1)`: with zero budget, correct and incorrect behaviour produce the same number and the assertion proves nothing |

## Architecture rules you cannot break silently

The reactor's compile classpath and three source scans in `mohs-store-jdbc` are the only build rules;
the rest is convention caught in review. Before proposing a change that crosses a boundary, read
[boundaries and fitness functions](../02-architecture/boundaries-and-fitness-functions.md).

The ones most likely to catch a newcomer:

| Rule | What it means in practice |
| --- | --- |
| `rest_only_sees_public_api` | Need new data in a controller? **Add a read method to the `Mohs` facade**, do not import the engine |
| `engine_never_reads_wall_clock_directly` | Inject a `Clock`. `System.nanoTime()` is fine — it is monotonic time, not a wall-clock "now" |
| `ids_are_generated_as_uuidv7_never_v4` | `UUIDv7.randomUUIDString()`. The rule catches method references too |
| `all_production_packages_declare_null_marked` | A new package needs a `package-info.java` |
| `engine_is_free_of_jdbc` | Do not put `ResultSet` or other JDBC types in a port signature; review enforces this rule |
| `no_synchronized_methods_in_concurrency_critical_code` | Use `ReentrantLock` |

## Documenting public behavior

Explain usage, guarantees, limitations and operational requirements in the guide that owns
the subject. Explain necessary implementation reasoning in a nearby code comment.
Keep internal decision records, findings, audits and technical-debt ledgers out of `/docs`.

## Updating this documentation

| Situation | Action |
| --- | --- |
| A behaviour changed | Update the relevant document **in the same change** |
| A property was added | [Configuration reference](../07-configuration/configuration-reference.md) |
| An endpoint was added | [Endpoints](../05-api/endpoints.md) **and** verify it is registered as a bean |
| The schema changed | [Data model](../06-data/data-model.md), [schema](../06-data/schema.md), [indexes](../06-data/indexes.md), [migrations](../06-data/migrations.md) |
| A metric was added | [Metrics](../09-observability/metrics.md) — **label values are contract** |
| A limitation affects users | Document its observable effect and available workaround in the relevant guide |
| A performance number was measured | [Performance characteristics](../10-performance/performance-characteristics.md), **with the environment stated** |

The documentation's own rules: **never invent behaviour**, mark what could not be determined as
`Unknown`, and distinguish *implemented* from *configured* from *referenced but not implemented*.

## What the repository does not have

Worth stating so nobody looks for it:

| Missing | Consequence |
| --- | --- |
| `CONTRIBUTING.md` | This document is the closest thing |
| A code of conduct | |
| Issue or PR templates | |
| A CI pipeline | **Nothing gates a commit.** Run `./mvnw clean verify` yourself |
| A `CHANGELOG` | |
| A release process | See [modules and publishing](../12-build/modules.md#release-process) |
| A formatter or linter configuration | Match the surrounding code |
| A branch-protection or review policy | |

## Getting oriented

Reading order for a first contribution:

1. [Architecture overview](../02-architecture/architecture-overview.md) — the style and the rules.
2. [Execution lifecycle](../02-architecture/execution-lifecycle.md) — the state machine and the
   transaction map. **This is the document that makes the rest make sense.**
3. [Module architecture](../02-architecture/module-architecture.md) — where things live.
4. `io.mohs.core.Mohs` and `io.mohs.core.definition.JobDefinition` — the two reading entry points the
   codebase itself nominates.
5. `io.mohs.engine.Engine` — the loop. Long, but its Javadoc is a map.
6. [Local development](local-development.md) — get it running.
