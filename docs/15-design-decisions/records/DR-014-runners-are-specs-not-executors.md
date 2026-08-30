# DR-014: A runner is a specification; Mohs owns the threads

## Status

Accepted

## Context

The natural Spring shape for "run this job on that pool" is `@Async("poolName")`, where the named bean
**is** an `Executor` supplied by the application.

That shape does not survive contact with the requirements. Cooperative cancellation needs the thread
registered so an interrupt can be delivered exactly inside the handler's window. Per-runner occupancy
needs a count the executor may not expose. The io-to-virtual and cpu-to-platform discipline needs the
threading model to be a decision Mohs makes, not one it inherits.

## Decision

`MohsRunner` is a **specification** — a record of name, mode and sizing — and **never** a
`java.util.concurrent.Executor`. Mohs creates and owns the threads.

| Mode | Threads | Ceiling | Above the ceiling | Queue |
| --- | --- | --- | --- | --- |
| `IO` | One **virtual** thread per task | `maxConcurrent` via a semaphore, **never pool size** | **Rejects** — real backpressure | None |
| `CPU` | A bounded **platform** pool | `maxSize` | `AbortPolicy` | `queueCapacity`, default **0** (direct hand-off) |

`MohsExecutors` is the single factory — no engine class creates an `Executor` by hand — and whoever
receives one owns its lifecycle.

## Consequences

### Positive

- **Cooperative cancellation is possible at all.** `CancellationSignal` registers the handler's thread
  immediately before the chain and deregisters in a `finally`, so an interrupt is delivered only
  inside that window — and the completion write (JDBC) never runs interrupted, and a CPU runner's
  platform thread never returns poisoned to its pool.
- **Occupancy is measurable.** `CountingExecutor` is a decorator that counts on **acceptance** and
  decrements on completion, including when the task throws, and gives the slot back in the `catch`
  when the executor rejects.
- **The threading discipline is code, not convention.** One place decides that I/O gets virtual threads
  with a semaphore and CPU gets a bounded platform pool with an explicit queue capacity.
- **The CPU defaults are deliberately not Spring's.** Spring's `spring.task.execution.pool.*` defaults
  to an effectively unbounded pool and queue because it cannot know whether the work is CPU- or
  I/O-bound. Here we know, and "backpressure at every boundary, never an unbounded wait" is a project
  rule.
- Two builders (`IoBuilder`, `CpuBuilder`) make a wrong-mode field unrepresentable at the call site,
  while the stored record stays one flat type.

### Negative

- **An application cannot supply its own `Executor`**, which is the shape a Spring developer expects.
  A `@Bean MohsRunner` supplies a *spec* instead.
- **`running` means different things per mode**, and the API says so: in `IO` it is effectively what is
  executing; in `CPU` it **includes what waits in the queue** and can therefore exceed `max`. A separate
  `queued` component was considered and refused — two numbers for the same fact would force every
  consumer to know which applies per mode, and adding a component to a public record breaks the
  canonical constructor and deconstruction patterns.
- **Runner occupancy is node-local by nature**, so `GET /runners` describes the process that answered
  the request, not the cluster. Behind a load balancer, two consecutive calls may legitimately differ.
- **A subtle coupling exists**: the claim's clamp uses `dispatch-concurrency` as the node's ceiling, and
  overriding the `io` runner with a smaller `max` breaks that single-source assumption silently. It is
  a WARN rather than a boot error, because capping `io` is a legitimate operational choice and the
  recovery path exists.
- The registry must build and close what it builds, and must close what has already been created if a
  later one fails — no orphan pool.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| `@Bean Executor` named per runner, `@Async`-style | Loses the interrupt window, the occupancy count, and control of the threading model. It is exactly the shape the Javadoc rejects |
| One sealed type per mode | Ergonomics belong in the builders, not the stored type; a flat record keeps persistence and equality simple |
| Add a `queued` component to `RunnerSnapshot` | Two numbers for one fact, and a breaking change to a public record. `running − max` in CPU mode is the backlog, and it is the difference's only possible meaning |
| Let the pool report its own active count | `SimpleAsyncTaskExecutor` does not expose one, and deriving it only in CPU mode would give the same field two meanings |

## Evidence

- `mohs-api/src/main/java/io/mohs/core/resource/MohsRunner.java` — the "spec, never an Executor"
  statement and the Spring-defaults comparison.
- `mohs-engine/src/main/java/io/mohs/engine/MohsExecutors.java` — the single factory and its
  discipline.
- `mohs-engine/src/main/java/io/mohs/engine/RunnerRegistry.java` — `CountingExecutor`, the shutdown
  action born with each runner, and the no-orphan-pool guarantee.
- `mohs-api/src/main/java/io/mohs/core/RunnerSnapshot.java` — the refused `queued` component.
- `mohs-spring-boot-starter/src/main/java/io/mohs/autoconfigure/MohsRunners.java` — assembly, the
  duplicate rule, and the `io`-below-`dispatch-concurrency` warning.
