# Design patterns in use

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

Only patterns that are genuinely present are listed. Where the code names a pattern in its own
Javadoc, that is noted — the project's rule is that a pattern is named only when the problem it
solves is actually there.

## Structural and creational

### Ports and Adapters (Hexagonal)

| | |
| --- | --- |
| **Location** | Ports in `io.mohs.engine`; adapters in `io.mohs.store.jdbc` |
| **Intent** | Keep the execution logic independent of storage technology |
| **Implementation** | Ten interfaces the engine defines and does not implement; one `Jdbc*` class each |
| **Enforced by** | The reactor (no dependency) and `engine_is_free_of_jdbc` (no JDBC type in a port signature) |
| **Benefit** | `InMemoryJobStore` exists with no database at all — which is also the proof the port leaked nothing |
| **Trade-off** | Ten interfaces to keep in sync with their single production implementation. Accepted because the second implementation (in-memory) already exists and the boundary is what makes the whole test strategy possible |

### Data Mapper / Repository (PoEAA)

| | |
| --- | --- |
| **Location** | `io.mohs.store.jdbc`, named in every class Javadoc |
| **Intent** | Move data between objects and the database without either knowing the other |
| **Trade-off** | Hand-written SQL rather than an ORM. Deliberate: the claim query's exact shape *is* the performance story, and an ORM would hide it |

### Unit of Work (PoEAA)

| | |
| --- | --- |
| **Location** | `StoreTransactions` / `JdbcStoreTransactions` |
| **Intent** | Make "history insert + queue insert (+ idempotency)" one atomic unit that can join the host's transaction |
| **Notes** | Deliberately **minimal**: "not a general-purpose `TransactionTemplate` for hire". The only legitimate caller is the facade composing the enqueue unit |

### Builder (GoF / Effective Java 2)

| | |
| --- | --- |
| **Location** | `MohsRunner.IoBuilder`/`CpuBuilder`, `ExecutionWindow.Builder`, and the **staged** `JobSpec` → `PolicySpec` |
| **Intent** | Many optional parameters, and — in the staged case — making an invalid combination unrepresentable |
| **Why staged** | `JobSpec` exposes only the four trigger methods, and each returns `PolicySpec`, which does not expose them again. "Cron *and* every" is a compilation error rather than a boot-time validation failure |
| **Trade-off** | Two builder types instead of one. Bought: the compiler enforces the constraint |

### Static Factory (Effective Java 1)

Applied wherever the name helps or construction is not 1:1 — `JobKey.of`, `ExecutionId.of`,
`JobDefinition.of`, `Execution.enqueued`, `BatchResponse.of`, `CursorPage.of`,
`OverviewStreamBroadcaster.start`, `MohsRunner.io`/`cpu`.

`OverviewStreamBroadcaster.start` is the interesting one: the factory exists specifically so
scheduling happens **outside** the constructor, eliminating the this-escape.

### Decorator (GoF)

| | |
| --- | --- |
| **Location** | `RunnerRegistry.CountingExecutor` |
| **Intent** | Count occupancy where it happens, rather than asking the pool |
| **Why** | `SimpleAsyncTaskExecutor` (IO mode) does not expose an active count, and deriving it from `ThreadPoolTaskExecutor#getActiveCount` only in CPU mode would give the same field two meanings |
| **Implementation note** | A *named class owning its own counter*, not a lambda over an external `AtomicInteger`: the counter and the executor only mean anything together. Loose in two fields, nothing stops a pair that does not talk to each other, and the number would lie in silence |
| **Correctness detail** | Decrements on completion **including when the task throws**, and gives the slot back in the `catch` when the executor rejects — the `execute` did not happen |

### Adapter (GoF)

| | |
| --- | --- |
| **Location** | `MohsEngineLifecycle`, `MohsOverviewStreamLifecycle` |
| **Intent** | Adapt `MohsLifecycle` (`start()` / `stop(Duration)`) to Spring's `SmartLifecycle` (`start()` / `stop()`) |
| **Why not one interface** | Similarly shaped interfaces with incompatible signatures are not the same thing under two names |

### Facade (GoF)

| | |
| --- | --- |
| **Location** | `io.mohs.core.Mohs` |
| **Intent** | One verb per operation over an engine of a dozen collaborators |
| **Trade-off, stated in the code** | Several read methods (`jobs`, `nodes`, `runners`, `overview`, `executions`, `payloadType`) exist **because** the architectural boundary forbids REST from seeing the engine. The facade is the only read path available to it |

## Behavioural

### Chain of Responsibility (GoF)

| | |
| --- | --- |
| **Location** | `ExecutionInterceptor` + `Dispatcher#runInterceptorChain` |
| **Intent** | Wrap handler execution on the attempt's own thread — MDC, tracing spans, context propagation |
| **Implementation** | Composed backwards so the first interceptor in the list is outermost |
| **Key difference from a listener** | An interceptor's exception **is** a failure of the attempt and follows the normal retry flow. Whatever sits on the critical path takes part in the outcome |

### Observer (GoF)

| | |
| --- | --- |
| **Location** | `ExecutionListener` + `ExecutionEventPublisher` |
| **Intent** | Observe the execution lifecycle without interfering |
| **Contract** | Best-effort, asynchronous, one virtual thread per publication. A listener's exception is caught and logged and never affects the job. **No ordering guarantee** between events of the same execution |
| **Trade-off** | A saturated event executor **drops** the event with a WARN. The observation pipeline never exerts backpressure on the control pipeline |
| **When not to use it** | A guaranteed reaction. Use the transactional outbox instead |

### Strategy (GoF)

| | |
| --- | --- |
| **Location** | `ActorResolver` (named as a Strategy in its Javadoc); `JdbcDialect` |
| **Intent (ActorResolver)** | With security plugged in, the authenticated principal; without it, the `X-Mohs-Actor` header |
| **Intent (JdbcDialect)** | Isolate the few genuine SQL divergences |
| **Design note** | Each dialect owns the claim's **entire** SQL template, not concatenable fragments — SQL Server's `TOP` changes *position* in the query, so a composition of generic fragments does not close cleanly. This is the same shape Quartz uses (`StdJDBCDelegate`/`MSSQLDelegate`) and how Hibernate actually implements `LimitHandler` underneath |
| **Selection** | An **explicit choice, never auto-detection** — detecting through `Connection.getMetaData()` is fragile across driver forks and versions |

### State machine

| | |
| --- | --- |
| **Location** | `EngineState` (`CREATED → RUNNING ⇄ PAUSED → DRAINING → STOPPED`) and `ExecutionState` |
| **Implementation** | Every engine transition is a **guarded CAS** on an `AtomicReference`, not a lock. A transition only applies if the source state is still the expected one; otherwise it throws with both states named |

### Template Method

| | |
| --- | --- |
| **Location** | `JdbcDialect`'s default methods |
| **Intent** | The portable three-statement claim is the default; PostgreSQL overrides the whole thing with a single statement, SQL Server overrides only the candidate sweep |

## Distributed-systems patterns

### Fencing token (DDIA ch. 8)

| | |
| --- | --- |
| **Location** | `(node_id, epoch)` on every write over owned work |
| **Intent** | Make a revived zombie's writes lose by construction |
| **Implementation** | The completion is `DELETE … WHERE execution_id = ? AND node_id = ? AND epoch = ?`; the **row count is the verdict** |

### Lease / heartbeat failure detection

| | |
| --- | --- |
| **Location** | `mohs_nodes.expires_at` written each tick; `Engine#aliveNodeIds` |
| **Intent** | Derive death from an expired promise rather than from a written notice — a crash writes nothing |

### Competing consumers (EIP)

| | |
| --- | --- |
| **Location** | The claim, across nodes |
| **Implementation** | `SELECT … FOR UPDATE SKIP LOCKED` inside the claim transaction, plus derived sharding so that consumers mostly do not compete for the same rows in the first place |

### Idempotent receiver (EIP)

| | |
| --- | --- |
| **Location** | `mohs_idempotency`, primary key `(job_key, idempotency_key)` |
| **Implementation** | The primary-key conflict **is** the check. See [idempotency](../03-functional/idempotency.md) |

### Transactional outbox (EIP)

Referenced, not implemented by Mohs — it is the pattern the documentation **prescribes to
consumers** for a guaranteed reaction: the handler enqueues the continuation inside its own
transaction, rather than relying on a best-effort listener.

### Group commit

| | |
| --- | --- |
| **Location** | `CompletionBatcher` |
| **Intent** | Amortise the fsync cost of one commit per completion |
| **Trade-off, declared in the class Javadoc** | The durability window grows from ~1 ms to at most the flush interval; a crash inside it re-executes up to `flushSize` results beyond those in flight. The contract was already at-least-once — this changes the *exposure to duplicates*, not the guarantee |

### Bulkhead / backpressure

Every executor boundary rejects or blocks rather than queueing without limit. See
[concurrency](concurrency.md#backpressure).

## Patterns deliberately **not** used

| Pattern | Why not |
| --- | --- |
| Singleton (GoF) | Spring's container owns the lifecycle. There is no `getInstance()` anywhere |
| Service Locator | Constructor injection throughout; the one deferred lookup (`ObjectProvider` in `MohsJobScanner`) exists to avoid premature bean creation, not to locate services |
| Active Record | The domain has no persistence methods |
| An ORM | Hand-written SQL, because the claim query's shape is the performance story |
| Leader election | The trigger CAS makes it unnecessary |
| Circuit breaker | There is one downstream (the database) and the adaptive backoff plus per-step isolation covers degradation. Adding a breaker over the store would stop the heartbeat, which is exactly what must not stop |
| Saga / compensation | No cross-service transactions exist |
| Event sourcing | History is state plus an append-only attempt log, not a rebuildable event stream |

## Anti-patterns actively avoided

Each was considered and rejected, with the reason recorded in the source:

| Avoided | Where the reasoning lives |
| --- | --- |
| An interface with one implementation, created "for testability" | `MohsAutoConfiguration`: "no bean here backs off with `@ConditionalOnMissingBean` … internal infrastructure is not an extension point" |
| A configuration flag for a hypothetical scenario | The only knob group commit adds is its opt-out |
| A counter that can drift from its inputs | `pending` is derived in `BatchSnapshot`/`BatchResponse` — "a fourth column could drift from the other three, and there is no question it would answer any faster" |
| A wrapper over a JDK API without demonstrated need | `JdbcTimestamps` exists only because the `java.sql.Timestamp` path had a reproduced DST bug |
| A cache with no ceiling | `NextFireCalculator`'s expression cache (10,000), `BatchCompletionCallbacks`' LRU (10,000), and the deliberate *absence* of `CachingResourceResolver` in the UI handler, because the SPA fallback removes the 404 that normally bounds it |
| Duplicated policy in two failure paths | `RetrySchedule` is shared by the dispatcher and the reaper — "two failure paths with their own copies of the policy would diverge on the first change" (Shotgun Surgery) |
| A `@RestControllerAdvice` without `basePackages` | It would start deciding the **host application's** error handling |
