# Concurrency model

Status: Active · Last Reviewed: 2026-09-04 · Source of Truth: Repository

Concurrency is the project's declared priority. This document is the complete inventory of threads,
shared state, and the reasoning behind each choice.

## Thread inventory

Every thread Mohs creates, per node:

| Thread | Kind | Count | Created by | Named | Owner |
| --- | --- | --- | --- | --- | --- |
| `mohs-engine-loop` | **Platform**, daemon | 1 | `Engine#start` | Yes | `Engine` |
| `mohs-runner-io-N` | **Virtual** | Up to `maxConcurrent` (default = `dispatch-concurrency`) | `MohsExecutors.ioBoundExecutor` | Yes | `RunnerRegistry` |
| `mohs-runner-cpu-N` | **Platform**, bounded pool | `coreSize`..`maxSize` | `MohsExecutors.cpuBoundExecutor` | Yes | `RunnerRegistry` |
| `mohs-events-N` | **Virtual** | Up to `mohs.engine.event-concurrency` (16) | `MohsExecutors.ioBoundExecutor` | Yes | `MohsAutoConfiguration` |
| `mohs-completion-flusher` | **Virtual** | 1 | `CompletionBatcher` | Yes | `CompletionBatcher` |
| `mohs-clock-sync-N` | **Virtual** via `ThreadPoolTaskScheduler` | 1 | `MohsExecutors.scheduler` | Yes | `MohsAutoConfiguration`, only when `mohs.time.mode=database` |
| `mohs-overview-sse` | **Platform**, daemon | 1 | `OverviewStreamBroadcaster` | Yes | The broadcaster, only when the REST API is on |
| `mohs-overview-sse-send-N` | **Virtual** | One per in-flight send | Same | Yes | Same |
| `mohs-overview-frame-N` | **Virtual** | 5 per tick, scoped to one call | Same | Yes | Same |

**Every thread is named.** That is a project rule, and the reason is 3 a.m.: an unnamed thread in a
profiler or a thread dump is a mystery.

## Workload classification

The rule is stated once and applied everywhere:

| Workload | Threading model | Ceiling mechanism |
| --- | --- | --- |
| I/O-bound (JDBC, HTTP, file, messaging) | Virtual threads, one per task | A **semaphore**, never pool size |
| CPU-bound | Platform threads, bounded pool | `maxSize` plus an explicit queue capacity |

**Never a fixed or cached pool for virtual threads.** `MohsExecutors` is the single place where this
discipline becomes code rather than a convention repeated in every new class — no engine class
creates an `Executor` by hand.

## Why the engine loop is a platform thread

From `Engine#start`:

> ONE platform thread, never a scheduler or a virtual one: latency-critical, immune to carrier
> starvation, and it appears with a name of its own in any profiler or thread dump.

It is a **daemon**, so a leaked engine never holds up JVM exit — a crash is already covered
semantics: the node's lease expires and a peer's reaper reclaims.

## Locks and synchronizers

`ReentrantLock` is preferred over `synchronized` throughout, by convention. The rationale is no
longer pinning — JDK 24 removed carrier pinning by
`synchronized`/`Object.wait()` — but the capabilities only an explicit lock gives: `tryLock` with a
timeout, interruptible acquisition, optional fairness, and multiple `Condition`s.

| Site | Primitive | Guards | Notes |
| --- | --- | --- | --- |
| `Engine.wakeLock` + `wakeCondition` | `ReentrantLock` + `Condition` | The loop's sleep | A timeout on the await is exactly what the backoff needs. The `wakeRequested` flag absorbs a signal arriving *before* the await — a `signal` with nobody waiting is lost; the flag is not |
| `CancellationSignal.lock` | `ReentrantLock` | Registration, interrupt delivery, deregistration | One lock shared by all three: the interrupt is delivered only while the thread is registered |
| `BatchCompletionCallbacks.lock` | `ReentrantLock` | An LRU `LinkedHashMap` | `ConcurrentHashMap` has no eviction, so the map plus a lock is the honest structure |
| `MohsJobScanner.scanned` | `synchronized` block | The accumulated scan map | Spring Framework 6.2+ background bootstrap can call `postProcessAfterInitialization` on concurrent threads in the host application |

### The wake protocol

```java
wakeLock.lock();
try {
    if (!wakeRequested) {
        wakeCondition.awaitNanos(bounded.toNanos());   // a SINGLE await, not a loop
    }
    wakeRequested = false;
} catch (InterruptedException e) {
    // swallowed deliberately — see below
} finally {
    wakeLock.unlock();
}
```

Two decisions worth understanding:

- **A single `await`, not a loop.** Waking without a signal (spurious or timeout) only brings a tick
  forward; the real predicate is the queue in the database. A `while` loop here would be wrong, not
  merely redundant.
- **`InterruptedException` is swallowed.** The engine *owns* this thread, and its stop protocol is
  state plus wake. An interrupt means nothing here, and re-arming the flag would become a busy-spin
  — every subsequent `awaitNanos` would throw on entry. Swallowing **is** the owner's interruption
  policy, not an empty catch.

## Atomics and concurrent collections

| Field | Type | Purpose |
| --- | --- | --- |
| `Engine.state` | `AtomicReference<EngineState>` | Every lifecycle transition is a **guarded CAS**, not a lock: a transition only applies if the source state is still the expected one |
| `Engine.inFlight` | `ConcurrentHashMap.newKeySet()` of futures | What the drain waits on |
| `Engine.inFlightAttempts` | `ConcurrentHashMap<ExecutionId, InFlightAttempt>` | What the timeout, cancel and watchdog sweeps reach |
| `Engine.drainEscalated` | `AtomicBoolean` | Fences the escalation **log**, never the loop |
| `Engine.nodeEpoch` | `volatile long` | Written by the tick thread, read by `writeFinalStoppedHeartbeat` from another thread |
| `Engine.loopThread` | `volatile Thread` | Written by `start`, read by `stop` — possibly different threads, since `MohsLifecycle` is public API |
| `CompletionBatcher.queue` | `LinkedBlockingQueue` bounded at `4 × flushSize` | Structural backpressure |
| `CompletionBatcher.inTransit` | `ConcurrentHashMap.newKeySet()` | The state-based guard the stray-lease reconcile consults |
| `RunnerRegistry.CountingExecutor.running` | `AtomicInteger` | Occupancy, incremented on acceptance and decremented on completion **including when the task throws** |
| `DatabaseClock.offset` | `AtomicReference<Duration>` with `accumulateAndGet` | The monotonic clamp is atomic regardless of who calls `sync()` |
| `MutableClock.now` | `AtomicReference<Instant>` with `updateAndGet` | Two concurrent advances cannot lose an increment |

## Thread confinement

Several pieces of state carry no synchronisation at all because they are **confined to the tick's
thread** — stated explicitly at each site:

| State | Confined to |
| --- | --- |
| `Engine.shardCursor` | The tick thread. Overflow is benign through `Math.floorMod` |
| `Engine.queueLooksEmpty` | The tick thread |
| `Engine.warnedAboutOwningNoShard` | The tick thread |
| `Engine.strayLeaseCandidates` | The tick thread |
| `Engine.nodeLeaseExpiresAt` | The tick thread |
| The `Admission` object and its mutable lease counts | One lap of one tick |
| The tick's `definitions` map (mutable, memoised on a miss) | One tick |

## Safe publication

- `CompletionBatcher`'s flusher thread starts in an idempotent `start()` **outside the
  constructor** — `this` must not escape before the object is built.
- `OverviewStreamBroadcaster` uses a static factory `start(...)` that schedules the timer **after**
  construction, eliminating the this-escape. It would be safe today (a submit establishes
  happens-before), but any future field assigned after scheduling would be published to the tick's
  thread without synchronisation.
- `Engine#start` publishes `loopThread` **before** starting it, and handles the resulting race:
  `stop` may win, so `joinLoopThread` returns immediately when the thread's state is still `NEW`
  (`join(Duration)` throws `IllegalThreadStateException` on an unstarted thread).

## Service-thread failure policies

Every long-lived thread declares what happens when it dies:

| Thread | Policy |
| --- | --- |
| `mohs-engine-loop` | Catches `Throwable` at the top of `runLoop`: logs "engine loop died — this node stops claiming. Its node lease will expire and peers will reclaim…", then **rethrows**. Without it, an `Error` escaped silently, the thread died, `state` stayed `RUNNING` forever, and `MohsLifecycle#state` and `GET /nodes` lied |
| `mohs-completion-flusher` | A `Throwable` guard around the flush plus a `finally` that sets `closed = true`, so `submit` degrades to the synchronous path. Without it an `Error` would kill the flusher with `closed` false: the queue fills, every submit blocks forever, and in-flight executions are stuck with their lease renewed, out of the reaper's reach |
| `mohs-overview-sse` | A `RuntimeException` becomes a WARN and the next tick retries. An `Error` is logged with an explicit consequence — "the periodic task is **CANCELLED** and every connected dashboard will silently freeze on stale data. Restart the application." — and **rethrown**, because `scheduleWithFixedDelay` cancels a throwing task permanently |

## Backpressure

There is no unbounded queue anywhere. Every boundary either rejects or blocks a *specific* thread:

| Boundary | Behaviour above the ceiling |
| --- | --- |
| IO runner | `RejectedExecutionException`. The engine catches it per execution, leaving the lease standing for a reaper |
| CPU runner | `AbortPolicy` once `queueCapacity` fills |
| Completion batcher | **Blocks the handler's thread** in `submit`. The dispatch stays in flight, the claim sees reduced headroom, and the node stops claiming beyond what it can persist |
| Event publisher | Drops the event with a WARN. The observation pipeline never exerts backpressure on the control pipeline — a mass reclaim publishing 1,000 events must not hijack the tick |
| SSE sends | **Conflation**: if a client's previous frame is still writing, the tick skips that client. Queueing would work against the design, since the next frame is the whole snapshot again |
| SSE subscribers | Hard cap of 64. Not capacity sizing — a guard against amplification, because every new connection pays a full snapshot read before joining and the API has no authentication |
| Claim | Bounded by dispatch headroom. A saturated node does not claim; the surplus stays claimable by any node with headroom |

## Structured concurrency

`StructuredTaskScope` (JEP 505) is **preview on JDK 25**, and a class file compiled with
`--enable-preview` locks the host application to that exact JDK — unacceptable for an embedded
library. The accepted pattern until it finalises is `ExecutorService` + `Future.get(timeout)` +
latch/barrier.

`OverviewStreamBroadcaster#buildFrames` is written in the **structured shape** so the migration is
mechanical:

```java
ExecutorService scope = Executors.newThreadPerTaskExecutor(virtualFactory);
try {
    List<Future<Frame>> forks = List.of(fork(...), fork(...), ...);   // 5 independent reads
    for (Future<Frame> f : forks) {
        frames.add(f.get(deadlineNanos - System.nanoTime(), NANOSECONDS));
    }
} finally {
    closeScope(scope);   // shutdownNow() THEN awaitTermination(grace)
}
```

`closeScope` is the point: an earlier version called `cancel(true)` and returned immediately with a
sibling thread still reading the database. **Cancellation is a two-phase protocol — signal *and*
await.** The bounded quiescence (`FRAME_CANCEL_GRACE = 1s`) means no subtask outlives the scope, and
a reader that ignores the interrupt becomes a leaked-thread WARN rather than an unbounded wait.

`launderThrowable` is applied to the `ExecutionException`: a `RuntimeException` is rethrown as
itself so `RestExceptionHandler` can map by type, and an `Error` never becomes an "illegal state".

## Lock ordering

Row locks are ordered to eliminate a whole class of deadlock. Both `JdbcLeaseStore#complete` and
`JdbcWorkQueue#requeue` sort by `executionId` before touching rows:

> A flusher (arrival order) and a reaper (`claimed_at` order) running concurrently over overlapping
> sets — exactly zombie versus reclaim — would lock rows in opposite orders. The canonical order by
> `executionId` eliminates the whole class.

A measured incident: 23 deadlocks in one benchmark run before the ordering was imposed.

The `V5__drop_partitioning.sql` PostgreSQL migration takes the same care at table granularity: it
locks `mohs_attempt` and `mohs_execution` **in the order the writers take them** (the completion
transaction goes attempt → execution), because locking in the inverse order would deadlock against a
live completion.

## Known contention points

| Point | Nature | Mitigation |
| --- | --- | --- |
| `mohs_rate_limits` row during `charge` | The only unconditional wait on the claim path | A 2 s statement timeout; the round is lost, never the heartbeat |
| Any other lock-waiting statement on the loop thread | Lock waits the tick cannot see coming — a peer's completion, a host transaction on a definition row | A 3 s statement timeout (`JdbcSupport#TICK_STATEMENT_TIMEOUT_SECONDS`) on the heartbeat, the node and lease reads, the cancel poll, the firing CAS, the claim and the requeue, and a 3 s transaction deadline on the reaper's completion; the step is lost, never the lease. The definition scans stay unbounded: their cost is rows, not a lock |
| `mohs_ready` claim | Row-level, resolved by `SKIP LOCKED`/`READPAST` | Never waits — on SQL Server only since the queue side became one `DELETE … OUTPUT` driven by the locking seek; the separate `DELETE … IN (…)` could be planned as an index scan and wait on a peer's keys |
| `mohs_lease` completion vs. requeue | Row-level | Canonical ordering by `executionId` |
| `mohs_nodes` purge | Every node issues the same `DELETE` every tick — a deadlock candidate on SQL Server | Isolated by `runMaintenance`, so a failure does not stop the claim |
| `mohs_batches` counter | Row-level per batch | Atomic increment; hot only for a single large batch |

## Race conditions handled explicitly

Each of these is a named, commented decision in the source:

| Race | Resolution |
| --- | --- |
| `stop()` wins against `start()` during launch | `start` re-checks the state after `thread.start()` and calls `wake()` |
| A tick registers a dispatch between `runAsync` and `inFlight.add` | `stop` waits **twice** — the second time with the loop joined, so no new submit can enter |
| A dispatch registered after the first drain escalation | The escalation's `AtomicBoolean` guards the **log**, never the loop, so the late dispatch is still signalled |
| The same `ExecutionId` re-claimed by this node after a requeue | Two-argument `remove(id, attempt)` with identity equality on `InFlightAttempt` — an in-memory ABA |
| A `submit` that passed the `closed` check landing after the flusher exits | `close()` performs a final synchronous sweep of the queue, separate from the join |
| A completion committing between `findByNodes` and the stray requeue | The fence loses; logged at DEBUG as routine, not a finding |
| A tick that read the state before `state.set(STOPPED)` committing its heartbeat *after* the final one | Accepted and documented: the row stays `RUNNING` with a fresh promise and this node's orphans wait one extra TTL — the same cost as a crash |
| Cancel falling between `cancelQueued` and `requestCancellation` | A second pass closes the window |
| `MohsImpl#adjustRateLimit`: find-then-upsert | Accepted as last-write-wins, which is what a `PATCH` promises |
