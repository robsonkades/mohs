# Startup and shutdown

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## Startup

```mermaid
sequenceDiagram
    participant Spring
    participant Beans as Stores, queue, firer
    participant Scan as MohsJobScanner
    participant RL as Rate-limit registrar
    participant Life as MohsEngineLifecycle
    participant Loop as mohs-engine-loop

    Note over Spring: the schema is ALREADY THERE — applied by the operator<br/>before this process started; nothing here runs DDL
    Spring->>Beans: create
    Note over Spring: mohs.time.mode=database?<br/>DatabaseClock.sync() blocks here — the engine<br/>must not start with an unsynchronised clock
    loop each singleton
        Spring->>Scan: postProcessAfterInitialization — accumulate @MohsJob
    end
    Spring->>Scan: afterSingletonsInstantiated — reconcile + register handlers
    Spring->>RL: afterSingletonsInstantiated — register declared limits
    Spring->>Life: SmartLifecycle.start() (DEFAULT_PHASE — LAST)
    Life->>Life: warn about declared policy gaps
    Life->>Loop: engine.start() — CAS CREATED → RUNNING, start the thread
```

### The ordering guarantees, and where each comes from

| Guarantee | Mechanism |
| --- | --- |
| The schema exists before anything writes | **Yours to guarantee.** No bean creates it. A store's constructor only builds a `JdbcTemplate`, so a missing table is not noticed at wiring time — it surfaces on the first statement, as the driver's own error, and the boot fails without corrupting anything |
| Definitions are registered before the first claim | `afterSingletonsInstantiated` happens during `finishBeanFactoryInitialization`, always ahead of `finishRefresh`, where `SmartLifecycle.start()` fires |
| The clock is synchronised before the engine starts | `DatabaseClock.sync()` is called synchronously in the bean method, deliberately blocking |
| The engine starts last | `SmartLifecycle` at `DEFAULT_PHASE`, stated explicitly even though it matches the interface default — **the phase is a documented architectural guarantee, not a coincidence of defaults** |

### Boot-time checks

| Category | Effect |
| --- | --- |
| Missing `mohs.jdbc.dialect` | **Boot fails**, naming the four valid values |
| Any `EngineSettings` violation | **Boot fails**, naming the property, the value and what the constraint means |
| Duplicate job id, two job annotations on one method, blank stereotype id, unsupported handler signature, `@RecurringJob` with no trigger, invalid `@OnExecution` signature or filter, annotation/programmatic identity collision | **Boot fails** |
| Definitional drift under `on-conflict=fail` | **Boot fails**, showing the diff |
| Duplicate runner or rate-limit name; a runner field of the wrong mode | **Boot fails** |
| Every warning in [configuration](../07-configuration/configuration-reference.md#warnings-emitted-at-boot) | Logged, boot continues |

Policy checks run over the definitions once. A detected missing `RetryPolicy` bean fails
automatic startup; a failure to read definitions is logged as a warning. Watchdog-versus-job
timeout mismatches are warnings. See [configuration](../07-configuration/configuration-reference.md).

### Manual start

```yaml
mohs:
  lifecycle:
    start-mode: manual
```

Then `mohs.lifecycle().start()` when you are ready. Useful when the application must warm caches or
open connections before accepting work.

### What a node does in its first tick

1. Write a heartbeat with `epoch = 1` and `expires_at = now + node-lease-ttl`.
2. Load the definitions snapshot.
3. Read `mohs_nodes` — serving both the reaper and the shard assignment.
4. Run the reaper (it may immediately reclaim leases from a node that died earlier).
5. Reconcile its own stray leases (none yet — it just started).
6. Purge stale node rows.
7. Fire any due triggers.
8. Claim and dispatch.

**Note `queueLooksEmpty` starts `false`**: a node that has just come up has no right to assume the
queue is empty, so its first tick always runs the full lap.

## Shutdown

```mermaid
sequenceDiagram
    participant Spring
    participant SSE as MohsOverviewStreamLifecycle
    participant Life as MohsEngineLifecycle
    participant Engine
    participant Batcher as CompletionBatcher
    participant Web as Web server

    Spring->>SSE: stop()  (phase ABOVE the web server's)
    SSE->>SSE: close every SSE emitter
    Spring->>Life: stop()  (DEFAULT_PHASE — FIRST)
    Life->>Engine: stop(grace-period)
    Engine->>Engine: drain(grace) — state := DRAINING, log INFO
    Engine->>Engine: awaitInFlight(deadline)
    Engine->>Engine: state := STOPPED, wake, join the loop (up to node-lease-ttl/4)
    Engine->>Engine: awaitInFlight AGAIN — now the loop is gone
    Engine->>Batcher: drainCompletions(remaining budget, floor 1s)
    Engine->>Engine: write the final STOPPED heartbeat (expires_at zeroed)
    Engine->>Engine: log INFO "engine stopped in {}"
    Spring->>Web: graceful shutdown (now with no active async request)
    Spring->>Batcher: close() — the bean destroy; drained already, but sweeps stragglers
```

### The order is load-bearing

| Step | Why it is exactly there |
| --- | --- |
| SSE streams close **first** | Emitters have timeout `0L` and the container waits while any async request is active. **One open dashboard held the entire shutdown phase until `spring.lifecycle.timeout-per-shutdown-phase` (30 s) expired**, at which point the container aborted graceful shutdown and dropped connections by force |
| The engine stops **before** beans are destroyed | So the batcher's `close()` drains what the last handlers submitted |
| `awaitInFlight` runs **twice** | The first wait observes `inFlight` with the loop still running, and a tick that has already claimed a batch registers its futures **after** the `runAsync` — finding the set empty there proves the work has not been registered yet, not that it finished. With the loop joined, empty means empty |
| The batcher drains **before** the final heartbeat | **Mandatory: durability before announcing death.** The final heartbeat zeroes `expires_at`, and from then on every peer considers this node dead — a result still in the queue would be reclaimed as an orphan |
| The final heartbeat writes `STOPPED` | Without it, a clean stop and a crash would be indistinguishable in the database |

### The grace budget

Both in-flight waits of **one** `stop` share the requested grace budget. Loop joining,
completion draining (with a one-second minimum wait) and final JDBC work can extend total
shutdown time beyond that grace; measure the whole application shutdown for deployment sizing. By contrast, `drain(g)` followed by `stop(g)` are two **separate** deadlines in
time — that sequence can cost up to `2 × g` when the drain escalates and the handler ignores the
interrupt.

The deadline uses `System.nanoTime()`, not the injected clock: the clock may be a `DatabaseClock`
whose offset jumps on every resync and would stretch or shorten the grace.

### Escalation

When the grace expires:

- **Flag plus interrupt** on everything still in flight.
- Those attempts fail asynchronously with a node-shutdown cause and follow the **normal retry**.
- There is **no second configurable wait window**.
- A handler deaf to the interrupt is orphaned once the ticks end — the heartbeat stops, the node's
  lease expires, and a peer's reaper reclaims.

The WARN names both counts on purpose: `inFlight` (the futures) is what holds the grace, while the
in-flight map is what the escalation reaches — with a watchdog-released zombie staying in the map,
they diverge during the completion window.

### Two things the post-loop wait does *not* cover

Recorded in `Engine#stop`'s Javadoc, and they decide the behaviour under the defaults:

1. **Nobody renews the node's lease during it.** `renewNodeLease` runs only on the tick, which has
   stopped. Protection ends at most `node-lease-ttl` after the last tick — less in practice. **With a
   15 s TTL and a 30 s grace, more than half the wait is uncovered**, and a peer may reclaim work
   still running here.
2. **The signalling ladder is suspended.** With no tick there is no timeout signalling, no cancel
   poll and no watchdog. A job with a `timeout` shorter than the grace stops being interrupted at its
   own deadline; during this window only the grace decides.

**Practical consequence**: keep `grace-period` close to `node-lease-ttl` if you want reclaim-free
shutdowns, or accept that a long grace trades shutdown patience for a duplicate-execution window.

### A deliberately accepted race

A tick that read the state before `state.set(STOPPED)` may commit its heartbeat **after** the final
one. The row then stays `RUNNING`/`DRAINING` with a fresh promise, and this node's orphans wait an
extra TTL for the reaper — **the same cost as a crash**. Closing it would require the stop to wait
for the current tick, coupling shutdown latency to a window that staleness plus the purge already
cover.

## Kubernetes-style deployment guidance

Mohs ships no manifests, but the code makes specific assumptions:

| Setting | Guidance |
| --- | --- |
| `terminationGracePeriodSeconds` | **Greater than** `mohs.lifecycle.shutdown.grace-period` **plus** the web server's own graceful-shutdown phase. Default 30 s + web ⇒ 60 s is a sane floor |
| `spring.lifecycle.timeout-per-shutdown-phase` | Must exceed the drain grace, or Spring aborts the phase mid-drain |
| `preStop` hook | Useful to let the load balancer drain first — although Mohs' work does **not** arrive through the load balancer, so this matters only for REST and the dashboard |
| Liveness probe | **Must not depend on the database.** A database outage would restart every healthy pod |
| Readiness probe | `state == RUNNING`, if you want a paused node out of the load balancer |
| Startup probe | Preferable to a guessed `initialDelaySeconds`; boot includes registration and, in database time mode, the initial clock sample |
| Replicas | Any number **up to 64**. Above that, extra nodes own no shard and never claim, with a WARN |
| Rolling updates | Supported and measured. See `NodeChurnScenario` and `RollingUpdateScenario` |

### What a rolling update looks like

1. After the schema has been upgraded separately, a new pod starts, registers definitions, and starts
   claiming.
2. Membership changes, so **shard assignment shifts** — nodes may disagree for one heartbeat, which
   degrades to the pre-shard behaviour and heals itself.
3. The old pod receives SIGTERM, drains, and writes a final `STOPPED` heartbeat.
4. Anything the old pod could not finish within the grace is interrupted, fails with a
   node-shutdown cause, and **retries** — on any node with headroom.

**If the new version adds a job**, part of the cluster lacks the handler during the rollout. A
missing handler goes through the **retry budget on purpose**, so a node running the newer version
may claim the retry. `RollingUpdateScenario` asserts both the survival case and the cost of a
permanently blind node.

## Crash behaviour

| Aspect | Value |
| --- | --- |
| What is written | **Nothing.** A crash gives no notice |
| Detection | A peer sees `expires_at <= now` and treats the node as dead |
| Detection latency | The remaining node lease (0 to 15 s by default), plus peer polling and processing time; recovery can span additional ticks |
| Recovery | The reaper reclaims each orphaned lease through the retry budget |
| Measured | `kill -9` mid-drain: 50,000 executions terminal; **827 re-executions = exactly the set that was `RUNNING` at the kill**; reclaim wave 15.4 s after the kill; whole recovery finished at 19.6 s; zero exception lines |
| If the node comes back | Its epoch bumps on the first tick that notices the expiry, so every write from the old incarnation loses the fence |
