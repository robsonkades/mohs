# Dashboard SSE stream

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository (`io.mohs.rest.overview.OverviewStreamBroadcaster`)

`GET /overview/stream` — `text/event-stream`.

## What it is, and what it is not

**Polling moved to the server, not event delivery.** Each tick emits the *complete current state*,
not a delta and not a change notification.

Use the execution listing and detail endpoints to inspect persisted history. The snapshot
stream supports a live dashboard and carries no delivery or replay guarantee.

After reconnecting, the next frame contains current state. Intermediate transitions can be
missed; this endpoint is not a durable event log or a complete execution history.

## The frames

Five named events per tick, each wrapped in a `SnapshotEnvelope`:

| Event name | Payload |
| --- | --- |
| `overview` | `OverviewResponse` with the default 60 s throughput window |
| `jobs` | `JobResponse[]` |
| `nodes` | `NodeResponse[]` |
| `runners` | `RunnerResponse[]` |
| `executions` | `ExecutionSummaryResponse[]` — the first page, at `CursorPage.DEFAULT_PAGE_SIZE` (50) |

```
event: overview
data: {"asOf":"2026-08-29T14:22:31.482Z","data":{"executionCountsByStatus":{...},...}}

event: jobs
data: {"asOf":"2026-08-29T14:22:31.482Z","data":[...]}
```

**All five events of one tick carry the same `asOf`**, taken once from the injected clock. It is the
stamp that lets a frontend order or discard a late frame; diverging between events of one snapshot
would only create false precedence.

A client subscribes only to what it consumes, via `EventSource.addEventListener`.

The `executions` frame deliberately uses the same page size as the first page of `GET /executions`:
the dashboard's panel shows the *same slice*, and if they ever diverge, the divergence should be a
decision rather than an accident.

## Cadence and cost

| Property | Value | Notes |
| --- | --- | --- |
| Interval | **2 s**, fixed | `STREAM_INTERVAL`. Becomes a property when somebody needs another |
| Scheduling | `scheduleWithFixedDelay`, not fixed rate | A slow tick against a degraded database must not accumulate a queue of ticks that then fire in a burst |
| Read cost | **One tick shared by every subscriber** | The reads cost the same as one `GET /overview` plus the lists per interval, regardless of how many dashboards are connected — and **zero with no subscriber**, since the tick returns before touching the database |
| Send cost | Per subscriber, on a virtual thread | Blocking network I/O |
| Subscriber cap | **64** (`MAX_SUBSCRIBERS`) | Not capacity sizing: a guard against amplification, because every new connection pays a full snapshot read before joining and the API has no authentication |

### The upper bound on the interval

`STREAM_INTERVAL` must stay **at or below** `MohsImpl.RECENT_WINDOW` (10 s). With a window shorter
than the tick, each frame would describe only a fraction of the elapsed interval and the panel would
start *ignoring* the rest of the work — with nothing to flag it, because each isolated reading
remains correct. The two constants live in different modules and the compiler will never connect
them.

## Conflation

If a client's previous frame has not finished writing — a slow client, a suspended laptop with the
connection still alive — the tick **skips that client**.

Queueing would work against the design itself, since the next frame is the complete snapshot again.
Without conflation, the slowest client's blocking servlet write would define everyone's latency (a
task with no time bound monopolising a fixed-cardinality executor), and a stuck send would hold the
`writeLock` that shutdown contends for.

The mechanism is a per-subscriber `AtomicBoolean` and a `compareAndSet(false, true)` gate.

## The snapshot read

The five reads are independent, and run as a structural fan-out:

```java
ExecutorService scope = Executors.newThreadPerTaskExecutor(virtualFactory);
try {
    List<Future<Frame>> forks = List.of(fork(overview), fork(jobs), fork(nodes),
                                        fork(runners), fork(executions));
    for (Future<Frame> f : forks) {
        frames.add(f.get(deadlineNanos - System.nanoTime(), NANOSECONDS));
    }
} finally {
    closeScope(scope);      // shutdownNow() THEN awaitTermination(FRAME_CANCEL_GRACE)
}
```

| Property | Value |
| --- | --- |
| Latency | The **slowest** read, not the sum |
| Deadline | One `STREAM_INTERVAL` (2 s) — a snapshot that does not close within a tick is already stale |
| Scope closure | `shutdownNow()` **and then** `awaitTermination(1 s)`. Cancellation is a two-phase protocol: signal *and* await. An earlier version returned immediately with a sibling thread still reading the database |
| Worst case under failure | `STREAM_INTERVAL + FRAME_CANCEL_GRACE` = 3 s |
| A reader ignoring the interrupt | Becomes a leaked-thread WARN, never an unbounded wait |

Written in the shape of JEP 505 so the migration to `StructuredTaskScope` is mechanical once it
finalises — the preview API cannot be used because `--enable-preview` would pin the host application
to one exact JDK.

## Subscribing

`subscribe()` registers the client and sends the **initial snapshot immediately**, so the dashboard
paints without waiting for the first tick. The initial send is synchronous on purpose: before Spring
initialises the emitter, `send` only buffers in memory — there is no network write to block on.

Emitters are created with timeout `0L`: the stream lives until the client disconnects, and callbacks
remove the subscriber on both possible outcomes.

Three ordering details, each deliberate:

1. **The snapshot is read *before* the subscriber is registered.** If the read fails against a
   degraded database, the exception becomes the `subscribe`'s 500 without leaving an orphan
   subscriber — an emitter that was never initialised fires no callback and would buffer every
   future tick's sends with no ceiling.
2. **The subscriber is born `inFlight = true`.** The initial snapshot goes out outside the
   conflation gate, so a concurrent tick could otherwise interleave its frames with these and
   deliver the *older* frame last.
3. **`closed` is read twice**, with the registration between them. Publishing the subscriber before
   re-reading the flag guarantees that either `close` finds it in the list, or we find the flag —
   never neither.

At a closed door (after shutdown began, or above the subscriber cap) `subscribe` returns an
**already completed** emitter without touching the database: the client sees end-of-stream and the
async request closes immediately.

## Shutdown

`MohsOverviewStreamLifecycle` is a `SmartLifecycle` one phase **above** the web server's graceful
shutdown, and it exists for a concrete arithmetic problem.

Without it, the broadcaster's only shutdown is its bean destroy method, which runs in
`destroyBeans()` — after *all* of `stopBeans()`. Emitters have timeout `0L`, and the container's
graceful shutdown waits while any async request is active. **One open dashboard held the entire
shutdown phase until `spring.lifecycle.timeout-per-shutdown-phase` (30 s by Boot's default)
expired**, at which point the container aborted the graceful shutdown and dropped the connections by
force.

With the lifecycle in place, the container reaches its phase with no async request left and the
phase costs milliseconds.

**One residual case stays uncovered, deliberately**: a client stuck in the `send` of the last tick
holds that emitter's own `writeLock`, and the `complete` queues behind it — so that stream again
costs the container the whole phase. That is the right outcome: closing it would require `stop` to
wait for the completes, coupling shutdown latency to the slowest client, which is exactly what
conflation avoids on the normal path.

## Client example

```javascript
const es = new EventSource("/api/mohs/v1/overview/stream");

es.addEventListener("overview", e => {
  const { asOf, data } = JSON.parse(e.data);
  render(data, asOf);
});

es.addEventListener("executions", e => {
  const { data } = JSON.parse(e.data);
  renderTable(data);
});

es.onerror = () => { /* the browser reconnects; the next frame is the whole snapshot */ };
```

## Known limitations

| Limitation | Detail |
| --- | --- |
| No authentication | Same as the rest of the API. The 64-subscriber cap is the only abuse guard |
| No `Last-Event-ID` resumption | Unnecessary — every frame is complete |
| No per-client filtering | Every subscriber receives all five events |
| Fixed 2 s cadence | Not configurable today |
| Frames may arrive out of order across a reconnect | `asOf` exists so a client can discard a stale frame; the server also avoids producing the reorder on the initial-snapshot path |
| SQL Server needs RCSI | `READ_COMMITTED_SNAPSHOT ON` is a boot requirement of the dialect, so a running system already has it — the counts read the last committed version without blocking |
| The reads inherit whatever `GET /overview` costs | On an idle database the throughput count costs the window (≈1.6 ms at 2 M rows); the backlog scan is what costs (≈13.2 ms at 500 k). Measured 2026-08-23 by `OverviewLatencyScenario`. **The number that still does not exist is the endpoint under load with a subscriber attached** |
