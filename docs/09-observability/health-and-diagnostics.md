# Health and diagnostics

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## Health checks

**Mohs contributes a `HealthIndicator` under the `mohs` key**, when the host application brings the
actuator on its classpath (`spring-boot-health` is an `optional` dependency of the starter, so an
application without an actuator inherits nothing and the auto-configuration stays inert).

| Engine state | Status | Why |
| --- | --- | --- |
| `RUNNING` | `UP` | Claiming and heartbeating |
| `PAUSED`, `DRAINING` | `OUT_OF_SERVICE` | Alive and heartbeating, deliberately not claiming — an operator's decision or a shutdown in progress, neither of which is a fault |
| `CREATED`, `STOPPED` | `DOWN` | No loop is running: work assigned to this node will not move |

`OUT_OF_SERVICE` rather than a custom `DEGRADED`, because Boot already maps it to 503 and already
orders it below `UP` — a status Boot does not know would answer 200 and quietly keep a paused node
in the load balancer.

**It never touches the database.** That is the whole design: an indicator that probes the store
turns a database outage into a rolling restart of every healthy pod. `MohsLifecycle#state()` reads a
field of the local engine, so the answer costs nothing and cannot block.

Turn it off the standard way (`management.health.mohs.enabled=false`), or declare your own bean
named `mohsHealthIndicator` to replace the mapping.

The other signals:

| Signal | Source | Scope |
| --- | --- | --- |
| `mohs.lifecycle().state()` | The Java API | This node |
| `GET /nodes` | REST | The cluster, as recorded in `mohs_nodes` |
| `GET /runners` | REST | This node's executor occupancy |
| `mohs.node.inflight` / `mohs.node.capacity` | Metrics | This node's saturation |
| `mohs.tick.failed{step}` | Metrics | Whether the tick is degrading |
| The `mohs-engine-loop` thread's existence | A thread dump | Whether the loop is alive at all |

### The engine's states

| State | Meaning | Claims? | Heartbeats? |
| --- | --- | --- | --- |
| `CREATED` | Constructed, not started | No | No |
| `RUNNING` | Normal operation | Yes | Yes |
| `PAUSED` | This node stopped claiming; the cluster continues | No | Yes |
| `DRAINING` | Shutdown in progress, waiting for in-flight work | No | Yes |
| `STOPPED` | The loop has ended | No | One final heartbeat, then no |

`PAUSED` and `DRAINING` nodes **still heartbeat** — which is why `GET /nodes` can show a draining
node — but they are excluded from shard assignment, so they hold no slice of the queue.

### Liveness versus readiness

The standard trap applies here as everywhere: **a liveness probe must not depend on the database.**
A database outage would otherwise restart every healthy pod.

| Probe | What it should check |
| --- | --- |
| Liveness | The process responds. Do **not** include the engine's state — a `DRAINING` node is deliberately winding down, and a `PAUSED` node is an operator's decision |
| Readiness | `state == RUNNING`, if you want a paused node removed from a load balancer. Note that Mohs' work does not arrive through the load balancer, so readiness matters only for the REST API and the dashboard |
| Startup | Boot can take time during bean registration and the initial database-clock sample. `startupProbe` is the right mechanism, not a guessed `initialDelaySeconds` |

## Tracing

**Mohs creates no spans and propagates no trace context.** There is no Micrometer Tracing or
OpenTelemetry dependency in any module's `pom.xml`.

The intended integration point is `ExecutionInterceptor`, which runs on the attempt's own thread:

```java
@Bean
ExecutionInterceptor tracing(Tracer tracer) {
    return (ctx, chain) -> {
        Span span = tracer.nextSpan()
                .name("mohs.job." + ctx.jobKey().value())
                .tag("mohs.execution_id", ctx.executionId().value())
                .tag("mohs.attempt", String.valueOf(ctx.attempt()))
                .start();
        try (var ws = tracer.withSpan(span)) {
            chain.proceed();
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    };
}
```

**Trace context does not survive the enqueue.** A job scheduled inside a traced request runs later,
on another thread and possibly on another node; the interceptor above starts a *new* trace at
execution time. To link them, put a trace or correlation id **into the payload** yourself and set it
as a tag in the interceptor.

## Diagnosing a live node

### Thread dump

```bash
jcmd <pid> Thread.print
# or, structured:
jcmd <pid> Thread.dump_to_file -format=json /tmp/threads.json
```

What to look for:

| Thread | Expectation |
| --- | --- |
| `mohs-engine-loop` | **Exactly one.** Normally parked in `awaitNanos`. Its absence means the loop died — check the log for `engine loop died` |
| `mohs-runner-io-*` | Virtual; count up to the runner's `maxConcurrent` |
| `mohs-runner-cpu-*` | Platform; between `coreSize` and `maxSize` |
| `mohs-completion-flusher` | Exactly one, virtual. Normally in `queue.poll` |
| `mohs-events-*` | Up to `event-concurrency` |
| `mohs-overview-sse` | One, if the REST API is on |
| `mohs-clock-sync-*` | One, only in `database` time mode |

Every Mohs thread is named, deliberately, for exactly this moment.

### Flight recording

```bash
java -XX:StartFlightRecording=filename=mohs.jfr,settings=profile -jar app.jar
jfr print --events jdk.VirtualThreadPinned mohs.jfr
```

**Note on pinning diagnostics**: `-Djdk.tracePinnedThreads` was removed in JDK 24 and is a **silent
no-op** on the JDK 25 this project targets. Use JFR's `jdk.VirtualThreadPinned` event instead. In
practice pinning should not occur: carrier pinning by `synchronized`/`Object.wait()` was eliminated
in JDK 24, and the remaining causes are native/JNI frames and class initialisers.

### Database-side inspection

```sql
-- Backlog, by shard
SELECT shard, COUNT(*) FROM mohs_ready WHERE visible_at <= now() GROUP BY shard ORDER BY 2 DESC;

-- What is executing across the cluster, and who owns it
SELECT node_id, epoch, COUNT(*) FROM mohs_lease GROUP BY node_id, epoch;

-- Who the cluster believes is alive
SELECT node_id, state, last_heartbeat_at, epoch, expires_at,
       expires_at > now() AS alive
  FROM mohs_nodes ORDER BY last_heartbeat_at DESC;

-- Armed triggers
SELECT job_key, next_fire_at, paused, orphaned, retired
  FROM mohs_job_definitions
 WHERE next_fire_at IS NOT NULL ORDER BY next_fire_at;

-- The most common failure classes in the last hour
SELECT error_type, COUNT(*) FROM mohs_attempt
 WHERE outcome = 'FAILED' AND finished_at > now() - interval '1 hour'
 GROUP BY error_type ORDER BY 2 DESC;

-- Rate-limit buckets
SELECT name, max_count, window_duration, tokens, refilled_at FROM mohs_rate_limits;
```

`error_type` exists precisely because "which exception class is failing" is the number-one
operational query.

### The dashboard

With `mohs-ui` on the classpath and `mohs.api.enabled=true`, `/mohs-ui` gives Overview, Jobs,
Executions, Rate Limits and Runners, live over SSE at a 2-second cadence.

## Interpreting an idle node

An idle node is normal — but "idle" has several distinct causes, and they are distinguishable:

| Observation | Cause |
| --- | --- |
| `mohs.claim.batch.size` at 0, no `tick.failed`, an empty `mohs_ready` | Genuinely idle |
| `mohs.tick.failed{step}` climbing | A tick step is failing — check the WARN naming the step |
| A "this node owns no shard of 64" WARN | More than 64 `RUNNING` nodes |
| `inflight / capacity` near 1.0 | Dispatch-saturated, not idle |
| No `mohs-engine-loop` thread | The loop died — restart |
| `state == PAUSED` | An operator paused this node |
| A backlog exists but this node claims nothing | Check `mohs_lease` for this node's `node_id`; check whether its shard slice is empty; check `mohs.claim.requeued` for a guard blocking every candidate |

## The most useful missing signal

**Per-runner occupancy as a metric.** Queue depth, once the missing signal, is `mohs.queue.depth`
now — sampled by the engine's own tick, not on the scrape. Per-runner occupancy is still only
visible through `GET /runners`; everything else needed for triage is already exposed.
