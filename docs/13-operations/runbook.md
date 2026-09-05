# Operational runbook

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

Task-oriented procedures. For diagnosing an unknown problem, start with
[troubleshooting](troubleshooting.md).

## Everyday operations

### Pause a job cluster-wide

```bash
curl -X POST -H "X-Mohs-Actor: alice@acme.com" \
  https://app/api/mohs/v1/jobs/nightly-invoices/pause
```

Or `mohs.pause(JobKey.of("nightly-invoices"))`.

| Effect | Detail |
| --- | --- |
| Automatic firing | **Stops**, cluster-wide, immediately |
| Manual scheduling | **Still works** — `POST .../schedule` and `Mohs.schedule` are unaffected |
| In-flight executions | Unaffected — a pause is not a cancel |
| Already-queued occurrences | Still claimed and run |
| Duration | Until an explicit resume. **A redeploy does not resume it** |

### Resume, and the burst that follows

```bash
curl -X POST https://app/api/mohs/v1/jobs/nightly-invoices/resume
```

> **Expect a burst.** On resume, every occurrence due within `misfire-threshold` (60 s) is
> materialised **at once**. That is not misfire — an occurrence within the threshold fires late under
> any policy. The burst is proportional to the pause length.

For a long pause on a dense schedule, consider setting `misfire: IGNORE` before resuming, or
rescheduling to a future point first.

### Cancel an execution

```bash
curl -X POST -H "X-Mohs-Actor: alice@acme.com" \
  https://app/api/mohs/v1/executions/{id}/cancel
```

Returns **202** with the current state. Cancellation is **cooperative**:

| Current state | What happens |
| --- | --- |
| `ENQUEUED` / `RETRY_WAITING` | `CANCELLED` immediately |
| `RUNNING` | The request is recorded; the owning node observes it within at most one loop interval, and **the handler decides when to stop** |
| Terminal | Nothing changes |

A handler that never checks `ctx.cancellationRequested()` and never blocks is uncancellable. Only the
watchdog bound (if configured) or node death resolves it.

### Retry a failed execution

```bash
curl -X POST -H "X-Mohs-Actor: alice@acme.com" \
  https://app/api/mohs/v1/executions/{id}/retry
```

Rearms the **same** execution, bypassing the retry budget. A 409 means one of: not `FAILED`, a
retired job, a duplicate request, or **a batch member** — which is refused permanently. To redo a
batch member's work, schedule the job standalone.

### Change a schedule in an emergency

```bash
curl -X PATCH -H "Content-Type: application/json" -H "X-Mohs-Actor: alice@acme.com" \
  -d '{"type":"INTERVAL","interval":"PT30M","afterFinish":false}' \
  https://app/api/mohs/v1/jobs/heavy-sync/schedule
```

> **This reverts on the next boot** under the default `on-conflict=override` — the scanner restores
> the code's version with a logged diff. The response says so in its `notice` field. **Follow up with
> a code change.**

### Throttle a job in an emergency

```bash
curl -X PATCH -H "Content-Type: application/json" -H "X-Mohs-Actor: alice@acme.com" \
  -d '{"max": 10, "window": "PT1M"}' \
  https://app/api/mohs/v1/rate-limits/smtp
```

| Note | Detail |
| --- | --- |
| The limit must already exist | A `PATCH` never creates one — a 404 names the property to declare |
| The bucket survives | Its balance is clamped to the new ceiling. Lowering does not give back what was consumed |
| Reverts on boot | Same as a schedule `PATCH` |
| A 503 | The bucket row is contended and **nothing changed**; retry in a few seconds |

### Drain a node for maintenance

Send `SIGTERM` — the normal shutdown path does exactly this. To drain without stopping:

```java
mohs.lifecycle().drain(Duration.ofSeconds(30));
```

The node stops claiming, waits for in-flight work, and continues heartbeating in `DRAINING` — so it
holds no shard slice and its peers pick up the work.

### Retire a job

| Source | How |
| --- | --- |
| `@MohsJob` | **Delete the annotation and deploy.** The next boot marks it `ORPHANED`; history is kept, and restoring the annotation clears the flag |
| Programmatic | `mohs.remove(JobKey.of("..."))` — a soft retire that drains the queue and preserves history |

`Mohs.remove` on an annotated job throws, naming the correct procedure.

## Incident procedures

### A node died

**Nothing to do.** This is the designed path:

1. The node's lease expires (≤ `node-lease-ttl`, 15 s by default).
2. A peer's reaper reclaims its leases through the retry budget.
3. Work re-executes on a node with headroom.

To confirm:

```sql
SELECT node_id, state, last_heartbeat_at, expires_at, expires_at > now() AS alive
  FROM mohs_nodes ORDER BY last_heartbeat_at DESC;
```

And `increase(mohs_lease_reclaimed_total[10m])` should be non-zero with `reason="retry"`.

**If `reason="attempts_exhausted"` dominates**, jobs are declaring `retries = 0` and losing work under
node failure. That is an opt-in to at-most-once, and it may not be what the job's author intended.

### A job is failing repeatedly

```sql
-- which exception class, in the last hour
SELECT error_type, COUNT(*)
  FROM mohs_attempt
 WHERE outcome = 'FAILED' AND finished_at > now() - interval '1 hour'
 GROUP BY error_type ORDER BY 2 DESC;
```

Then read the detail of one: `GET /executions/{id}` returns every attempt with its message. The
**full stack trace** is in the server log's WARN at the moment of failure — that is the only place it
appears by default.

To stop the bleeding: pause the job. To retry after fixing: `POST /executions/{id}/retry` per
execution.

### The backlog is growing

Diagnose with the two claim metrics:

| `mohs.claim.batch.size` | `inflight / capacity` | Diagnosis | Action |
| --- | --- | --- | --- |
| Full | Low | Claim-bound | Raise `claim-rounds` or `batch-size`; lower `poll-interval` |
| Small | High | Dispatch-bound | Raise `dispatch-concurrency` **and the pool** |
| Small | Low with a real backlog | A guard is blocking | Check `mohs.claim.requeued{reason}` |
| Full | High | Both saturated | Add nodes (up to 64) |

Also check for a **stuck job dominating the queue head** — a sustained rise in
`mohs.claim.requeued` means the inadmissible list is arriving late.

### The database went down

**Nothing to do.** Measured behaviour (a 30-second `docker pause` mid-drain): no loss, no exception
storm, the first completion 259 ms after unpause, 24,109 completions in the following 10 seconds,
**zero re-executions** on a single node.

The engine backs off exactly as it does for an empty queue, and maintenance steps are isolated so one
failing step does not stop the claim. Ownership stands; a reaper resolves anything whose node dies.

### The cluster is re-executing work

Check, in this order:

1. **Clocks.** `grep "clock moved backwards"` — the log line names NTP and `mohs.time.mode` as the
   things to check. A backwards jump between heartbeats makes peers declare a live node dead.
2. **Node lease expiry under load.** `grep "node lease expired"` — a node stalled longer than its
   TTL. Raise `node-lease-ttl`, or find the stall (GC, a blocked tick, a saturated database).
3. **Shutdown grace versus lease TTL.** With a 15 s TTL and a 30 s grace, more than half the drain is
   uncovered by any heartbeat, and a peer may reclaim work still running.
4. **Watchdog bound too low.** `grep "exceeded mohs.engine.watchdog-timeout"` — it releases ownership
   of a still-healthy long-running job.

Note that duplicates are **within contract** (at-least-once). The question is whether the *rate* is
explained by one of the above.

### A node is alive but claims nothing

| Check | Meaning |
| --- | --- |
| `grep "owns no shard of 64"` | More than 64 `RUNNING` nodes. Reduce the count |
| `mohs.tick.failed{step}` climbing | A tick step is failing; the WARN names it |
| No `mohs-engine-loop` thread in a dump | The loop died — the ERROR says "Restart the instance" |
| `state` is `PAUSED` | An operator paused this node |
| `mohs.claim.requeued` high | A guard is rejecting everything: a closed window, an exhausted rate limit, or a full concurrency cap |

### The dashboard is empty

| Check | Fix |
| --- | --- |
| `grep "mohs-ui is on the classpath but mohs.api.enabled=false"` | Set `mohs.api.enabled=true` |
| `grep "mohs-ui is pinned to /api/mohs/v1 but mohs.api.base-path="` | The dashboard pins the default prefix in compiled JavaScript. Serve the API at the default, or proxy to it |
| Browser 401/403 | The host's `SecurityFilterChain` is blocking it |

### Contention alerts (503 on a `PATCH`)

The only row the API contends for with the hot path is the rate-limit bucket. A 503 means **nothing
changed**. Retry after a few seconds. Persistent 503s mean the bucket is saturated by claim traffic —
consider whether the limit is set far below demand, which maximises contention on that row.

## Periodic maintenance

### Retention

Set `mohs.engine.history-retention` when execution payloads, attempts and completed batches must
expire. Cleanup runs automatically in bounded batches when the property is enabled. Idempotency
records use `mohs.engine.idempotency-retention` and are pruned automatically; the default is seven
days. See [data lifecycle](../06-data/data-lifecycle.md) for ordering and operational effects.

### Reviewing warnings

Worth a periodic grep, because each one names a real gap:

```bash
grep -E "timeout .* >= mohs.engine.watchdog-timeout|dialect=h2|NO authentication|owns no shard|max-concurrent .* below" app.log
```

### Verifying the cluster's view of itself

```sql
SELECT node_id, state, last_heartbeat_at, epoch, expires_at,
       expires_at > now() AS alive
  FROM mohs_nodes ORDER BY last_heartbeat_at DESC;
```

Rows older than `node-lease-ttl × 10` should not be there — the purge collects them. Their presence
means the purge step is failing (check `mohs.tick.failed{step="stale-node-purge"}`).

## Capacity changes

### Adding nodes

Start more replicas. Shard assignment is derived and rebalances within one heartbeat. **The ceiling
is 64.**

### Removing nodes

`SIGTERM` and let the drain run. Ensure `terminationGracePeriodSeconds` exceeds the drain grace plus
the web server's phase, or the pod is killed mid-drain and its work is reclaimed instead of finished.

### Changing `dispatch-concurrency`

Remember it is three things at once — the in-flight ceiling, the claim budget, **and** the size of
the built-in `io` runner. Re-measure database, pool and downstream latency after changing it.

## Recovering from operator error

| Mistake | Recovery |
| --- | --- |
| Paused a job and forgot | `POST .../resume`. Expect the burst |
| `PATCH`ed a schedule and want it back | Restart the node — `on-conflict=override` restores the code's version, with a diff logged |
| Lowered a rate limit too far | `PATCH` it back. The bucket refills from the moment of the change; it does not retroactively restore |
| Cancelled the wrong execution | `POST .../retry` **if** it is `FAILED`. A `CANCELLED` execution **cannot** be retried — cancelling was an explicit decision. Schedule the job again |
| Deleted a `@MohsJob` annotation by accident | Restore it and deploy. The upsert clears `ORPHANED` — the source reappearing is proof of life |
| Called `Mohs.remove` by mistake | `mohs.define(...)` the same key. Retirement is soft; an upsert resurrects it |
| Edited `mohs_ready`/`mohs_lease` by hand and set `attempt` (or `attempt_number`) to `0` | Every tick ends in `engine tick failed — will retry next tick` at ERROR, with `IllegalArgumentException: attemptNumber must be >= 1` underneath, and **nothing in that claim batch dispatches** until the row is fixed: an attempt below 1 is not fenceable by any path (claim, requeue, reap), so no grant can carry it and the whole batch unwinds. Set it back to `1`, or delete the row. The engine does not guard against it by decision — the column is written by the engine only, and a guard would be a flag for a hypothetical |
