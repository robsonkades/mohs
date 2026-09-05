# Metrics

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository (`io.mohs.engine.EngineMetrics`)

Metrics are **always on**. A host with Micrometer in the context (Actuator, Prometheus) sees
everything under `mohs.*`; with no registry, a local `SimpleMeterRegistry` keeps the engine
identical — inert for the host, and with **no conditional path in the hot code**.

## The complete inventory

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `mohs.claim.latency` | Timer | — | Duration of one claim round against the store |
| `mohs.claim.batch.size` | Distribution summary | — | Executions claimed per round. Interpret together with backlog, admission guards and dispatch occupancy |
| `mohs.claim.requeued` | Counter | `reason` | Post-claim admission losses |
| `mohs.dispatch.latency` | Timer | `job` | **From `scheduled_at` to the handler's start — the SLO visible to the user** |
| `mohs.attempt.total` | Counter | `job`, `outcome` | Every confirmed attempt |
| `mohs.execution.duration` | Timer | `job`, `outcome` | One attempt's runtime, from the persisted `Attempt` timestamps |
| `mohs.execution.total` | Counter | `job`, `outcome` | **Terminal transitions only** — a retry is not yet the execution's outcome |
| `mohs.lease.reclaimed` | Counter | `reason` | A reaper reclaimed a dead node's lease |
| `mohs.tick.failed` | Counter | `step` | A tick step threw |
| `mohs.node.inflight` | Gauge | — | Executions currently dispatched on this node |
| `mohs.queue.depth` | Gauge | — | Entries visible to a claim — the backlog, **cluster-wide**. Sampled by the engine's tick every 10 s, never on the scrape, so a metrics pipeline cannot decide the load it is measuring. Every node publishes the same number: aggregate with `max`, never `sum` |
| `mohs.node.capacity` | Gauge | — | This node's `dispatch-concurrency` |

### Label values

| Label | Values |
| --- | --- |
| `job` | The `JobKey` — bounded by the number of definitions |
| `outcome` | `succeeded`, `failed`, `cancelled` (and the state names for the terminal counter) |
| `reason` on `mohs.claim.requeued` | `concurrency-cap`, `rate-limit`, `window-closed`, `stray-lease` |
| `reason` on `mohs.lease.reclaimed` | `retry`, `attempts_exhausted`, `job_retired`, `cancelled` |
| `step` on `mohs.tick.failed` | `signal-job-timeouts`, `poll-cancel-requests`, `signal-watchdog-overruns`, `queue-depth-sample`, `idempotency-prune`, `history-prune`, `reap-orphaned-leases` (counted once per failed reclaim chunk of 50, so up to 10 per tick), `reconcile-stray-leases`, `stale-node-purge`, and `tick` — the whole tick dying outside a maintenance step: the heartbeat, the definitions read, the due-trigger read or the claim |

**Label values are contract, just as much as the names**: preserve the exact spelling above.
Outcomes use lower case, reclaim reasons use underscores and several step/requeue values use hyphens. The first saved
dashboard freezes that vocabulary.

## The cardinality rule

Enforced in code rather than by convention: **an execution id NEVER becomes a label.** The only
unbounded-looking label is `job`, and it is bounded by the number of definitions.

## Two design decisions worth understanding

### Pre-registered `requeued` reasons

All four `reason` values are registered as counters at construction, before any increment:

> A lazy counter is only born on its first increment, and an alert using `increase()` cannot
> distinguish "series missing" from zero — the series existing from boot is the contract.

### `attempt.total` versus `execution.total`

Every attempt counts in `mohs.attempt.total`; only a **terminal transition** counts in
`mohs.execution.total`. A retry is not yet the execution's outcome, and

> the attempts-to-executions ratio is the health indicator.

```promql
# average attempts per execution, per job
sum by (job) (rate(mohs_attempt_total[5m]))
  /
sum by (job) (rate(mohs_execution_total[5m]))
```

A value near 1.0 is consistent with one attempt per terminal execution, including terminal
failures. Compare it with failure rates; a short window can also differ while jobs remain in flight.

## Reading the operational picture

### Is the system healthy?

```promql
# terminal throughput
sum(rate(mohs_execution_total[1m]))

# failure ratio
sum(rate(mohs_execution_total{outcome="failed"}[5m])) / sum(rate(mohs_execution_total[5m]))

# dispatch latency, the user-visible SLO
histogram_quantile(0.99, sum by (le, job) (rate(mohs_dispatch_latency_seconds_bucket[5m])))
```

### Is the node saturated?

```promql
mohs_node_inflight / mohs_node_capacity
```

Sustained near 1.0 means the node is dispatch-bound. Combine with the claim batch size:

| `mohs.claim.batch.size` | `inflight / capacity` | Diagnosis |
| --- | --- | --- |
| Consistently full (= `batch-size`) | Low | **Claim-bound** — raise `claim-rounds` or `batch-size`, or lower `poll-interval` |
| Consistently small | High | **Dispatch-bound** — raise `dispatch-concurrency`, or the work is genuinely slow |
| Consistently small | Low | Check for an empty shard, future work or admission guards; this alone does not prove the whole queue is empty |
| Full | High | Both are saturated — add nodes |

### Is something wrong?

| Query | Meaning |
| --- | --- |
| `increase(mohs_lease_reclaimed_total[10m]) > 0` | A node died or stopped. The `reason` label says what the reclaim decided |
| `increase(mohs_tick_failed_total[10m]) > 0` | A tick step is failing. **This is the metric that distinguishes "idle because the queue is empty" from "idle because every tick dies"** — a distinction that previously existed only in a line of `log.error` |
| `rate(mohs_claim_requeued_total[5m])` rising steadily | The inadmissible list is arriving late — typically the queue's head dominated by a stuck job |
| `mohs_claim_latency_seconds` p99 rising | The database is degrading, or the backlog is growing |

## Alerting starting points

```yaml
- alert: MohsNodeDied
  expr: increase(mohs_lease_reclaimed_total[10m]) > 0
  annotations:
    summary: "A Mohs node's lease was reclaimed ({{ $labels.reason }})"

- alert: MohsTickStepFailing
  expr: increase(mohs_tick_failed_total[10m]) > 3
  annotations:
    summary: "Mohs tick step '{{ $labels.step }}' is failing repeatedly"

- alert: MohsRetriesExhausted
  expr: increase(mohs_execution_total{outcome="failed"}[15m]) > 0
  annotations:
    summary: "Job {{ $labels.job }} is failing terminally"

- alert: MohsDispatchLatencyHigh
  expr: histogram_quantile(0.99,
          sum by (le, job) (rate(mohs_dispatch_latency_seconds_bucket[5m]))) > 60
  annotations:
    summary: "p99 dispatch latency above 60s for {{ $labels.job }}"

- alert: MohsNodeSaturated
  expr: mohs_node_inflight / mohs_node_capacity > 0.9
  for: 10m
```

`mohs.lease.reclaimed` is the single best alert in the set: **any non-zero value means a dead or
stopped node**, and the `reason` label already carries the triage.

## Measurement details

| Detail | Value |
| --- | --- |
| Execution duration source | The **persisted `Attempt` timestamps** — the same window the dashboard reads, so there is a single source |
| Claim latency source | `System.nanoTime()` in the caller, honouring the project's monotonic-time invariant |
| Behaviour during a backwards clock resync | `DatabaseClock` keeps emitted instants nondecreasing when resynchronizing. Application-clock mode or a custom clock can still regress; Micrometer drops negative durations |
| Gauge strength | `strongReference(true)` — without it the gauge's ephemeral method-reference supplier would vanish at the first GC |

## A recorded limitation

`mohs.node.inflight`, `mohs.node.capacity` and `mohs.queue.depth` carry **no `node` label**. With two
engines in the same registry (one Mohs per `DataSource`), they collide on the meter id — Micrometer
keeps the first meter and logs one generic WARN naming neither the engine nor the fix, so a
dashboard would show one engine's saturation, or backlog, under the other's name.

The engine refuses that at construction instead: the second bind throws an `IllegalStateException`
naming the gauge and the mitigation, which is one `MeterRegistry` per engine — a registry the
actuator endpoint does not scrape, so the second engine's `mohs.*` need their own exporter. That is
the mitigation, not the fix; the fix is the label, and it is a deliberate deferral: the trigger for
adding it is the first real multi-engine scenario, paying the cardinality only then. The counters and
timers (`mohs.claim.latency`, `mohs.attempt.total`, ...) do not collide — with two engines in one
registry they **sum** both engines' samples into one series, under the same limitation.

## What is not measured

| Gap | Consequence |
| --- | --- |
| Lease-table size | Only available via `GET /overview`, not as a metric |
| Rate-limit bucket balance | Only via `GET /rate-limits` |
| Per-runner occupancy | Only via `GET /runners`. `mohs.node.inflight` is the node-wide total |
| Trigger-firing counts | Not instrumented |
| Group-commit flush size or latency | Not instrumented |
| Event-publication drops | Logged at WARN, not counted |
| Idle-gate probe hit rate | Not instrumented |

Queue depth, once the most useful missing signal, is `mohs.queue.depth` now. The most useful addition
left would be **per-runner occupancy** — `mohs.node.inflight` is the node-wide total.

## Micrometer wiring

```java
@Bean
public EngineMetrics mohsEngineMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
    return new EngineMetrics(meterRegistry.getIfAvailable(SimpleMeterRegistry::new));
}
```

The host needs only `spring-boot-starter-actuator` plus a registry (for example
`micrometer-registry-prometheus`) and:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
  metrics:
    distribution:
      percentiles-histogram:
        mohs.dispatch.latency: true
```

The histogram setting is required for the `_bucket` queries above; verify the series on the
host's Prometheus endpoint and adapt alert thresholds to the workload.

Mohs adds no Actuator endpoint of its own; it contributes a `HealthIndicator` under the `mohs` key
when the host brings the actuator — see [health and diagnostics](health-and-diagnostics.md).
