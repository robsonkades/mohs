# Metrics

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (`io.mohs.engine.EngineMetrics`)

Metrics are **always on**. A host with Micrometer in the context (Actuator, Prometheus) sees
everything under `mohs.*`; with no registry, a local `SimpleMeterRegistry` keeps the engine
identical — inert for the host, and with **no conditional path in the hot code**.

## The complete inventory

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `mohs.claim.latency` | Timer | — | Duration of one claim round against the store |
| `mohs.claim.batch.size` | Distribution summary | — | Executions claimed per round. **Full batches mean claim-bound; small ones mean dispatch-bound** |
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
| `step` on `mohs.tick.failed` | `signal-job-timeouts`, `poll-cancel-requests`, `signal-watchdog-overruns`, `queue-depth-sample`, `idempotency-prune`, `reap-orphaned-leases`, `reconcile-stray-leases`, `stale-node-purge` |

**Label values are contract, just as much as the names**: lower case, `snake_case`. The first saved
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

A value near 1.0 means jobs succeed first time. A rising value means retries are climbing.

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
| Consistently small | Low | The queue is empty — the system is idle |
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
| Behaviour during a backwards clock resync | The duration window may come out negative and Micrometer's `Timer` **silently drops the sample**. Missing samples during a resync is expected behaviour, not data loss |
| Gauge strength | `strongReference(true)` — without it the gauge's ephemeral method-reference supplier would vanish at the first GC |

## A recorded limitation

`mohs.node.inflight` and `mohs.node.capacity` carry **no `node` label**. With two engines in the same
registry (one Mohs per `DataSource`), they collide on the meter id and the second bind is silently
ignored by Micrometer.

This is a deliberate deferral: the trigger for adding the tag is the first real multi-engine
scenario, paying the cardinality only then.

## What is not measured

| Gap | Consequence |
| --- | --- |
| Queue depth (`mohs_ready` row count) | Only available via `GET /overview`, not as a metric. Backlog alerting must go through the API |
| Lease-table size | Same |
| Rate-limit bucket balance | Only via `GET /rate-limits` |
| Per-runner occupancy | Only via `GET /runners`. `mohs.node.inflight` is the node-wide total |
| Trigger-firing counts | Not instrumented |
| Group-commit flush size or latency | Not instrumented |
| Event-publication drops | Logged at WARN, not counted |
| Idle-gate probe hit rate | Not instrumented |

The most useful addition would be **queue depth as a gauge**: it is the leading indicator of a
backlog, and it is the one number an operator currently has to poll an HTTP endpoint to see.

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
```

Mohs adds no Actuator endpoint of its own and registers no `HealthIndicator` — see
[health and diagnostics](health-and-diagnostics.md).
