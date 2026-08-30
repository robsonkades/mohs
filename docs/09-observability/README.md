# 9. Observability

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [metrics.md](metrics.md) | Every `mohs.*` meter, the cardinality rule, how to read the operational picture, alerting starting points, and what is not measured |
| [logging.md](logging.md) | Level distribution, the loggers, every message that matters, conventions, correlation, sensitive-data hazards |
| [health-and-diagnostics.md](health-and-diagnostics.md) | The absent `HealthIndicator` and how to write one, probe guidance, thread dumps, JFR, database-side inspection queries |

## The five signals to watch

| Signal | Why it is the one to watch |
| --- | --- |
| `mohs.lease.reclaimed{reason}` | **Any non-zero value means a node died or stopped.** The label already carries the triage |
| `mohs.tick.failed{step}` | Distinguishes "idle because the queue is empty" from "idle because every tick dies" |
| `mohs.dispatch.latency{job}` p99 | The SLO visible to the user: scheduled → started |
| `mohs.node.inflight / mohs.node.capacity` | Saturation. Combine with `mohs.claim.batch.size` to tell claim-bound from dispatch-bound |
| `sum(rate(mohs_attempt_total)) / sum(rate(mohs_execution_total))` | Attempts per execution — the health ratio |

## Known observability gaps

| Gap | Impact |
| --- | --- |
| No `HealthIndicator` | Kubernetes probes have to be written by the host |
| No tracing | No span crosses the enqueue boundary; `ExecutionInterceptor` is the integration point |
| No queue-depth metric | Backlog alerting must poll `GET /overview` |
| No MDC populated by Mohs | Correlation inside a handler requires an interceptor |
| `mohs.node.*` gauges lack a `node` label | Two engines in one registry collide silently |
