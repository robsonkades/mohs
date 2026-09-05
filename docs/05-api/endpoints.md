# REST endpoints

Status: Active · Last Reviewed: 2026-09-04 · Source of Truth: Repository (`io.mohs.rest`)

All paths are relative to `mohs.api.base-path` (default `/api/mohs/v1`), which is itself relative to
the host's `server.servlet.context-path`.

**Authentication: none.** See [security](../08-security/security-overview.md).

## Endpoint index

| Method | Path | Purpose | Success |
| --- | --- | --- | --- |
| `GET` | `/overview` | The dashboard's polling anchor | 200 |
| `GET` | `/overview/stream` | The same snapshot pushed over SSE | 200 `text/event-stream` |
| `GET` | `/jobs` | Every registered job | 200 |
| `GET` | `/jobs/{jobKey}` | One job | 200 |
| `POST` | `/jobs/{jobKey}/schedule` | Invoke a job | **202** |
| `POST` | `/jobs/{jobKey}/pause` | Suspend automatic firing | 200 |
| `POST` | `/jobs/{jobKey}/resume` | Re-enable firing | 200 |
| `PATCH` | `/jobs/{jobKey}/schedule` | Change the schedule at runtime | 200 |
| `GET` | `/jobs/{jobKey}/executions` | That job's executions, paginated | 200 |
| `GET` | `/executions` | Global execution search, paginated | 200 |
| `GET` | `/executions/{id}` | One execution, with attempts | 200 |
| `POST` | `/executions/{id}/cancel` | Cooperative cancellation | **202** |
| `POST` | `/executions/{id}/retry` | Manual retry of a `FAILED` execution | **202** |
| `GET` | `/rate-limits` | Declared limits and bucket balances | 200 |
| `PATCH` | `/rate-limits/{name}` | Adjust a limit at runtime | 200 |
| `GET` | `/runners` | **This node's** runners and occupancy | 200 |
| `GET` | `/nodes` | The cluster's nodes | 200 |
| `GET` | `/batches/{id}` | Batch counters | 200 |

---

## Overview

### `GET /overview`

The dashboard's polling anchor: live-work counts plus terminal throughput.

| Parameter | In | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `window` | query | No | `60s` | A duration in either style (`15m` or `PT15M`). Clamped to `[1s, 1h]` — asking for more is not an error, and the **applied** window travels back in `throughput.window` |

```json
{
  "executionCountsByStatus": { "ENQUEUED": 12, "RUNNING": 3, "RETRY_WAITING": 0 },
  "throughput": { "window": "PT1M",  "succeeded": 940, "failed": 6, "ratePerSecond": 15.77 },
  "recent":     { "window": "PT10S", "succeeded": 158, "failed": 1, "ratePerSecond": 15.90 }
}
```

**Why two throughput readings.** The live counts are instantaneous gauges, and by Little's Law
(`L = λ × W`) they are about zero for any fast job: a 1 ms job fired once per second has an average
concurrency of 0.001, so sampling every 2 s finds it in one reading out of a thousand. Measured on
the dashboard: 4 samples with live work out of 60, peaking at 1, while 39 executions completed per
minute. Anyone receiving only `L` concludes the system is idle while it works.

`recent` is a **short, fixed 10 s window whose only purpose is to be divided** — `ratePerSecond`
yields the missing λ. It cannot be derived from `throughput`, which is a long sliding window:
differencing two consecutive readings gives *(what entered) − (what left through the other end)*,
which is zero in steady state.

**The two are NOT nested, and that is contract.** `recent` is not a slice of `throughput`. Two
independent reasons: the long window is caller-chosen and may be *shorter* than 10 s (`?window=1s`
is valid); and the counts are separate round trips in distinct snapshots — measured at about 19 rows
of asymmetry per call at a 4 k/s operating point. **Adding, subtracting or stacking one on the other
produces a negative number sooner or later.**

`executionCountsByStatus` always carries all three live states, zeros included — an absent key and a
zero are the same information, and a polling contract should not force the consumer to tell them
apart. Terminal states deliberately have **no all-time count**: history grows without bound, and this
endpoint is polled.

| Status | Condition |
| --- | --- |
| 200 | |
| 422 | `window` is not a parsable duration |

### `GET /overview/stream`

Server-Sent Events. See [streaming](streaming.md).

---

## Jobs

### `GET /jobs`

Returns `JobResponse[]`. Bounded cardinality, no pagination.

```json
[{
  "jobKey": "nightly-invoices",
  "name": "nightly-invoices",
  "handlerType": "com.acme.Invoices",
  "schedule": { "type": "CRON", "expression": "0 0 3 * * *", "zone": "America/Sao_Paulo" },
  "runner": "io", "window": null, "rateLimit": null,
  "misfire": "IGNORE", "retries": 3, "timeout": "PT10M", "retryPolicy": null,
  "source": "ANNOTATION", "paused": false, "nextFireAt": "2026-08-30T06:00:00Z"
}]
```

`name` falls back to the `jobKey` when no label was set. `nextFireAt` is `null` for an on-demand job
or a paused one.

`schedule` is a **discriminated union** with an explicit `type` in the JSON, so it is portable across
client languages:

| `type` | Fields |
| --- | --- |
| `CRON` | `expression`, `zone` |
| `INTERVAL` | `interval` (ISO-8601), `afterFinish` (boolean) |
| `ON_DEMAND` | — |

### `GET /jobs/{jobKey}`

| Status | Condition |
| --- | --- |
| 200 | |
| 404 | Unknown job — the body carries a `nearbyJobKeys` array of registered keys within Levenshtein distance 2 |

### `POST /jobs/{jobKey}/schedule`

Invoke a job.

| Parameter | In | Required |
| --- | --- | --- |
| `Idempotency-Key` | header | No — a blank header is treated as absent (tolerant reader); longer than 255 characters is a 422 |
| `X-Mohs-Actor` | header | No — falls back to `anonymous` |

```json
{
  "payload": { "invoiceId": 4711 },
  "at": "2026-08-30T03:00:00Z",
  "delay": "PT30M",
  "priority": "HIGH"
}
```

| Field | Required | Notes |
| --- | --- | --- |
| `payload` | Yes (may be `{}`) | Converted to the handler's declared parameter type. A job that declares no payload rejects a non-empty one |
| `at` | No | Absolute time |
| `delay` | No | ISO-8601 duration from now, **computed on the server** and therefore immune to client clock skew |
| `priority` | No | `CRITICAL`, `HIGH`, `NORMAL` (default), `LOW`, `BACKGROUND` |

`at` and `delay` are **mutually exclusive**; both absent means now. A negative `delay` is rejected —
it would silently become `at(now − X)`, an execution immediately due with no warning.

**202 Accepted** with `Location: …/executions/{id}`:

```json
{ "executionId": "0198e...", "jobKey": "send-invoice",
  "scheduledAt": "2026-08-30T03:00:00Z", "actor": "ops@acme.com" }
```

| Status | Condition |
| --- | --- |
| 202 | Accepted, or **deduplicated** — a repeat with the same `Idempotency-Key` returns the original receipt |
| 400 | Invalid `X-Mohs-Actor` (too long, control/bidi characters, or `scheduler`) |
| 404 | Unknown job |
| 422 | `at` and `delay` together; negative `delay`; payload incompatible with the handler's parameter type, **an unknown property included** (a misspelt field is refused, never a silent `null`); a payload sent to a job that accepts none; an `Idempotency-Key` longer than 255 characters |

### `POST /jobs/{jobKey}/pause` · `POST /jobs/{jobKey}/resume`

No body. Returns the updated `JobResponse` (200). Cluster-wide. **Manual scheduling still works while
paused.**

### `PATCH /jobs/{jobKey}/schedule`

Body is a `ScheduleView`:

```json
{ "type": "INTERVAL", "interval": "PT5M", "afterFinish": false }
```

Response is a `RuntimePatchResponse<JobResponse>`:

```json
{
  "resource": { "jobKey": "...", "schedule": { "type": "INTERVAL", ... }, ... },
  "notice": "Emergency change: it holds until the next boot; encode it in properties to make it permanent."
}
```

The trigger is recomputed from the clock in the same write; `ON_DEMAND` disarms the recurrence. The
actor is resolved **before** the mutation, so a 4xx genuinely means "nothing changed" — and the
audit log records what *this* actor asked for (the body), not the post-write snapshot, so two
concurrent `PATCH`es never swap authorship.

| Status | Condition |
| --- | --- |
| 200 | |
| 400 | Invalid actor |
| 404 | Unknown or retired job |
| 422 | Unrealisable schedule — a non-positive interval, a blank cron, or a syntactically valid cron that never fires |

### `GET /jobs/{jobKey}/executions`

| Parameter | In | Default |
| --- | --- | --- |
| `cursor` | query | none |
| `size` | query | 50, max 200 |

Returns `CursorPage<ExecutionSummaryResponse>`. 404 for an unknown job.

---

## Executions

### `GET /executions`

| Parameter | In | Notes |
| --- | --- | --- |
| `status` | query | `ENQUEUED`, `RUNNING`, `RETRY_WAITING`, `SUCCEEDED`, `FAILED`, `CANCELLED`. Applies to the **derived** state |
| `jobKey` | query | |
| `from`, `to` | query | ISO-8601 instants, filtering `scheduled_at` |
| `cursor` | query | Opaque; the previous page's last `executionId` |
| `size` | query | 50, max 200 |

```json
{
  "items": [{
    "executionId": "0198e...", "jobKey": "send-invoice", "state": "SUCCEEDED",
    "scheduledAt": "2026-08-29T14:00:00Z", "firedAt": null, "actor": "ops@acme.com"
  }],
  "nextCursor": "0198d..."
}
```

`firedAt` is the claim instant while `RUNNING`, and **`null` once the execution completes** —
ownership lives in the lease table, which is deleted at completion. The historical question "when
did each attempt start" belongs to `attempts[].startedAt` in the detail view.

The `status` filter targets different storage per value: a terminal value filters the column,
`RUNNING` filters ownership, and `ENQUEUED`/`RETRY_WAITING` filter the queue.

### `GET /executions/{id}`

The same shape **plus `attempts`**:

```json
{
  "executionId": "0198e...", "jobKey": "send-invoice", "state": "FAILED",
  "scheduledAt": "...", "firedAt": null, "actor": "ops@acme.com",
  "attempts": [
    { "number": 1, "startedAt": "...", "finishedAt": "...", "outcome": "FAILED",    "error": "connect timed out" },
    { "number": 2, "startedAt": "...", "finishedAt": "...", "outcome": "FAILED",    "error": "connect timed out" }
  ]
}
```

`error` is present **if and only if** `outcome` is `FAILED`. 404 for an unknown id.

### `POST /executions/{id}/cancel`

No body. **202 Accepted** with `Location: …/executions/{id}` and the execution's **current** state —
not necessarily terminal.

| Current state | Effect |
| --- | --- |
| `ENQUEUED` / `RETRY_WAITING` | Becomes `CANCELLED` immediately |
| `RUNNING` | The cooperative request is recorded; the owning node observes it within at most one loop interval, and the handler decides when to stop |
| Terminal | No change; the response shows the state that stood |

Cancellation is never immediate and never guaranteed: a completion may win the race, and in that
case it stands.

The actor is resolved **before** the mutation, like on every mutation of this API, and logged with
the request at INFO — the execution keeps its original invoker, so the log line is the audit trail
of who asked.

| Status | Condition |
| --- | --- |
| 202 | |
| 400 | Invalid `X-Mohs-Actor` |
| 404 | Unknown id |
| 422 | Blank id |

### `POST /executions/{id}/retry`

No body. Rearms the **same** `FAILED` execution as `RETRY_WAITING`, due now, bypassing the retry
budget.

**202 Accepted** with `AcceptedExecutionResponse`. The `actor` is the *original* invocation's, since
nothing new is inserted.

Deliberately **no `Idempotency-Key`**: nothing new is inserted, so there is nothing to deduplicate.
The idempotence is the CAS itself — repeating the POST returns 409 naming the current state.

The actor is resolved **before** the mutation and logged at INFO with the request, as on `cancel`.

| Status | Condition |
| --- | --- |
| 202 | |
| 400 | Invalid `X-Mohs-Actor` |
| 404 | Unknown id |
| 409 | Not `FAILED`; belongs to a retired job; **or is a batch member** |
| 422 | Blank id |

---

## Rate limits

### `GET /rate-limits`

Ordered by name — the list is read by people, and a stable order between calls is the minimum for
comparing two snapshots.

```json
[{ "name": "smtp", "max": 100, "window": "PT1M", "available": 37 }]
```

`available` means **tokens available now**, not "used". The refill is applied in memory at read time,
so a bucket that has refilled shows as refilled; the read consumes no token and takes no lock.

### `PATCH /rate-limits/{name}`

```json
{ "max": 20, "window": "PT1M" }
```

Returns `RuntimePatchResponse<RateLimitResponse>`.

| Status | Condition |
| --- | --- |
| 200 | |
| 400 | Invalid actor |
| 404 | The limit was never declared — the body names the property to set. **A `PATCH` never creates a limit**: declaring is an act of boot, not of emergency |
| 422 | `max < 1`; non-positive `window`; a `window` too short for `max` (the refill interval must be representable) |
| 503 | The bucket row is under contention and the 2 s statement timeout expired — **nothing changed**; retry in a few seconds |

The bucket survives the adjustment, its balance clamped to the new ceiling. Lowering the limit cuts
future throughput; it does not give back what was already consumed.

---

## Runners

### `GET /runners`

```json
[{ "name": "io", "mode": "IO", "max": 64, "running": 12 },
 { "name": "cpu", "mode": "CPU", "max": 8, "running": 11 }]
```

**Node-local by nature.** The response describes the process that served the request, not the
cluster. Behind a load balancer, two consecutive calls may legitimately answer with different
numbers — a thread pool is not shared state, and there is no sum across nodes that would mean
anything.

| Field | Meaning |
| --- | --- |
| `max` | `maxConcurrent` for an IO runner, `maxSize` for a CPU runner |
| `running` | Accepted and not finished. In IO mode that is effectively what is executing; **in CPU mode it includes what waits in the queue, so it can exceed `max`** — which is exactly the backlog an operator needs to see. The CPU-mode backlog is `running − max` |

Read-only: a runner is configuration, not adjustable runtime.

---

## Nodes

### `GET /nodes`

```json
[{ "nodeId": "0198e...", "state": "RUNNING", "lastHeartbeatAt": "2026-08-29T14:22:31Z" }]
```

Most recent heartbeat first, ties broken by `nodeId` — an order exposed through an API is contract,
never the table's physical order.

**Death is not a field.** A crash writes nothing; the reader derives alive-versus-suspect from the
age of `lastHeartbeatAt`. `STOPPED` is the only self-reported outcome, and the purge keeps the list
to recent nodes. `state` is one of `CREATED`, `RUNNING`, `PAUSED`, `DRAINING`, `STOPPED`.

Bounded cardinality — the cluster's size plus whatever residue the purge has not yet collected — so
no pagination.

---

## Batches

### `GET /batches/{id}`

```json
{ "batchId": "0198e...", "name": "nightly-invoices", "state": "RUNNING",
  "total": 100, "succeeded": 61, "failed": 2, "pending": 37 }
```

`pending` and `state` are derived from the three counters rather than stored. A seek on the primary
key, flat in the batch's size.
