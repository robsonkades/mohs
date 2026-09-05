# API overview

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

Mohs exposes two APIs, and they have different audiences and different guarantees.

| API | Audience | Stability | Enabled by default |
| --- | --- | --- | --- |
| **Java API** (`io.mohs.core`) | Application developers | The compatibility contract in [java-api.md](java-api.md) | Yes — it is the library |
| **REST API v1** (`io.mohs.rest`) | Operators, and internal services invoking on-demand jobs | Versioned in the path (`/v1`) | **No** — `mohs.api.enabled=false` |

## Enabling the REST API

```yaml
mohs:
  api:
    enabled: true
    base-path: /api/mohs/v1     # the default
```

Turning it on logs a WARN at boot, and the wording is the only guardrail a user reads before
exposing the API:

> `mohs.api.enabled=true: the operational API is served at /api/mohs/v1 with NO authentication. It
> can trigger any registered job with a caller-supplied payload, cancel and retry executions, pause,
> resume and reschedule jobs, and change rate limits. Restrict it to an internal network, or put a
> gateway/mTLS in front of /api/mohs/v1 and /mohs-ui before exposing this instance.`

Two further conditions must hold for the controllers to exist:

- `mohs.enabled` must not be `false` — the master kill switch wins silently, because turning it off
  promises to remove every Mohs bean.
- `spring-boot-starter-webmvc` must be on the classpath. The dependency is `<optional>` in
  `mohs-rest` and in the starter, following the actuator pattern, and
  `@ConditionalOnClass(DispatcherServlet.class)` gates the whole configuration.

## Design principles of the REST surface

| Principle | Consequence |
| --- | --- |
| **A job's definition is code, not API** | There is no `POST`/`PUT`/`DELETE` of a definition. Only reads and invocation over an existing one |
| **A `PATCH` is an emergency lever** | Every `PATCH` response carries a `notice` field warning that the change holds only until the next boot under the default conflict policy |
| **Identities cross as strings** | `jobKey`, `executionId` and `batchId` are plain strings on the wire, never wrapped types |
| **Invocation is asynchronous** | Every invocation returns `202 Accepted` with a receipt and a `Location` header, never a result |
| **Cheap by construction** | The polling anchor's cost is proportional to live work and to the requested window, never to the size of history |
| **Bounded cardinality means no pagination** | Jobs, nodes, runners and rate limits return plain lists. Only the genuinely unbounded listings (executions) are paginated |
| **Errors teach** | Every error is RFC 7807 and its `detail` names the fix |

## Resource areas

Each maps 1:1 to a subpackage of `io.mohs.rest`, which is a navigability rule rather than an accident.

| Area | Base | Operations |
| --- | --- | --- |
| Overview | `/overview` | `GET`, `GET /stream` (SSE) |
| Jobs | `/jobs` | `GET`, `GET /{k}`, `POST /{k}/schedule`, `POST /{k}/pause`, `POST /{k}/resume`, `PATCH /{k}/schedule`, `GET /{k}/executions` |
| Executions | `/executions` | `GET`, `GET /{id}`, `POST /{id}/cancel`, `POST /{id}/retry` |
| Rate limits | `/rate-limits` | `GET`, `PATCH /{name}` |
| Runners | `/runners` | `GET` |
| Nodes | `/nodes` | `GET` |
| Batches | `/batches` | `GET /{id}` |

## Cross-cutting conventions

### Content types

| Direction | Type |
| --- | --- |
| Request bodies | `application/json` |
| Success responses | `application/json` |
| Error responses | `application/problem+json` (RFC 7807) |
| `GET /overview/stream` | `text/event-stream` |

### Headers

| Header | Direction | Meaning |
| --- | --- | --- |
| `X-Mohs-Actor` | Request | Declarative attribution for the audit trail. **Not a credential.** Falls back to `anonymous`. Validated: at most 255 characters, no control or bidi characters, and never `scheduler` |
| `Idempotency-Key` | Request, on `POST /jobs/{k}/schedule` only | Deduplication scoped to `(job, key)`. Validated: at most 255 characters (the column's width), a 422 above it |
| `Location` | Response, on every 202 | The execution's detail URL |

### The `Location` header is derived from the request

Never assembled by concatenating configuration. `mohs.api.base-path` is the path *inside* the
application, and an app with `server.servlet.context-path=/app` serves the execution at
`/app/api/mohs/v1/executions/{id}`.

Concatenating the base path returned a header pointing at a 404 — and on a 202 receipt that is the
worst possible outcome: the client follows the `Location`, does not find the execution it just
scheduled, concludes the write was lost, and resends.

### Pagination

Keyset (cursor) pagination on the two unbounded listings — `GET /executions` and
`GET /jobs/{k}/executions`:

| Parameter | Meaning |
| --- | --- |
| `size` | Page size. Default 50, hard ceiling 200. **Asking for more is not an error — it saturates at the ceiling**; asking for 0 or a negative value saturates at 1 |
| `cursor` | Opaque: the `executionId` of the previous page's last item |
| `nextCursor` in the response | Absent on the last page |

Ordering is **descending by `executionId`**, which is chronological because ids are UUIDv7. The
implementation fetches `size + 1` items so it can reveal "there is a next page" without an extra
round trip, and drops the extra item from the body.

An unbounded page over an unbounded table is a real denial-of-service surface, which is why the
ceiling is decided at contract time rather than left to configuration.

### Listings that are **not** paginated

`GET /jobs`, `GET /nodes`, `GET /runners`, `GET /rate-limits`. The criterion is cardinality:
these are bounded by what the application **declared**, not by what it **accumulated** while
running.

### Summary versus detail

`GET /executions` and `GET /jobs/{k}/executions` return `ExecutionSummaryResponse` — **without
`attempts`**. The detail view (`GET /executions/{id}`) returns `ExecutionResponse` with them.

What the list stops carrying is not only volume: an attempt's `error` is arbitrarily long text that
used to travel on every page and on every tick of the dashboard's stream.

### Durations and instants

| Type | Wire format |
| --- | --- |
| `Instant` | ISO-8601 (`2026-08-29T14:22:31.482Z`) |
| `Duration` in a response | ISO-8601 (`PT1M`) |
| `Duration` in a request body | ISO-8601 |
| `?window=` on `/overview` | **Either** form — `15m` or `PT15M` |

The `?window=` parameter is deliberately parsed as a `String` with an explicit
`DurationStyle.detectAndParse`, **never** the host's binder: Mohs is an embedded library, and
depending on the host application's MVC `ConversionService` would make the accepted format vary per
host. An unparseable value becomes a 422 that teaches, not a 500.

## Versioning

| Aspect | Approach |
| --- | --- |
| Version location | In the path: `/api/mohs/v1` |
| Configurable | Yes, via `mohs.api.base-path` — but see the caveat below |
| Media-type versioning | Not used |
| Deprecation headers | Not present |
| OpenAPI document | **Not present** — no springdoc dependency |

**The base-path caveat**: the bundled dashboard pins `/api/mohs/v1` in its compiled JavaScript. If
you change `mohs.api.base-path` and also ship `mohs-ui`, every dashboard call 404s.
`MohsUiAutoConfiguration` logs a WARN naming both values and telling you to serve the API at the
default prefix or proxy to it.

## What the API is not

| Not | Detail |
| --- | --- |
| Authenticated | No authentication, no authorization, no roles. See [security](../08-security/security-overview.md) |
| Rate limited | The only limit is the 64-subscriber cap on the SSE stream |
| Discoverable (HATEOAS) | Only the `Location` header on 202 responses |
| A definition management API | Definitions are code |
| A result-fetching API | Invocation returns a receipt; the result is the execution's state, polled |
