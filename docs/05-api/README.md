# 5. API

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [api-overview.md](api-overview.md) | The two APIs and their audiences, how to enable REST, design principles, cross-cutting conventions, versioning |
| [endpoints.md](endpoints.md) | Every endpoint: parameters, bodies, status codes, semantics, and the reasoning behind the response shapes |
| [error-model.md](error-model.md) | RFC 7807, the full exception-to-status mapping, and client guidance |
| [java-api.md](java-api.md) | The typed `Mohs` facade, `JobContext`, extension beans, and transaction contracts |
| [programmatic-jobs.md](programmatic-jobs.md) | Register once, submit typed inputs, create independent recurrences, track work and migrate existing consumers |
| [streaming.md](streaming.md) | `GET /overview/stream` — frames, cadence, conflation, the structured snapshot read, shutdown |

## At a glance

```
GET    /overview                        the polling anchor
GET    /overview/stream                 the same snapshot over SSE

GET    /jobs                            every registered job
GET    /jobs/{jobKey}
POST   /jobs/{jobKey}/schedule          202 + Location  [Idempotency-Key]
POST   /jobs/{jobKey}/pause
POST   /jobs/{jobKey}/resume
PATCH  /jobs/{jobKey}/schedule          emergency; reverts on next boot
GET    /jobs/{jobKey}/executions        cursor paginated

GET    /executions                      cursor paginated  [status, jobKey, from, to]
GET    /executions/{id}                 with latest completed attempt
POST   /executions/{id}/cancel          202, cooperative
POST   /executions/{id}/retry           202, bypasses the retry budget

GET    /rate-limits
PATCH  /rate-limits/{name}              emergency; 404 if never declared

GET    /runners                         NODE-LOCAL
GET    /nodes

GET    /batches/{id}                    implemented but NOT WIRED
```

**Prefix**: `mohs.api.base-path`, default `/api/mohs/v1`.
**Enabled**: `mohs.api.enabled=true` — off by default.
**Authentication**: none. The host application's security is the only protection.
