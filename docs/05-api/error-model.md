# Error model (RFC 7807)

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository (`io.mohs.rest.error`)

Every error the Mohs REST API returns is `application/problem+json`, per RFC 7807.

## The envelope

```json
{
  "type": "about:blank",
  "title": "Job not found",
  "status": 404,
  "detail": "The requested job was not found",
  "instance": "/api/mohs/v1/jobs/send-invoce",
  "nearbyJobKeys": ["send-invoice"]
}
```

| Field | Notes |
| --- | --- |
| `type` | Always `about:blank`. The `mohs.io` domain has no confirmed URI registry yet, and inventing one would be worse than omitting it |
| `title` | A short, stable type label |
| `status` | The HTTP status |
| `detail` | **The actionable sentence.** This is the field a client should surface to a human |
| `instance` | Supplied by Spring |
| *extensions* | Two exist: `nearbyJobKeys` (404 on a job) and `field` (422 from a typed validation) |

## The mapping table

| Exception | Status | `title` | `detail` |
| --- | --- | --- | --- |
| `JobNotFoundException` | 404 | Job not found | Generic not-found message; `nearbyJobKeys` is included when suggestions exist |
| `ExecutionNotFoundException` | 404 | Execution not found | Generic not-found message |
| `BatchNotFoundException` | 404 | Batch not found | Generic not-found message |
| `RateLimitNotFoundException` | 404 | Rate limit not found | Names the property to declare: `mohs.rate-limits.<name>.max/.window` (or a `@Bean RateLimit`), and says that `PATCH` only adjusts what boot declared |
| `InvalidActorException` | 400 | Invalid actor | Which rule the `X-Mohs-Actor` header broke |
| `ExecutionNotRetryableException` | 409 | Execution not retryable | The current state, or the batch-membership explanation |
| `PayloadValidationException` | 422 | Request validation failed | The message; plus a `field` extension |
| `IllegalArgumentException` from a Mohs REST request constructor, identified by Jackson | 422 | Request validation failed | The original message from the record's compact constructor |
| `QueryTimeoutException`, `PessimisticLockingFailureException` | **503** | Resource busy | "A database row is under contention and nothing changed — retry in a few seconds" |
| `UnsupportedOperationException` | 501 | Not implemented | "This operation is part of the v1 contract but is not implemented yet" |
| Anything else | 500 | *(Spring's default)* | "An unexpected error occurred" — **never** the exception's message |
| Malformed JSON or an unrecognized deserialization failure | 400 | *(Spring's default)* | Spring's default |
| A missing or untypeable request parameter | 400 | *(Spring's default)* | Inherited from `ResponseEntityExceptionHandler` |

## Status-code choices worth explaining

### 503 for contention, not 500

The only row the API contends for with the engine's hot path is the rate-limit bucket, whose lock
has a 2 s ceiling.

> Under contention the `PATCH` is precisely the emergency lever the operator is pulling, and
> "unexpected error" would leave them unsure whether it applied — retrying on top of an already
> saturated row.

The body states explicitly that **nothing changed**. Both exception types are caught because
Spring's translator sends statement timeout and deadlock to *sibling* branches of the hierarchy —
measured, not guessed: H2 SQLState 50200 at 2,013 ms, PostgreSQL 18 57014 at 2,022 ms.

### 409 for a retry that cannot proceed

`POST /executions/{id}/retry` is a state-machine transition. A duplicate POST, a non-`FAILED`
execution, a retired job and a batch member all produce 409 with a `detail` naming which of them it
was. The batch-member message is the longest in the codebase, and deliberately so:

> execution … is a member of batch … — a batch member is not retried individually, because the batch
> already counted this failure and counting it again would close the batch early. Schedule the job
> standalone to redo the work.

### 422 for domain validation, 400 for protocol errors

| Situation | Status |
| --- | --- |
| The JSON does not parse | 400 |
| A required parameter is missing | 400 |
| The `X-Mohs-Actor` header breaks a rule | 400 |
| The JSON parses but a record's invariant rejects it (`at` **and** `delay`; negative `delay`; `max < 1`) | **422** |
| The payload does not fit the handler's parameter type | **422** |
| The `window` parameter is not a duration | **422** |
| The schedule is unrealisable | **422** |

### 404 that helps

`JobNotFoundException` carries `nearbyJobKeys`: registered keys within Levenshtein distance 2 of
what was asked for. Typing `send-invoce` gets you `["send-invoice"]` instead of a mute "Not Found".

## Two implementation decisions that matter to a host application

### The advice is scoped, never global

```java
@RestControllerAdvice(basePackages = "io.mohs.rest")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RestExceptionHandler extends ResponseEntityExceptionHandler { … }
```

A `@RestControllerAdvice` without `basePackages` applies to **every** controller in the context. So
turning on `mohs.api.enabled` would start deciding the host application's error handling — one of
its `@ResponseStatus(NOT_FOUND)` methods would become a 500 because of the
`@ExceptionHandler(Exception.class)` here, without a line of the app changing.

`HIGHEST_PRECEDENCE` is needed because on Mohs' endpoints the house advice must beat a generic advice
from the host; without it, a tie between two `ResponseEntityExceptionHandler`s at
`LOWEST_PRECEDENCE` falls to bean registration order.

### `handleHttpMessageNotReadable` is overridden

The base implementation replaces the message with a fixed `"Failed to read request"`, losing the
well-written validation the request records already perform in their compact constructors — thrown
*during* Jackson's deserialisation.

The override inspects at most 64 causes. It returns 422 only when Jackson identifies
a constructor in `io.mohs.rest.*` whose direct cause is `IllegalArgumentException`.
That constructor's validation message is public; there is no `field` extension on this path.
Other conversion failures, malformed JSON, cycles and deeper unrecognized chains retain
the generic 400 response. Payload conversion to an application's handler type produces
a 422 with a controlled message, without exposing Jackson internals or Java class names.

## Client guidance

| Status | Retry? | How |
| --- | --- | --- |
| 400, 404, 409, 422 | **No** | Fix the request. 409 on a retry usually means the operation already succeeded |
| 501 | No | The operation is not implemented in this version |
| 503 | **Yes** | The request changed nothing. Back off a few seconds |
| 500 | Maybe | Check the server log — the real cause is there, never in the body |
| A network failure on a `POST .../schedule` | **Yes, safely** — if you sent an `Idempotency-Key` | The repeat returns the original receipt with no duplication |

The bundled dashboard's client prefers `detail`, then `title`, then `statusText` — which is exactly
what puts the near-miss job name on screen for an unknown-job 404 rather than a mute "Not Found".
