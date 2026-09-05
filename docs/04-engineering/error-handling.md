# Error handling

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

## Exception taxonomy

Mohs defines **no exception hierarchy of its own** in the public API. It uses the JDK's unchecked
exceptions with messages that teach, and defines a small set of REST-boundary exceptions that exist
solely to be translated into HTTP status codes.

| Layer | Exceptions used | Checked? |
| --- | --- | --- |
| Public API (`io.mohs.core`) | `IllegalArgumentException` (bad input), `IllegalStateException` (wrong state), `NullPointerException` (contract violation) | Unchecked |
| Engine | The same, plus `NoSuchElementException` from `RunnerRegistry#resolve` | Unchecked |
| Store | Spring's `DataAccessException` hierarchy (`DuplicateKeyException`, `QueryTimeoutException`, `PessimisticLockingFailureException`) | Unchecked |
| Handler contract | `JobHandler#invoke(...) throws Exception` — a handler may throw **anything** | Checked allowed |
| REST | Seven domain exceptions in `io.mohs.rest.error` | Unchecked |

The handler being allowed to throw checked exceptions is deliberate: `MohsJobs#adaptHandler` unwraps
`InvocationTargetException` and rethrows the original, so `Attempt.error` records the real message
rather than the string `"InvocationTargetException"`.

## The five error classes, and how each is treated

This is the operative taxonomy — not by exception type, but by what the system must do:

| Class | Example | Treatment | Retryable |
| --- | --- | --- | --- |
| **Caller error** | Blank job id, negative delay, `at` and `delay` together, a reserved actor | Fail fast at the boundary with a message naming the field | No |
| **Configuration error** | Missing dialect, duplicate job id, unsupported handler signature, invalid `@OnExecution` declaration | **Fail the boot** | No |
| **Transient infrastructure** | The payload query threw, the tick step threw, the rate-limit lock timed out | Log, back off, leave ownership intact; a reaper resolves it if this node dies | Yes, implicitly |
| **Terminal-by-nature** | Unreadable payload, removed definition, unresolvable runner | Terminal `FAILED` with `attemptsExhausted = false` | No |
| **Handler failure** | Whatever the job threw | Retry through the budget, or terminal | Per the budget |

The distinction between *transient infrastructure* and *terminal-by-nature* is structural, not a
judgement call at the catch site. `HistoryStore#findPayloads` returns `PayloadBatch {rows,
unreadable}`: a per-row deserialisation failure lands in `unreadable`, while an infrastructure
failure **propagates as an exception from the call itself**. One shape cannot be mistaken for the
other.

## Failure isolation, by granularity

The governing principle, stated in `Engine#runMaintenance`:

> The granularity of error handling is the granularity of degradation. A `try` covering independent
> steps turns any one of them into a single point of failure for the whole set.

| Scope | Failure isolation |
| --- | --- |
| One tick step (`signalJobTimeouts`, `pollCancelRequests`, `signalWatchdogOverruns`, `reapOrphanedLeases`, `reconcileOwnStrayLeases`, `purgeStaleNodeRows`) | Isolated individually. A failure logs a WARN, increments `mohs.tick.failed{step}`, and **claim and dispatch continue this tick** |
| The heartbeat | **Deliberately not isolated** — with no heartbeat the node is already dead to its peers, so continuing the tick would help nobody |
| One due trigger | Isolated. One job's failure does not take down the sweep of the others, nor the tick's claim |
| One claimed execution's dispatch | Isolated. Without it, a single executor rejection mid-batch would abort the loop and leave the following, already-owned executions orphaned |
| One listener | Isolated. A throwing listener never affects the job, nor the other listeners |
| One `onCompletion` callback | Isolated. The batch is complete regardless, and the remaining callbacks still run |
| One result in a group-commit flush | A batch failure falls back to individual completion; an individual failure leaves the execution for the reaper and never kills the flusher |
| One SSE subscriber | A failing send removes that subscriber without taking the others down |

## Severity mapping

The rule is that severity reflects **operational meaning**, not the code path's excitement:

| Level | Used for | Example |
| --- | --- | --- |
| `ERROR` | The system is degraded in a way that needs a human | "engine loop died — this node stops claiming… Restart the instance." |
| `WARN` | Something abnormal happened, recovery is automatic, but the operator should know | Misfire applied; drain grace elapsed; lease reclaimed; clock moved backwards; result discarded by the fence |
| `INFO` | A notable, *correct* state change | Draining started; engine stopped in X; job definition changed with a diff; execution manually rearmed |
| `DEBUG` | Routine detail | Trigger fired; stray-lease candidates vanished before the requeue |

Two deliberate placements worth noting:

- **An honoured cancellation is `INFO`, not `WARN`, and carries no stack trace.** It is the system
  doing what the operator asked, not a failure.
- **A fence loss during the stray-lease reconcile is `DEBUG`.** At high throughput it is routine —
  completions won the race — not a finding.

## The REST error model

Every error crosses the wire as RFC 7807 `application/problem+json`. See
[error model](../05-api/error-model.md) for the full mapping table. The engineering points:

| Decision | Reason |
| --- | --- |
| `@RestControllerAdvice(basePackages = "io.mohs.rest")`, **never global** | An unscoped advice applies to every controller in the context, so enabling the API would start deciding the **host application's** error handling — one of its `@ResponseStatus(NOT_FOUND)` methods would become a 500 because of the `@ExceptionHandler(Exception.class)` here, without a line of the app changing |
| `@Order(HIGHEST_PRECEDENCE)` | On Mohs' endpoints the house advice must beat a generic advice from the host. Without it, a tie between two `ResponseEntityExceptionHandler`s at `LOWEST_PRECEDENCE` falls to bean registration order |
| Extends `ResponseEntityExceptionHandler` | Inherits framework-error translation (malformed JSON, missing parameter) for free |
| `handleHttpMessageNotReadable` is overridden | The base implementation replaces the message with a fixed "Failed to read request", losing the well-written validation the request records already perform in their compact constructors. Only a Jackson-identified Mohs REST constructor validation returns its public message with 422. Other deserialization failures use a generic response; the cause walk is bounded at 64 |
| The catch-all logs the cause and returns a generic body | `"An unexpected error occurred"` — never `ex.getMessage()`, so internal detail does not leak to an untrusted caller |
| `type` stays at `about:blank` | The `mohs.io` domain has no confirmed URI registry, and inventing one would be worse than omitting it |

### Errors that teach

Every domain exception's `detail` names the fix. Two examples:

> `Rate limit 'smtp' not found — declare it with mohs.rate-limits.smtp.max/.window (or a @Bean
> RateLimit) and restart; PATCH only adjusts what boot declared`

> `Job 'send-invoce' not found` — plus a `nearbyJobKeys` property computed with Levenshtein
> distance ≤ 2 over the registered jobs.

## Contention as a first-class outcome

`QueryTimeoutException` and `PessimisticLockingFailureException` map to **503 Service Unavailable**,
not 500:

> Under contention the `PATCH` is precisely the emergency lever the operator is pulling, and
> "unexpected error" would leave them unsure whether it applied — retrying on top of an already
> saturated row. Transient by definition: the same request repeated later tends to succeed.

The body says explicitly *"A database row is under contention and nothing changed — retry in a few
seconds"*. A 4xx/5xx must be honest about whether it mutated anything.

Both types are caught because Spring's translator sends statement timeout and deadlock to **sibling**
branches of the hierarchy — a measured detail, not a guess.

## Errors that must fail the boot

Silent acceptance would mean silent misbehaviour:

| Condition | Message names |
| --- | --- |
| `mohs.jdbc.dialect` unset | The four valid values |
| Duplicate job id | Both declaring methods |
| More than one job annotation on one method | The method |
| Blank id on a stereotype | The concise form to use |
| Unsupported handler signature | The method, the reason, and the count found |
| `@RecurringJob` with no trigger | The three attributes to set, or `@OnDemandJob` |
| Invalid `@OnExecution` signature or filter | The method and incompatible event selection |
| Missing named `RetryPolicy` bean | The affected job and bean name |
| Hikari acquisition timeout at least the node lease | Lower `spring.datasource.hikari.connection-timeout` or adjust the lease |
| `@MohsJob` colliding with a `PROGRAMMATIC` definition | The id and the method |
| Definitional drift under `on-conflict=fail` | The diff |
| Duplicate runner or window name | Both sources |
| A runner field belonging to the wrong mode | The property and the mode |
| `watchdog-timeout <= node-lease-ttl` | Both values and why the ordering matters |
| Any `EngineSettings` violation | The property name, the value, and the meaning of the constraint |

## Failure-path guard rails

Two runtime assertions that convert a silent corruption into a loud failure:

```java
static {
    if (!CLAIM_READY_FILTERED.contains(":inadmissible")) {
        throw new ExceptionInInitializerError(
            "CLAIM_READY_FILTERED lost its :inadmissible predicate — the replace anchor drifted");
    }
}
```

```java
if (deleted[i] == Statement.SUCCESS_NO_INFO) {
    throw new IllegalStateException("driver returned SUCCESS_NO_INFO for the fenced lease delete batch — "
        + "completion cannot tell fence winners apart; implement the per-row path for this driver");
}
```

The second is worth studying as a model: it refuses to guess. A presence-based fallback would be
ambiguous — an absent row may be *our* win or somebody else's already-completed re-claim — so the
code fails loudly and names the work required.

## Correlation

| Available today | Not available |
| --- | --- |
| `ExecutionId` in every log line about an execution | A cross-cutting request/correlation id propagated by the framework |
| `JobKey` in claim, dispatch and completion logs | MDC populated automatically |
| `nodeId` in `mohs_attempt.node_id` — forensic "who executed this attempt" | Trace/span ids |
| `actor` on every execution | |

The intended extension point is `ExecutionInterceptor`, which runs on the attempt's own thread and
is documented as "the place for MDC, tracing spans and context through `ScopedValue`". See
[extensibility](extensibility.md).
