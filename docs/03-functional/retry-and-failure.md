# Retry and failure handling

Status: Active · Last Reviewed: 2026-09-04 · Source of Truth: Repository

## The budget

`retries` is the number of attempts **beyond the first**. Total attempts = `retries + 1`.

**The default is 1, not 0**, and the reason is the delivery guarantee. From `@MohsJob#retries()`:

> The delivery contract is only at-least-once when there is budget: without it, reclaiming an
> execution whose ownership was lost (a dead node, an expired lease, the shutdown window) has
> nowhere to reschedule and becomes a terminal `FAILED` — silently lost work, in exactly the event
> the product promises to survive.

Declaring `retries = 0` is a deliberate opt-in to at-most-once for that job, accepting loss under
node failure.

## The backoff

`RetrySchedule.nextRetryAt(failedAttempt, retries, now)` — one place decides budget *and* backoff,
shared by the dispatcher (an attempt failed) and the reaper (a lease was reclaimed). Two failure
paths with their own copies of the policy would diverge on the first change.

```
delay ~ Uniform[0, min(1s × 2^(attempt−1), 10min)]
```

**Exponential backoff with full jitter**, in the AWS style. Full jitter rather than pure exponential
because the 3 a.m. case is a shared resource going down and taking many executions with it: without
jitter they would all come back in lockstep against a resource that is still recovering.

| Attempt that failed | Upper bound of the delay |
| --- | --- |
| 1 | 1 s |
| 2 | 2 s |
| 3 | 4 s |
| 4 | 8 s |
| … | … |
| 10 | 512 s |
| 11 and beyond | 600 s (the cap) |

The constants are internal, with no configuration property. The exponent is capped at 20 because
`2^20 × 1s` already exceeds the cap and a larger shift would risk overflow.

`retryPolicy` (a bean name) names a `RetryPolicy` bean consulted on both failure paths — a handler
that threw, and a lease reclaimed from a dead node. While it returns a delay it replaces the
`retries` budget; a job naming a bean that does not exist fails the boot rather than falling back.

## Where a retry is written

The retry entry travels **inside** the completion result and lands in the same transaction:

```
DELETE mohs_lease (fenced)
INSERT mohs_attempt (the failure)
INSERT mohs_ready   (attempt + 1, visible_at = retryAt)
```

`LeaseStore.CompletionResult` enforces this structurally: a result is terminal **or** carries a
retry entry, never both and never neither. The reason is stated in its Javadoc — outside the
transaction, a crash between the completion's commit and the queue insert would leave the execution
with no lease, no queue entry and a non-terminal state: an orphan invisible forever.

**The caller never calls `WorkQueue#offer` for a retry.** Doing so would duplicate `mohs_ready`'s
primary key.

## Outcome mapping

`Dispatcher` decides the outcome from the handler's exit *and* the cancellation signal's reason:

| Handler exit | Signal reason | Outcome | Budget consulted? |
| --- | --- | --- | --- |
| Returned normally | none | `SUCCEEDED` | — |
| Returned normally | any | `SUCCEEDED` | — |
| Threw | none | `FAILED` → retry or terminal | Yes |
| Threw | `TIMEOUT` | `FAILED`, wrapped in a `TimeoutException` naming the job's timeout | Yes |
| Threw | `SHUTDOWN` | `FAILED`, cause "node shutdown: drain grace elapsed" | Yes |
| Threw | `MANUAL` | **`CANCELLED`, terminal** | No — a cancel beats the budget |
| Never started, signal already raised `MANUAL` | — | `CANCELLED`, no invocation | No |
| Never started, signal already raised `SHUTDOWN` | — | `FAILED`, no invocation (work not done, so a clean retry elsewhere) | Yes |

**A normal return is `SUCCEEDED` even with the signal raised.** The work finished; recording anything
else would lie and would schedule a duplicate.

## Terminal-by-nature failures

Some failures never consult the budget, because repeating cannot help:

| Failure | Path | `attemptsExhausted` |
| --- | --- | --- |
| Payload could not be deserialised (corrupt JSON, class gone) | `failUnreadablePayload` → `failBeforeDispatch` | `false` — terminal by nature, not by budget |
| Definition removed between claim and dispatch | `failBeforeDispatchGuarded` | `false` |
| Runner name does not resolve | `failBeforeDispatchGuarded` | `false` |

By contrast, **a missing handler goes through the budget on purpose**: during a rolling update
another node running the version that still registers the handler may claim the retry.

The distinction between an unreadable *row* and a failed *query* is deliberate and structural.
`HistoryStore#findPayloads` returns `{rows, unreadable}` — a per-row verdict — while infrastructure
failure propagates as an exception from the call itself, and the whole batch is left to the reaper.
A transient database problem must never become a terminal failure.

## The reaper's decision

When a lease is reclaimed from a dead node (`Engine#decideReclaim`):

| Condition, in order | Outcome | Metric label |
| --- | --- | --- |
| `cancel_requested` already set | `CANCELLED`, terminal, `error = null` | `cancelled` |
| Job retired | `FAILED`, terminal — a rescheduled entry would never be claimed | `job_retired` |
| Budget remains | Synthetic `FAILED` attempt (`node lease expired — node presumed dead`) plus a queue entry at `attempt + 1` | `retry` |
| Budget exhausted | `FAILED`, terminal | `attempts_exhausted` |

The synthetic attempt **consumes budget like any failure**. The metric label separates a
budget-exhausted `FAILED` from a retired-job `FAILED`, because the `Execution` alone cannot tell them
apart — confusing them would send an operator investigating retry budgets during a mass retirement.

Reclaim outcomes publish exactly the same events as the dispatch path: a retry gives `AttemptFailed`
+ `RetryScheduled`; an honoured pending cancel gives `Cancelled`; a terminal gives `Failed` alone.
This is the node-death alerting hook.

## Manual retry

`Mohs.retry(executionId)` / `POST /executions/{id}/retry` rearms the **same** row as
`RETRY_WAITING`, due now, and the new attempt travels the normal claim path.

```sql
UPDATE mohs_execution SET state = 'PENDING', finished_at = NULL
 WHERE execution_id = :id AND state = 'FAILED'
   AND correlation_id IS NULL
   AND EXISTS (SELECT 1 FROM mohs_job_definitions j
                WHERE j.job_key = mohs_execution.job_key AND j.retired = false);

INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
SELECT e.execution_id, e.job_key, e.shard, e.priority,
       (SELECT COUNT(*) + 1 FROM mohs_attempt a WHERE a.execution_id = e.execution_id),
       :now
  FROM mohs_execution e WHERE e.execution_id = :id;
```

| Property | Value |
| --- | --- |
| Bypasses the `retries` budget | **Yes, on purpose** — the policy protects the system from automatic loops; here the decision is the operator's |
| Goes through `Idempotency-Key` deduplication | **No** — nothing new is inserted |
| Idempotence | The CAS itself. Repeating the call finds the execution already rearmed and fails with a state exception (HTTP 409) |
| Attempt number | Derived from history: `COUNT(mohs_attempt) + 1` — the caller carries nothing |
| Priority | The original one, read from the row |

### Refusals, with their reasons

| Precondition violated | Exception / HTTP | Reason |
| --- | --- | --- |
| Unknown id | Empty → 404 | |
| State is not `FAILED` | `IllegalStateException` → 409 | A cancelled execution was an explicit decision; the other states have an owner — the engine |
| The job is retired | `IllegalStateException` → 409 | The rearmed row would never be claimed |
| **It is a batch member** | `IllegalStateException` → 409 | The batch already counted this failure. Counting it again would close the batch early, `BatchCompleted` would stop being terminal, and a second event would no longer find `onCompletion`'s one-shot callback — leaving precisely the person who rescued the member without the notification of the real end. Schedule the job standalone to redo the work |

The CAS is the authority; the read afterwards only distinguishes the *reasons* for defeat. Losing a
race to another mutation between the CAS and the read changes the message, not the outcome.

## Failure visibility

Where the information about a failure actually lives:

| Sink | Content | Retention |
| --- | --- | --- |
| `mohs_attempt.error` | The exception's **message** only, capped at 256 KB (`… [truncated N chars]` past it — a handler echoing a response body into its exception cannot make an attempt row megabytes wide) | With `mohs.engine.history-retention` unset (the default), forever; otherwise until the hourly sweep removes the terminal execution — see [data lifecycle](../06-data/data-lifecycle.md) |
| `mohs_attempt.error_type` | The exception's class name — the number-one operational query | Same as `error` |
| WARN log at the failure | The **full stack trace**. This is the only place a stack trace appears by default | The log's own retention |
| `Failed` / `AttemptFailed` events | The live `Throwable` | Ephemeral, best-effort delivery |
| `mohs.attempt.total{job,outcome}` | Counts | Metric retention |

**A privacy note recorded in `Failed`'s source and worth repeating**: `Execution` deliberately does
not carry the payload, but the handler exception's *message* travels from the event into the log of
anyone writing `log.info("{}", event)`. That discipline is lost through the error-message door, and
whoever writes the handler needs to know it.

## Alerting hooks

| Signal | Meaning |
| --- | --- |
| `Failed` event with `attemptsExhausted = true` | The retry policy ran out. The canonical alert: `case Failed f when f.attemptsExhausted() -> alert(...)` |
| `mohs.lease.reclaimed{reason}` non-zero | A node died or stopped. The label says what the reclaim decided |
| `mohs.tick.failed{step}` non-zero | A tick step is failing persistently — distinguishes "idle because the queue is empty" from "idle because every tick dies" |
| `mohs.claim.requeued{reason}` rising steadily | The inadmissible list is arriving late — typically the queue's head dominated by a stuck job |
| `mohs.attempt.total` divided by `mohs.execution.total` | The attempts-to-executions ratio; the health indicator |
