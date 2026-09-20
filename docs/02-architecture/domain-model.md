# Domain model and invariants

Status: Active · Last Reviewed: 2026-09-10 · Source of Truth: Repository

The public API describes executable code, execution policy and typed input. The engine turns those values into complete persistence operations. JDBC owns transactions and dialect-specific locking; application handlers own their external effects.

## Identities and declarations

| Value | Meaning |
| --- | --- |
| `JobKey` | Stable technical identity, exact ASCII comparison on every database. A retained identity keeps its executable binding and input contract. |
| `JobRef<T>` | Job key, local Java input type and explicit input-contract identifier. It does not serialize the handler or the class name. |
| `JobRegistration<T>` | Local executable declaration with its policy and typed action. Annotations normalize into the same catalog model. |
| `JobDefinition` | Immutable schedule and execution policy: runner, window, shared rate name, retry budget/policy, timeout and concurrency ceiling. |
| `StoredJob` | Definition plus pause state, next trigger instant and schedule revision. Retirement is durable; ordinary publication does not reactivate it. |
| `JobBinding` | Direct executable owner and optional input for automatic occurrences. Executables bind to themselves; recurrences use one direct hop. |
| `ExecutableCatalog` | Homogeneous set of executable keys and contracts. Durable publication and session join serialize on the catalog authority. |
| `ExecutionId`, session ID, claim token, batch ID | Canonical lowercase UUIDv7 values with distinct roles. A grant token changes on every acquisition. |

Zero `maxConcurrentExecutions` means unlimited. Positive values reserve durable acquisition capacity under the job lock. There is no separate concurrency-enabled boolean. A named recurrence has its own job identity and policy even when it invokes shared code.

`origin`, `schedule_revision` and `batch_id` are explicit persisted facts. An actor is an audit label, never a control-flow sentinel. Generated IDs have 36 characters; technical keys have at most 255 exact ASCII characters. Display/audit strings have their own nonblank/length constraints. See [type contracts](../06-data/type-contract.md).

## An accepted execution

Acceptance stores an immutable input snapshot, identity, intended eligibility, audit actor, provenance and optional batch membership. An optional idempotency receipt is written in the same transaction. The store validates the definition and input contract while arbitrating against retirement.

| Logical phase | Durable result | Active work | Latest attempt |
| --- | --- | --- | --- |
| Queued, delayed or awaiting retry | `PENDING` | Ready at a canonical eligibility instant | Absent before any attempt; otherwise the last completed result |
| Acquired | `PENDING` | One session/token/attempt grant | The previous completed result, if any |
| Succeeded or failed terminally | `SUCCEEDED` or `FAILED` | None | The terminal attempt |
| Cancelled before acquisition | `CANCELLED` | None | No fabricated handler attempt |
| Cancelled after an attempt | `CANCELLED` | None | The completed attempt when one was produced |

`ENQUEUED` and `RUNNING` are derived read states; delayed and retry work use the same eligibility rule. An API listing may omit latest-attempt details deliberately. It does not mean the result was erased.

A normal handler failure consumes an attempt. Admission deferral does not: the same attempt can be acquired again with a different token. Automatic retries keep execution identity and snapshot. Manual retry is a guarded operation on a failed, standalone manual execution whose job remains active; it continues the attempt numbering.

## Complete transitions

- Submission creates execution, active work and any receipt/batch membership atomically.
- Firing compares schedule revision, observed trigger and active/unpaused state, reads automatic input under lock and creates all selected occurrences in that transaction.
- Claim validates its session, locks jobs canonically, reserves capacity and creates grants atomically.
- Completion applies only to the current grant. Latest result, retry/terminal work transition, batch counting and fixed-delay rearm commit together.
- Cancellation and retirement arbitrate against those same transitions. An already produced success may complete; cancellation is not rollback of an external effect.
- Recovery operates only on revoked sessions. It applies a synthetic result through the same retry, cancellation, batch and rearm rules.

A fixed-delay occurrence rearms only its matching schedule revision. Completing an older series cannot overwrite a newer schedule. Manual work never drives the automatic fixed-delay chain.

## Receipts, batches and retention

An idempotency receipt is scoped by job key and idempotency key. Its expiry is fixed using database acceptance time and remains independent of history retention. Zero configured retention creates indefinite new receipts. Reusing a still-valid receipt returns its original accepted identity; it does not deduplicate effects inside the job.

A batch has a durable total, succeeded and failed count. Pending is derived. A terminal member contributes once in the same transaction as its result; retry does not count. The transaction that reaches zero pending can publish a best-effort event. Durable continuation callbacks and per-batch callback registries are not part of the API.

History retains the execution and only the latest completed attempt. Earlier attempt details are intentionally overwritten. Pending work is never history-pruned. Receipt and batch retention have independent invariants; deleting history must not reopen a live idempotency key.

The database decides persisted transitions. Local futures, cancellation signals, metrics, listeners, shard assignment and dashboard counters are derived observations. None is an alternative ownership authority. See [execution lifecycle](execution-lifecycle.md), [clustering](clustering-and-liveness.md) and [transactions](../04-engineering/transactions.md).
