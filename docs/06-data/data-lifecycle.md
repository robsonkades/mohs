# Data lifecycle

Status: Active · Last reviewed: 2026-09-10.

| Data | Growth and removal |
|---|---|
| Active work | One `mohs_work` row per active execution; terminal completion or queued cancellation removes it. Backlog is not intrinsically bounded. |
| Execution history | One immutable input snapshot and only the latest completed attempt per execution. Terminal history is removed only when retention is configured. |
| Idempotency receipts | Independent fixed expiry set at acceptance; zero configured retention creates indefinite receipts. |
| Sessions | Revoked old sessions without grants are purged; live sessions and sessions still referenced by work remain. |
| Definitions/catalog/resources | Retained operational configuration; retirement preserves executable identity and input contract. |
| Batches | Counters remain with member history. An empty retained batch can be collected with history retention. |

## Work and history

READY/LEASED transitions update one active row. Losing resource admission preserves the attempt number; a completed retryable failure consumes an attempt and schedules the next one. Completion atomically updates the latest result, work, batch counters and eligible fixed-delay trigger.

`mohs.engine.history-retention` is disabled by default. When positive, the coordinator attempts an hourly sweep of terminal executions older than the cutoff and batches with no remaining members. Each pass is bounded to 1,000 candidates per category, with statement deadlines, a two-second sweep sub-budget and the shared round deadline. A deferred or unsuccessful sweep leaves rows for later passes; retention is an eligibility window, not an exact deletion time.

The sweep uses finished-time predicates and excludes active work. It does not derive completion age from the timestamp inside a UUID. Removing a FAILED execution also removes the ability to manually retry that execution. Idempotency receipts remain independent and may still return an ID whose history has been pruned.

History without retention grows with total accepted executions. The latest-attempt format limits diagnostics per execution, not the number or size of snapshots. Work indexes exclude retained terminal history, but cache, storage and database load are shared.

## Idempotency

The original receipt stores execution ID, normalized scheduled time, actor, acceptance time and `expires_at`. Database time decides expiry. At equality with `expires_at`, the key is expired even if the cleanup has not deleted it. A fresh acceptance replaces an expired receipt and creates its execution in the same transaction.

The default lifetime for new receipts is seven days. Zero creates receipts without expiry; changing configuration does not rewrite existing expiry times. Cleanup starts hourly when caught up, removing pages of at most 1,000 expired receipts within the shared tick budget and a two-second pass budget. A full last page or failed pass schedules continuation after one second; exhausted idle cleanup returns to the hourly cadence. Cleanup frequency never defines deduplication correctness.

Receipt retention does not guarantee uniqueness of a handler's external effects. Jobs can run again after lost confirmation, revoked sessions or retries. Business idempotency belongs at the effect boundary.

## Session cleanup

Session expiry and revocation are distinct. A peer must atomically revoke an expired session before reclaiming its grants. A stale membership snapshot cannot revoke a session that renewed first. Cleanup removes only sufficiently old revoked sessions with no outstanding grants, using a cutoff derived from the last confirmed database heartbeat and ten session TTLs.

A stopped/revoked identity cannot renew. New process membership uses a new UUIDv7; there is no epoch compatibility fallback or nullable-expiry legacy path.

## Coordinator budget

One monotonic node-TTL/4 budget covers a whole round. The next round resumes from the stage after the one that exhausted the budget. Queue metrics, cleanup and cancellation observations may therefore be delayed under load. Failed maintenance retains its previous sample or persisted state and records a failure metric.

Statement and transaction deadlines bound individual database operations. A pool wait or JDBC call already in progress is not forcibly completed by the round checkpoint. Membership fencing protects database transitions if the process loses its session while blocked; it cannot undo an external effect.

## Input format

`InputCodec.JSON` owns the strict JSON format used for stored inputs and HTTP conversion. Unknown properties are rejected. Snapshots identify a declared input contract rather than a Java class name; workers decode using their local registered type.

An input update changes future recurring occurrences. Existing executions and retries retain their original JSON. A compatible class rename can preserve the same input contract. An incompatible format requires a different job identity; retirement/reactivation does not permit changing the retained contract. There is no tolerant legacy reader or dual format path.

See [data model](data-model.md), [type contract](type-contract.md) and [transactions](../04-engineering/transactions.md).
