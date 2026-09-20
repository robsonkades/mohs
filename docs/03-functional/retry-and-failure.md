# Retry and failure

Retries are additional attempts beyond the first. A finite budget does not guarantee that a handler will eventually run or succeed: repeated crashes can consume it. Zero disables automatic retry; explicit later submissions and manual retry are separate user actions.

The built-in retry schedule and named RetryPolicy determine whether a failed attempt is terminal and, if not, its next eligible instant. The persisted attempt number belongs to active work. Only the latest completed attempt result remains in execution history, including while the next attempt waits or runs.

## Atomic completion

The database transaction validates the session and execution/node/token/attempt fence. It then writes the latest result and either returns the same work row to READY with the next attempt or removes it on terminal completion. Terminal history, batch counters and a matching scheduled fixed-delay rearm commit together. A failed attempt that retries does not close its batch member.

Completion replay after an uncertain commit returns ALREADY_APPLIED when the retained result matches its token. A stale grant cannot write a different generation. Connection loss does not prove rollback, so completion groups retry the whole operation with their original tokens. Buffers and retry attempts are bounded; revocation/recovery resolves grants whose result cannot be delivered.

## Failure and cancellation races

| Sequence | Result |
|---|---|
| Cancellation flag commits before retry/deferral | That transition closes CANCELLED rather than queueing another attempt |
| Success commits before cancellation | Success remains terminal |
| Session expires, peer revokes it, paused worker resumes | Renewal and completion from that session lose authority |
| Heartbeat renews first while still valid | Revocation rechecks under lock and does not recover a live session |
| Handler effect succeeds, process dies before completion | Recovery may repeat the effect; use application idempotency |

Recovery uses the same complete transition rules after irreversible session revocation. It reads bounded grant/policy pages and retains the retry limit. An own-session stray grant is requeued only after a grace period and absence from local handler/pending-completion tracking, with the same fence deciding a race.

Manual retry requires a retained FAILED execution and an active definition. It atomically creates READY work, preserves its input snapshot/latest result, clears terminal execution status and becomes MANUAL. It does not rearm the former recurring series or count the old batch a second time. Other terminal outcomes are not manual-retry candidates.

See [execution lifecycle](../02-architecture/execution-lifecycle.md), [cancellation](cancellation-and-timeouts.md) and [distributed guarantees](../02-architecture/clustering-and-liveness.md).
