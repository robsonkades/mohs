# Resilience

Durable correctness rests on complete database operations, live-session checks and execution/node/token/attempt fencing. A timeout is an unknown outcome until the corresponding receipt/result/state resolves it. An exception alone does not prove rollback.

| Boundary | Mechanism | Practical limit |
|---|---|---|
| Catalog / session admission | Singleton catalog plus ordered session locks | Incompatible live executables refuse publication/entry |
| Process pauses and connection loss | Database expiry, irreversible revocation before recovery | A stale handler can still act on external systems |
| Two claimers | Session/job locks, positive headroom and guarded work update | Database waits/deadlocks remain possible |
| Recovery versus heartbeat | Same session lock and fresh database observation | Valid renewal wins; expired session cannot renew |
| Completion versus cancel/retry | Same job/work protocol, conditional fence and retained latest token | Success may win before cancellation; cancellation prevents a later retry |
| Overloaded completion writer | Bounded groups with jittered operation retry | Finite buffers and loss of local outcomes on process death |
| Coordinator overload | One rotating TTL/4 round budget, bounded pages and statement deadlines | A driver/pool call already in progress can exceed the checkpoint |
| Retry load | Finite attempt budget and backoff | Does not guarantee eventual success; retries add load |
| External effects | User idempotency / destination fencing / suitable application transaction | Scheduler deduplication alone cannot protect this boundary |

The tick retains cancellation/timeout processing and rotates maintenance rather than spending separate full budgets on every stage. Recovery processes up to 500 grants in chunks of 50. Own-session grants are read in pages of 128; cancellation/watchdog iterations continue across passes of at most 1,000 entries. Cleanup uses bounded statements and independent retention contracts.

There is no nullable-expiry compatibility fallback, epoch-bump rejoin, missing-handler rollout mechanism or legacy rate arithmetic path. The supported SQL Server mode requires committed row-versioned reads.

See [current failure sequences](../02-architecture/clustering-and-liveness.md) and [testing strategy](../11-testing/testing-strategy.md). Historical chaos measurements describe their recorded binaries and do not recertify the present runtime. Process-level kill/suspend is distinct from multiple Engine objects inside one JVM.

## Historical process-suspension limitation of the `SUSPEND` scenario

Recorded in the script: if the freeze catches the node **mid-claim-transaction**, the batch's rows
stay locked-but-uncommitted — invisible to the reaper (not `RUNNING`) and skipped by other claims
(`SKIP LOCKED`) until the frozen session ends. A run reporting "reclaimed: 0" with a full drain
right after resume is *that* mode, not a fence failure. It is a pre-existing property, and the
database-side mitigation is `idle_in_transaction_session_timeout`.

## What is deliberately absent

| Mechanism | Why not |
| --- | --- |
| **Circuit breaker** over the database | There is one downstream, and the adaptive backoff plus per-step isolation already covers degradation. Opening a breaker would stop the heartbeat — which is exactly what must not stop, since a stopped heartbeat means peers reclaim work that is still running here |
| **Dead-letter queue** | Exhausted retries become terminal `FAILED` rows in history, queryable and manually retryable. A separate DLQ table would be a second place to look |
| **Priority aging** | `BACKGROUND` can starve under sustained higher-priority load. Documented as a known risk in `Priority`'s Javadoc rather than silently mitigated |
| **Cross-node `onCompletion` delivery** | Explicitly out of contract; the outbox pattern is prescribed instead |
| **Load shedding at the API** | The REST layer has no rate limiting of its own beyond the SSE subscriber cap |
