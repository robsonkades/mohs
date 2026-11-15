# Concurrency

There are two authority boundaries: local task lifecycle and committed database transitions. Java synchronization protects the first. It cannot arbitrate another process or guarantee what a stale handler does to an external service.

## Local ownership

| State | Ownership / synchronization |
|---|---|
| Engine lifecycle | Atomic state and lifecycle coordination; start/drain/stop validate budgets before state changes |
| Coordinator cursors, round policy cache and shard rotation | Loop-thread state; bounded pages and persistent continuation across rounds |
| In-flight futures/dispatches | Concurrent collections shared with completion callbacks; identity rechecked before watchdog action |
| Runner capacity | Process-local permits and explicit maximum concurrency |
| Work wakeup | Lock/condition signaling the local loop; remote submissions are discovered by polling |
| Cancellation | Atomic cooperative flag plus thread interrupt; handler must cooperate |
| CompletionBatcher | Bounded pending groups and one flush lifecycle; retry original outcomes after uncertain commit |
| Metrics | Concurrent/atomic counters, advisory observations |

Cancellation and watchdog iterations use weakly consistent continuations, so concurrent additions may be seen in a later pass. Own-session reconciliation pages by claimed_at/execution_id and retains its cursor. It checks grace, local handlers and pending completion before a fenced deferral. There is no second-observation map that grows with all retained grants.

## Database lock protocol

Complete ownership operations lock sessions first, then job keys in canonical order, then active work/history, then batch identities in canonical order. Catalog operations add the leading catalog arbitration lock. This convention applies to all supported writers, including cancellation, retirement, retry and recovery. It reduces cyclic lock acquisition; database deadlock detection and operation retry remain necessary.

| Race | Atomic decision |
|---|---|
| Competing claims | Valid session + locked job headroom + guarded READY update |
| Positive cap across workers | Count authorized LEASED grants while holding the job lock |
| Heartbeat versus recovery | Session lock, fresh database time, irreversible revocation |
| Stale completion versus new grant | Execution/node/token/attempt fence |
| Completion replay | Retained latest token recognizes already-applied result |
| Cancellation versus retry | Cancellation read and fenced work/history transition under the common locks |
| Trigger edit versus firing | Revision/time CAS under the definition lock |
| Fixed-delay old completion | Only matching SCHEDULED revision rearms |
| Bucket adjustment versus charge | Full-state CAS includes configuration, balance, time and remainder |

Candidate row skipping is one part of the claim. It does not remove waits on sessions, job rows, secondary-index maintenance or driver/pool access. SQL Server requires RCSI for committed non-dirty reads while ownership writes use explicit locks.

The coordinator creates a shared TTL/4 monotonic round budget before heartbeat. Stage rotation provides opportunities for later stages after an expensive one. Bounded statements and page sizes limit units of work, not the time a driver can remain blocked. Tune the pool, SQL deadlines and TTL from observed contention; do not infer liveness from thread count alone.

## Verification and limits

Four-database protocol tests use barriers and actual writes before fault injection to cover rollback, lost commit responses, stale owners, cancellation/retry, catalog entry and recovery/renewal races. Joined read-model tests verify committed before/after observations. In-memory tests cover local cursor progress and shared deadlines; they do not certify database lock semantics.

Load measurements report offered/accepted/completed work, producer delay, queue growth, handler latency, JDBC calls and sampled waiters. Zero sampled waiters does not prove zero contention. Multiple engines inside one JVM do not replace process suspend/kill or database failover tests. See [performance evidence](../10-performance/README.md).
