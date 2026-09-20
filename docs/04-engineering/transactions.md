# Transaction management

Status: Active · Last Reviewed: 2026-09-10 · Source of Truth: Repository

Mohs uses explicit JDBC transactions over the configured DataSource. Storage ports expose complete operations: callers do not compose history, queue and batch writes. `JdbcSubmissionStore` implements both application acceptance and scheduled firing; its SQL writers are private. `HistoryStore` and `BatchStore` expose queries and retention, not lifecycle write fragments.

## Transaction boundaries

| Operation | Propagation and isolation | Atomic decision and writes |
| --- | --- | --- |
| Application submission or batch | NESTED; standalone READ COMMITTED, inherited isolation inside a host transaction; 3 s when standalone | Lock distinct jobs in key order; validate active input contracts; write optional batch, receipts, execution snapshots and ready work |
| Recurring firing | REQUIRES_NEW, READ COMMITTED, 3 s | CAS on job identity, schedule revision, observed next fire, active and unpaused state; read committed recurring input under that lock; advance trigger and create all occurrences |
| Acquisition | REQUIRES_NEW, READ COMMITTED, 3 s | Lock session, validate database-time expiry, lock candidate jobs, reserve within each global grant cap, recheck session validity, transfer ready rows to grants with fresh tokens |
| Completion, recovery or release | REQUIRES_NEW, READ COMMITTED, 3 s | Validate the appropriate live or revoked session, match grant token, update latest attempt and terminal state or retry work; count batch and rearm matching fixed-delay revision |
| Cancellation | REQUIRES_NEW, READ COMMITTED, 3 s | Cancel queued work and count/rearm atomically, or persist cancellation intent on the acquired grant |
| Manual retry | REQUIRES_NEW, READ COMMITTED, 3 s | Reopen an eligible standalone MANUAL failed execution for its next attempt, under its active job lock |
| Session register/renew/revoke/stop | REQUIRES_NEW, READ COMMITTED, 3 s | Database time and guarded session row decide validity; revoked or expired identities cannot renew |
| Catalog publication | REQUIRES_NEW, READ COMMITTED, 30 s | Singleton catalog mutex, existing session locks, all executable definitions and required input contracts commit together |
| Definition/binding publication and explicit reactivation | NESTED, standalone READ COMMITTED, 3 s | Catalog gate and ordered definition locks protect retained identities, input contracts and schedule revisions |
| Definition retirement | REQUIRED, READ COMMITTED required also in a host; 30 s standalone | Lock and retire definition; cancel drained ready work and count affected batches together |
| Idempotency pruning | REQUIRES_NEW, READ COMMITTED, 3 s | At most 1000 expired receipts, rechecking their persisted expiry |
| History pruning | Bounded autocommit statements, 1 s per statement | Delete terminal execution candidates, then empty old batches; the caller controls the maintenance budget |

Transaction deadlines bound database work through Spring/JDBC. They do not by themselves bound connection-pool acquisition or guarantee that a broken driver immediately responds to cancellation. Configure and exercise pool and network timeouts separately. Retirement locks the complete queued set, deletes in parameter-bounded chunks and cancels/counts the same set before commit.

## Host transaction integration

Use the same DataSource and a JDBC transaction manager with savepoints when submission must commit with a business change:

```java
@Transactional
public void issueInvoice(Invoice invoice) {
    invoices.insert(invoice);
    mohs.enqueue(SEND_INVOICE, new SendInvoice(invoice.id()));
}
```

Without a host transaction, `accept` commits before the durable callback. Inside a host transaction on the same DataSource, the receipt remains provisional until that outer commit. Retirement differs from submission: `remove` requires the actual JDBC isolation to be READ_COMMITTED, including inside a host. A retained REPEATABLE_READ/SNAPSHOT view could omit work accepted before the job lock; unsupported isolation is rejected and the enclosing transaction fails.

Submission's savepoint lets a duplicate-key conflict roll back the attempted submission without aborting unrelated host work, including on PostgreSQL. A host rollback removes the execution, ready work, optional batch and newly inserted receipt.

The joined host must enable transaction synchronization. Mohs registers its wake-up/event callback for the host's successful commit and rejects a host transaction without synchronization before writing. A transaction on another DataSource does not own this submission: Mohs commits its own database transaction and publishes accordingly. This is not a distributed transaction across DataSources.

The host isolation level is inherited at a savepoint. Acceptance consumes the locking definition read itself, so a MySQL REPEATABLE READ snapshot cannot hide a retirement already committed before the lock was acquired.

## Ownership and lock order

Internal operations take applicable locks in this order:

1. Catalog singleton, for catalog publication/join or executable definition changes.
2. Session identities in sorted order.
3. Job identities in sorted order.
4. Selected work and execution identities in deterministic order.
5. Batch identities in sorted order.

The catalog mutex is absent from normal renew, claim and completion. A job's acquisition cap limits valid scheduler grants for that JobKey. It cannot prevent a paused process from continuing an external effect after losing its session.

Each acquisition has a fresh claim token. Even release without consuming an attempt produces a different token when reacquired. A stale result cannot consume a newer grant. A successfully applied result leaves its token in the last-attempt snapshot, allowing an uncertain caller to recognize ALREADY_APPLIED without counting the batch again.

Session expiry uses a fresh database clock sample under the session lock. Recovery first makes expiry irrevocable by revoking the session. An old list of live nodes is never sufficient authority to take work. Scheduled eligibility and local policy evaluation use the configured engine clock; see [type and time contract](../06-data/type-contract.md).

## Completion and external effects

User code runs outside Mohs' acquisition and completion transactions and holds no Mohs database connection between them. Its own transactions are independent. The following sequence can therefore occur:

1. The job successfully changes an external system.
2. The worker crashes before durable completion, or loses the commit response.
3. Recovery retries the execution while its retry policy allows it.

Handlers must use an idempotency key, an atomic application transaction, or fencing enforced by the destination when their effects require those guarantees. Scheduler acquisition and receipt deduplication do not provide exactly-once external effects. No global ordering is promised.

Completions may wait in a bounded process queue before group commit. A transient database failure retries the same group once with jitter; persistent failure does not expand into one transaction per result. Unresolved local markers are released so recovery can make progress. Close drains within its budget and starts no synchronous backlog sweep after that deadline. An already blocked JDBC call can outlive the caller's wait.

An active cancellation request suppresses a retry when it wins before that decision. Cancellation is cooperative: an already produced success can remain SUCCEEDED, and an external effect is not undone. Retiring a job prevents future firing and acceptance, drains ready work, and does not forcibly stop acquired user code.

## Unknown results and observable reads

A connection exception around commit does not prove rollback. A keyed submission can reconcile through its independent retained receipt. An unkeyed submission or batch can have committed despite an exception and must not be blindly repeated. Completion reconciles through its claim token; an uncertain acquisition can leave a grant that session recovery will resolve. After-commit events are process-local best effort and may be absent even when database state committed.

Terminal execution state, the latest attempt, retry insertion, batch count and fixed-delay rearm change in one transaction. A pending execution must have exactly one active work record. The history reader reports an invariant failure if it finds neither queue membership nor a grant, or both. It does not fabricate ENQUEUED as a recovery mechanism. The joined statement sees a coherent committed version on the supported database configuration; four-database tests pause a real completion immediately before commit and verify the old owned state, followed by the terminal state after commit.

Overview counts and throughput readings are separate queries and may observe different committed instants. They are operational estimates, not an atomic ledger. Code must not subtract independently sampled counters as though they came from one snapshot.

## Connections and verification

The host owns the pool. Streams from `JobStore.findAll` and `RateLimitStore.findAll` must be closed with try-with-resources. JDBC binding uses the common [type contract](../06-data/type-contract.md); IN-list chunking stays below SQL Server's parameter limit. SQL Server uses the supported RCSI configuration checked at startup.

Regression tests exercise host rollback and PostgreSQL savepoints, real SQL failure after queue insertion, lost responses after actual commit, trigger/input/revision races, expired-session recovery, cancellation versus retry, concurrent cap enforcement and concurrent terminal batch accounting on H2, PostgreSQL, MySQL and SQL Server. Source and test evidence for the ongoing refactor is in `review-notes/refactoring-implementation.md`.
