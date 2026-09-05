# Transaction management

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

Mohs manages transactions **programmatically**, with `TransactionTemplate` over a
`DataSourceTransactionManager` built on the host's `DataSource`. There is no `@Transactional`
anywhere in the production code, and that is deliberate: propagation and isolation are decisions per
operation, and an annotation would hide them.

## Transactional postures

| Posture | Propagation | Isolation | Used by |
| --- | --- | --- | --- |
| **Own transaction, always** | `REQUIRES_NEW` | Explicit `READ COMMITTED` | Claim, completion, requeue, cancel-queued, manual retry |
| **Own transaction, joins nothing** | Default (`REQUIRED`) | Explicit `READ COMMITTED` | Trigger firing — it only ever runs on the engine's loop thread, where there is no outer transaction, so the explicit level is always the one applied |
| **Joins the host's transaction** | `NESTED` | Inherited | The enqueue unit |
| **Joins the host's transaction when there is one** | Default (`REQUIRED`) | Explicit `READ COMMITTED` when it opens the transaction itself; inherited inside the host's | `Mohs.remove` — draining the queue and marking the definition retired as one indivisible pair, the only act `JdbcJobStore` wraps in a transaction of its own. Called from the host's code, so a `remove` inside a `@Transactional` rolls back with it — that is the point; inside that transaction Spring silently ignores the requested level and the host's runs. Every other definition write is either one guarded statement (`pause`, `resume`, `reschedule`, arming the next fire) or, for `define`, a trigger-snapshot read plus one guarded UPDATE/INSERT — protected against a lost update by not rewriting `next_fire_at` on an unchanged schedule, never by an isolation level; in autocommit or in whatever transaction the host has bound |

### Why claim and completion are `REQUIRES_NEW`

From `JdbcWorkQueue`'s constructor comment:

> A claim is ALWAYS its own transaction — `REQUIRES_NEW` makes that executable rather than merely
> conventional: with the default `REQUIRED`, an outer transaction (an interceptor, a test) would
> impose ITS isolation and the `READ COMMITTED` below would be silently ignored (MySQL defaults to
> `REPEATABLE READ`, the divergence this killed).

The engine calls from its own loop with no outer transaction, so the suspension never actually
happens in normal operation. `REQUIRES_NEW` is there to make the guarantee **structural** rather
than dependent on the caller.

### Why `READ COMMITTED` is explicit

Three mechanisms depend on it:

| Mechanism | Dependency |
| --- | --- |
| `SKIP LOCKED` in the claim | Assumes a per-statement snapshot; the candidate sweep must see rows committed by concurrent claims |
| The fence (`DELETE … WHERE node_id = ? AND epoch = ?`) | Assumes "last write wins" |
| `RateLimitStore#charge` | **Re-reads the row between CAS attempts.** Under `REPEATABLE READ` the re-read would return the same snapshot, every attempt would fail identically, and the retry would become an expensive no-op |

None of them may inherit the database's default, because MySQL's default is `REPEATABLE READ`.

### Why the enqueue unit is `NESTED`

`JdbcStoreTransactions` is the exact opposite of the claim, and the reason is composability with the
host:

| Situation | `NESTED` behaviour |
| --- | --- |
| No active transaction | Behaves like `REQUIRED` — opens and commits its own |
| Inside the host's `@Transactional` method | Becomes a **savepoint** |

The savepoint is what makes the Idempotent Receiver composable. Two concrete failures it prevents:

1. On PostgreSQL, a constraint violation **without** a savepoint aborts the whole transaction
   (`25P02`), so the recovery path that reads the winning execution would be unreachable — the
   connection is poisoned.
2. With `REQUIRED`, the template's rollback-only flag would doom the **host's** commit *after* Mohs
   had already returned a successful `Enqueued` receipt to the caller.

The "joins your transaction and falls together with it" semantics are preserved: the unit only
becomes durable when the host's transaction commits. No explicit isolation is set — inside the
host's transaction the isolation is theirs, and pure inserts do not depend on the level.

## The four atomic units

### 1. The enqueue unit

```
INSERT mohs_idempotency   (only when a key is present)
INSERT mohs_execution     (state = 'PENDING')
INSERT mohs_ready         (visible_at = the scheduled time)
```

**The ports deliberately do not open transactions.** `HistoryStore#record` and `WorkQueue#offer`
each state in their Javadoc that the caller **must** compose them into one transaction. Calling
either alone is not a supported mode: a partial failure leaves either an orphan idempotency key
(deduplicating against nothing for the whole window) or an unreachable `PENDING` execution.

### 2. The trigger firing

```
UPDATE mohs_job_definitions SET next_fire_at = :new
 WHERE job_key = :key AND next_fire_at = :observed AND retired = false
-- if the CAS won:
INSERT mohs_execution   (one per occurrence)
INSERT mohs_ready       (one per occurrence)
```

Atomicity here **is** the correctness argument: a crash between the advance and the insert can
neither lose nor duplicate an occurrence, which is why occurrences need no idempotency key.

The CAS compares a value **read from the column**, never one computed in the JVM that never went
through the database — temporal precision does not round-trip identically across four dialects.

### 3. The claim

```
SELECT … FOR UPDATE SKIP LOCKED
DELETE mohs_ready WHERE execution_id IN (…)
INSERT mohs_lease                     (batched)
```

PostgreSQL folds all three into one statement (`WITH picked … DELETE … USING … INSERT … SELECT`);
SQL Server folds the first two into a `DELETE … OUTPUT` self-joined on the `TOP … WITH (UPDLOCK,
ROWLOCK, READPAST)` pick and keeps the batched lease insert. The SQL Server fold is not an optimisation but a correctness fix: the
separate `DELETE … WHERE execution_id IN (…)` was compiled as a scan of `idx_mohs_ready_claim` whenever
the table's statistics were empty (a fresh install, a queue that emptied and refilled), and a
write-plan scan takes `U` locks on every key it passes — the keys a peer's claim had just locked —
so two claims deadlocked on each other's picks. The fold is driven by the same seek that takes the
locks, whatever the statistics say.

Either way, the storage guarantees there is no instant at which an execution is neither queued nor
owned — **not the application's call order**.

### 4. The completion

```
DELETE mohs_lease WHERE execution_id = ? AND node_id = ? AND epoch = ?   (batched; the fence)
INSERT mohs_attempt                                                      (winners only)
UPDATE mohs_execution SET state = :terminal, finished_at = :t            (terminal results)
INSERT mohs_ready                                                        (non-terminal results: the retry)
UPDATE mohs_batches SET succeeded|failed = … RETURNING …                 (batch members)
UPDATE mohs_job_definitions SET next_fire_at = …                         (fixed-delay rearm, guarded by IS NULL)
```

Six statement kinds, one transaction, **all or nothing**. Three reasons they cannot be split:

- A retry written outside would leave, on a crash between commits, an execution with no lease, no
  queue entry and a non-terminal state — an orphan invisible forever.
- A batch count written outside would leave a batch that never closes.
- A fixed-delay rearm written outside would break the chain silently.

`jobStore` is passed **into** `LeaseStore#complete` for exactly this: the rearm takes part in the
transaction by sharing the `DataSource`.

## Ordering inside the completion

Results are sorted by `executionId` before any row is touched — in `complete` **and** in `requeue`.
This is JCIP ch. 10 (lock ordering) applied to row locks: a flusher (arrival order) and a reaper
(`claimed_at` order) over overlapping sets would otherwise lock in opposite orders. A measured run
recorded 23 deadlocks before the ordering was imposed.

## Transaction boundaries relative to the handler

**The handler does not run inside a Mohs transaction.** The claim commits, the handler runs, the
completion opens its own transaction afterwards. Consequences:

| Consequence | Detail |
| --- | --- |
| A handler may open its own transactions freely | It shares the host's `DataSource` but not a Mohs transaction |
| A handler enqueueing a job **does** compose | `StoreTransactions` is `NESTED`, so `mohs.schedule(...)` inside a `@Transactional` handler joins that transaction — the transactional-outbox pattern |
| A long handler holds **no** database connection through Mohs | Only its own, if it opens one |
| The completion is a separate commit | This is what makes group commit possible, and what creates the declared durability window |

## Group commit and its window

With `mohs.engine.completion-flush-on-every-result=false` (the default), completions accumulate for
**256 results or 5 ms** and commit in one `LeaseStore#complete` transaction.

| Property | Value |
| --- | --- |
| The declared cost | Batching adds a nominal flush delay of 5 ms; queueing and database commit latency can extend the actual durability window |
| The declared risk | A crash can leave finished handlers without durable completions; recovery may invoke them again while retry budget remains |
| What does **not** change | The contract was already at-least-once. This changes the exposure to duplicates, not the guarantee |
| Batch failure | Falls back to one transaction per result |
| Two paths bypass it | `failBeforeDispatch` and `abandonOwnership` — their callers depend on the "it threw, so it did not happen" contract, and an enqueue that always returns would silently change that |

`Engine#stop` drains the batcher **before** writing the final `STOPPED` heartbeat. The ordering is
mandatory: the final heartbeat zeroes `expires_at`, and from then on every peer considers this node
dead — a result still in the queue would be reclaimed as an orphan.

## Isolation-level anomalies, accepted knowingly

| Anomaly | Where | Why it is acceptable |
| --- | --- | --- |
| **Read skew** between the counts in `GET /overview` | `Mohs#overview` performs independent reads rather than one transactional cut | Executions transitioning during the query may disagree between the numbers. Acceptable for polling; a serialisable cut here would be cost without benefit |
| The two throughput readings are **not nested** | `OverviewSnapshot` | They are separate round trips in distinct snapshots — measured at ~19 rows of asymmetry per call at a 4 k/s operating point. Adding, subtracting or stacking one on the other produces a negative number sooner or later |
| Advisory `state` staleness | The derived read model | A `PENDING` row with neither queue nor lease reads as `ENQUEUED` — the bounded window of a completion flush in progress |

## Connection usage

| Pattern | Detail |
| --- | --- |
| Pooling | The **host's** pool. Mohs never creates a `DataSource` |
| Streaming reads | `JobStore#findAll`, `findAllAnnotationSourced` and `RateLimitStore#findAll` return `Stream` over an open cursor. **The caller owns the lifecycle** (try-with-resources) |
| PostgreSQL caveat, documented on the ports | The cursor only streams **inside a transaction** (autocommit off). Outside one, the driver materialises the entire result before returning the first item, despite the configured `fetchSize` |
| Never write with a read cursor open | Stated at two sites: `MohsJobScanner#reconcileOrphans` collects keys to a list and marks them **after** the try-with-resources closes; the same rule is cited for `JdbcJobStore` |
| Statement timeouts | Store templates have bounded statement timeouts; rate-limit operations use 2 s, history pruning uses 1 s per statement, and idempotency pruning uses 5 s. Acquisition and driver network timeouts are separate |
| Chunking | `JdbcSupport.chunksOf` splits `IN` lists so no statement approaches SQL Server's ~2,100-parameter ceiling |

## Recommended practice for host applications

| Do | Do not |
| --- | --- |
| Call `mohs.schedule(...)` **inside** your business transaction when the job must only exist if the business change commits | Rely on `Batch#onCompletion` for anything that must happen |
| Make handlers idempotent | Assume exactly-once |
| Size the pool for measured concurrent database usage from handlers, engine, monitoring and host traffic | Set a global `spring.datasource.hikari.transaction-isolation` without checking the PostgreSQL `V5` migration's requirement (it refuses to run outside `READ COMMITTED`, rather than silently losing rows) |
| Keep handlers' own transactions short | Hold a transaction open across the whole handler when it performs network I/O |
