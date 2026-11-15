# Java API

The first release uses typed executable registrations and direct scheduling methods. Register code through `JobRegistration<T>` or the supported annotations before the worker joins its [executable catalog](../04-engineering/executable-catalog.md).

## Registration and submission

```java
record SendInvoice(long invoiceId) {}

static final JobRef<SendInvoice> SEND_INVOICE =
        JobRef.of("send-invoice", SendInvoice.class, "invoice-input-v1");

@Bean
JobRegistration<SendInvoice> sendInvoice(InvoiceService service) {
    return JobRegistration.of(SEND_INVOICE,
            (input, context) -> service.send(input.invoiceId()));
}
```

After Spring finishes registration, inject `Mohs` into application services:

```java
var options = SubmissionOptions.defaults()
        .withPriority(Priority.HIGH)
        .withActor("ops@acme.com")
        .withIdempotencyKey("invoice-4711");

Enqueued receipt = mohs.enqueue(SEND_INVOICE, new SendInvoice(4711), options);
mohs.scheduleAt(SEND_INVOICE, new SendInvoice(4712), when);
mohs.scheduleAfter(SEND_INVOICE, new SendInvoice(4713), Duration.ofMinutes(10));

var recurring = mohs.scheduleRecurring("invoice-4711-weekly", SEND_INVOICE,
        new SendInvoice(4711), new CronSpec("0 0 9 * * MON", ZoneId.of("America/Sao_Paulo")));
```

`JobRef<T>` carries the stable key, local Java input class and durable input-contract ID. Its two-argument factory uses the job ID as the contract. Persisted snapshots contain JSON and that contract ID; Java class names and executable lambdas are never persisted. Use `NoInput` for a job that takes no input. Generic root types use `Class<T>`; wrap a parameterized collection in an application record.

The first three operations return `Enqueued`; recurring registration returns `RecurringSchedule`. Options are immutable values: constructing or copying them has no storage side effect. `JobDefinition.of` also constructs a value and does not register executable code.

Submission encodes and validates the input before opening its acceptance transaction. The store rechecks active definition and input contract under locks. See [programmatic jobs](programmatic-jobs.md) for complete application examples.

## Receipts and transactions

`Enqueued` identifies accepted work; it is not a future for the handler's result. Within a host transaction using the same DataSource, its durability depends on the host commit. A rollback removes the accepted rows. A connection failure around commit can leave the outcome unknown even if the method throws.

An optional idempotency key is scoped to the job and returns the original retained receipt on a duplicate; input differences do not replace that receipt. Receipt expiration is fixed at acceptance and independent of execution-history pruning. An unkeyed operation must not be blindly repeated after an unknown result. Events are published after a confirmed commit and can be lost on a crash. See [transactions](../04-engineering/transactions.md) and [idempotency](../03-functional/idempotency.md).

## Batches

```java
Batch batch = mohs.batch("nightly-invoices", members -> {
    for (Customer customer : customers) {
        members.add(SEND_INVOICE, new SendInvoice(customer.id()));
    }
});
Optional<BatchSnapshot> progress = mohs.findBatch(batch.batchId());
```

A batch and all of its members are accepted atomically. The returned `Batch` contains only its identity. Progress is durable; global listeners remain process-local best effort. A batch has no per-object completion callback and provides no durable workflow continuation. See [batches](../03-functional/batches.md).

## Controls and reads

| Operation | Contract |
| --- | --- |
| `pause(JobKey)` / `resume(JobKey)` | Suspend or resume automatic firing. Accepted work and explicit submissions remain eligible. |
| `reschedule(JobKey, Schedule)` | Atomically advance the schedule revision and calculate a new trigger. Unknown or retired key returns empty. |
| `remove(JobKey)` | Retire either an annotated or programmatic definition, cancel queued work and prevent new acceptance. Acquired work can finish. A joined host transaction must use READ_COMMITTED. Retire active named recurrences before their executable. |
| `reactivate(JobKey)` | Explicitly restore a retained definition against the installed/local catalog, preserving operator pause and starting a new schedule revision. Returns false if absent or already active. |
| `cancel(ExecutionId)` | Request cooperative cancellation; the returned state can still be RUNNING. An already produced success can remain successful. |
| `retry(ExecutionId)` | Reopen a standalone MANUAL FAILED execution with an active job. Scheduled occurrences and batch members cannot be retried individually. |
| `adjustRateLimit(String, int, Duration)` | Change an already declared shared bucket. Unknown name returns empty. |
| `lifecycle()` | Control this node's RUNNING, PAUSED and DRAINING lifecycle. |
| `findJob`, `jobs`, `jobRef` | Read active definitions or locally declared typed references. `jobs()` materializes the active catalog. |
| `findExecution` | Read the current projection and only the latest completed attempt. |
| `executions(ExecutionQuery)` | Read summaries without attempt error details, ordered by descending execution identity. |
| `findBatch`, `nodes`, `rateLimits`, `runners` | Read durable progress, session observations, buckets, and local runner state respectively. |
| `overview(Duration)` | Operational estimates from separate reads, not a single atomic ledger. |

Execution and batch IDs are canonical lowercase UUIDv7 strings. A nonempty page cursor must be an execution ID; an absent or blank cursor starts the first page. Technical job/resource/idempotency keys use exact ASCII comparison; human labels and actors allow Unicode. See the [type contract](../06-data/type-contract.md).

## Execution and extension contracts

`JobContext` exposes identity, attempt information and cooperative cancellation to the handler. User code runs outside acquisition/completion transactions. A worker that loses its lease can still finish an external effect; rejecting its old completion does not undo that effect. Handlers must supply idempotency or destination-enforced fencing when needed.

Annotations, `JobRegistration`, runners, windows, retry policies and global listeners are the extension points. Internal persistence ports expose complete operations and read models; consumers should use the test kit instead of implementing a mock facade tied to every method. There is no compatibility path for the removed scheduling builder, metadata-only definition API, typeless registration or pre-release schemas.
