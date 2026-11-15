# Batches

A batch is a flat set of independent executions with durable progress counters. Members have no ordering, dependencies or compensation.

```java
Batch batch = mohs.batch("nightly-invoices", members -> {
    for (Customer customer : customers) {
        members.add(SEND_INVOICE, new SendInvoice(customer.id()));
    }
});
String id = batch.batchId();
Optional<BatchSnapshot> progress = mohs.findBatch(id);
```

## Acceptance

The facade collects typed members, validates each distinct job and encodes every input before storage. The acceptance transaction locks the submitted jobs in key order, rechecks their active status and contracts, then writes the batch and all members together. The total comes from that immutable member list; an empty batch is rejected. The human-readable name must be nonblank and at most 255 characters.

Inside a host transaction on the same DataSource, durability belongs to the host commit. A rollback removes all members and the batch. An exception after an unknown commit does **not** prove that nothing persisted. Batch submission has no idempotency key and must not be automatically repeated after a lost response. See [transactions](../04-engineering/transactions.md).

## Progress

`mohs_batches` stores `total`, `succeeded` and `failed` as BIGINT values exposed as Java `long`. `pending = total - succeeded - failed` is derived. Constraints and Java validation reject negative counters and totals exceeded by terminal counts.

| Transition | Counter effect |
| --- | --- |
| Successful terminal completion | Increment succeeded once. |
| Failed or cancelled terminal completion | Increment failed once. |
| Retry or same-attempt deferral | No increment. |
| Queued cancellation or job retirement | Cancel the exact drained members and increment their batch counters in the same transaction. |
| Duplicate or stale completion | No second increment. |

All counter writes belong to the complete transition transaction. Batch locks follow sorted batch identities when multiple batches are touched. A missing referenced batch fails the operation and rolls it back instead of silently dropping a count. `BatchStore` is a reader, not an independent counter-writing API.

## Notifications and retry

The terminal operation can identify the single completion that changes pending to zero while holding the batch lock. Its global `BatchCompleted` event is best effort on that process. A commit with an unknown response, crash or bulk operational cancellation can leave durable completion without a delivered event. Read `BatchSnapshot` for progress; notifications are not a durable continuation protocol.

`Batch` is a value containing a UUIDv7 identity. There is no callback registration, local callback map or LRU lifecycle. Consumers that need a durable reaction must design that reaction explicitly in their application's transaction/message protocol.

A terminal batch member cannot be manually reopened: its terminal result is already counted. Submit new independent work or a new batch deliberately. Automatic retries before a terminal result remain part of the original member and do not count early.

History retention may remove terminal member snapshots. The counters remain meaningful without reading every member; an empty old batch can subsequently be pruned. A page or overview assembled from separate queries can observe different committed moments.
