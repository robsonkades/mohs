# Idempotent acceptance

An idempotency key deduplicates acceptance for one job/key pair. It does not make the handler's effects exactly once. Technical keys compare exactly, including case; invalid keys are rejected before SQL.

JdbcSubmissionStore owns acceptance of history, READY work and receipt in one transaction. A matching unexpired receipt returns its original identity, schedule and actor even if history was pruned. The receipt's expiry is fixed from database time plus the retention configured when accepted. At now == expires_at it is expired and may be replaced atomically. Bounded cleanup starts hourly when caught up and continues after one second while a full page or failure leaves work pending; it does not define the deduplication window.

Zero retention creates new nonexpiring receipts. A configuration change does not rewrite existing expiry values. History retention and receipt retention are independent. Batch members do not accept individual idempotency keys; a batch is one atomic creation of its members and counters.

Inside a compatible host transaction, acceptance uses a savepoint. A duplicate-key collision rolls back the failed operation before resolving the receipt, preserving the host transaction where supported. Scheduled firing uses its own transaction with trigger revision/time CAS. After-commit notification wakes only the local coordinator; it never runs before durable acceptance.

A lost connection at commit leaves the result unknown. Retrying a manual submission with the same key resolves the accepted receipt while it remains valid. Without a key, a new submission can create another execution. Expiration also intentionally permits another acceptance. Applications requiring a durable business uniqueness rule must enforce that rule with their own data.

See [Java API](../05-api/java-api.md), [type/expiry contract](../06-data/type-contract.md) and [transactions](../04-engineering/transactions.md).
