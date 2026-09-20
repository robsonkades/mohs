# Package architecture

Public API is in mohs-api, under io.mohs.core. Engine and JDBC types are implementation contracts for the library, not consumer compatibility SPIs. The starter composes them; the REST module accepts the public facade and read models.

| Package | Contract |
|---|---|
| io.mohs.core | Mohs, lifecycle, batch values/builders, submission options and query/read models |
| io.mohs.core.job | JobKey, typed JobRef and NoInput |
| io.mohs.core.definition | Immutable JobDefinition/JobOptions, registrations and annotations |
| io.mohs.core.schedule | Schedule variants and misfire policy |
| io.mohs.core.execution | Execution identity, state, latest attempt and handler context |
| io.mohs.core.event | Best-effort events, listeners and execution interceptors |
| io.mohs.core.resource | Runner specifications, windows and rate limits |
| io.mohs.engine | Scheduling/execution orchestration, typed snapshot codec, pure planning and storage operation ports |
| io.mohs.store.jdbc | Transaction-owning implementations, row mappings and shared SQL with dialect overrides |
| io.mohs.autoconfigure | Spring bean scanning, atomic publication, resources and engine lifecycle |
| io.mohs.rest | HTTP conversion, validation, controllers and DTOs; host supplies security |
| io.mohs.cron | Calendar parsing and occurrence calculation |
| io.mohs.test | Handler testing and deterministic in-memory protocol implementations |

## Storage ports follow atomic operations

| Port | Responsibility |
|---|---|
| SubmissionStore | Accept immutable history/work/receipt/batch together |
| TriggerFirer | Accept scheduled occurrences with revision/time CAS; implemented by JdbcSubmissionStore |
| JobStore | Publish definitions/catalog, query bounded policies, retire/reactivate and change triggers |
| WorkQueue | Acquire visible work under authoritative admission, cancel, manual retry and fenced deferral |
| LeaseStore | Complete/recover owned work, including latest result, batch effects and fixed-delay rearm |
| HistoryStore | Coherent reads and independent bounded history/receipt cleanup |
| BatchStore | Read durable batch value; no callback registry |
| RateLimitStore | Publish and charge shared buckets with full-state CAS and database time |
| NodeStore | Register/renew/revoke sessions and query membership |

No HistoryStore.record, WorkQueue.offer, StoreTransactions or JdbcStoreTransactions public composition is required. A caller cannot construct a supported half-execution by composing table writes. Manual submission joins a compatible host transaction with a savepoint; scheduled work and ownership operations use their own short transactions. See [transactions](../04-engineering/transactions.md).

## Extension and release boundaries

Consumers declare typed registrations or annotations and implement handlers, listeners, interceptors and supported resource policies. Payload contracts identify formats independently of local Java class names. Applications should use the test kit instead of implementing the entire Mohs facade.

This version has not been released. Removed staged definition/scheduling APIs have no deprecated adapters. Future compatibility promises apply to released public contracts; they do not constrain this baseline. Public/internal dependencies and nullability are checked by the relevant source tests and review; not every package rule is mechanically enforced.
