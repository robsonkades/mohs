# Mechanisms and their purpose

The first-release design uses a small set of concrete mechanisms. Their value is the invariant they enforce, not a pattern count.

| Mechanism | Concrete purpose and cost |
|---|---|
| Immutable JobRef / JobOptions / JobDefinition | Validate identity, format and policy at construction; remove staged mutable definition APIs |
| SubmissionStore complete acceptance | History/work/receipt/batch commit together; savepoint support for a compatible host transaction |
| Catalog singleton | Serialize empty-catalog birth, publication and session entry; incompatible live catalogs cannot coexist |
| Database session lease and revocation | One authority for membership, irreversible after expiry; failure detection can suspect a paused process |
| Per-acquisition token and conditional writes | Reject stale completion/requeue; replay recognizes the retained latest token |
| Canonically ordered job locks | Strict authorized-grant concurrency limits and shared writer lock order; hot jobs may serialize |
| One READY/LEASED work row | Fewer physical transitions, separate active indexes from retained snapshots |
| Keyset pages and rotating stages | Bounded returned metadata and progress past excluded prefixes; no global FIFO guarantee |
| Completion groups | Amortize commit calls with bounded buffering; a crash can lose local outcomes and lead to duplicate external effects |
| Full-state bucket CAS | Configuration and charging cannot overwrite each other's arithmetic; missing/unknown admission fails closed |
| Shared JDBC defaults | Remove repeated identical SQL while keeping meaningful dialect differences explicit |

There is no compatibility adapter for ScheduleCommand, JobSpec, PolicySpec or table-write composition. Batch callbacks are not kept in a process registry. There is no dual-write or rolling mixed-schema path for unreleased versions.

Use [architecture overview](../02-architecture/architecture-overview.md), [transactions](transactions.md) and [data model](../06-data/data-model.md) for the full contracts and storage layout.
