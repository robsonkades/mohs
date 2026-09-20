# 6. Data

The first-release schema has eight tables. The [index contracts](indexes.md) describe the access paths and operations supported by this layout.

| Document | Contents |
|---|---|
| [data-model.md](data-model.md) | Identities, relationships, invariants and retention |
| [schema.md](schema.md) | Links to the four canonical installers |
| [type-contract.md](type-contract.md) | Java, JDBC, column precision and exact key comparison |
| [indexes.md](indexes.md) | Queries served by each index and measurement limits |
| [migrations.md](migrations.md) | Fresh installation; no pre-release upgrade chain |
| [dialects.md](dialects.md) | SQL and locking differences |
| [data-lifecycle.md](data-lifecycle.md) | Work, receipts, history and session cleanup |

| Table | Lifetime |
|---|---|
| mohs_catalog | Singleton arbitration of executable contracts |
| mohs_job_definitions | Durable executable/recurrence policy and trigger revision |
| mohs_work | One READY or LEASED row per active execution; deleted at terminal completion |
| mohs_execution | Immutable birth plus latest completed attempt and terminal state |
| mohs_idempotency | Acceptance receipt with independent expiry |
| mohs_nodes | Process session, expiry and irreversible revocation |
| mohs_batches | Durable terminal member counters |
| mohs_rate_limits | Shared token bucket, including fractional refill |

The active-work indexes exclude retained execution snapshots. This structural separation does not make shared database CPU, cache, storage or retention traffic irrelevant to claims. Compare equivalent loads before making a capacity claim.
