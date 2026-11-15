# 2. Architecture

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

| Document | What it answers |
| --- | --- |
| [architecture-overview.md](architecture-overview.md) | What architectural style this is and how the code proves it; the layer diagram; the dependency rules; the cross-cutting invariants |
| [module-architecture.md](module-architecture.md) | The eleven Maven modules: purpose, dependencies, consumers, notes, what a consumer declares |
| [package-architecture.md](package-architecture.md) | Package-by-package contracts, the acyclic public-API graph, the compatibility contract |
| [domain-model.md](domain-model.md) | Aggregates, value objects, domain events, invariants, business rules |
| [execution-lifecycle.md](execution-lifecycle.md) | The state machine, the transaction map, the tick, the complete failure-mode catalogue |
| [clustering-and-liveness.md](clustering-and-liveness.md) | Node identity, the node lease, fencing, sharding, the reaper, the failure detector's properties |
| [boundaries-and-fitness-functions.md](boundaries-and-fitness-functions.md) | Every architectural rule that is executable, and the gaps each mechanism admits |

## Current model

- Typed inputs and immutable executable contracts are published atomically before sessions join.
- The database arbitrates complete operations, with irreversible session revocation and acquisition-token fencing.
- Eight tables include one active-work table and independent history and receipts.
- A bounded coordinator rotates candidate and maintenance pages; job locks enforce positive cluster concurrency caps.
- Recovery is finite and duplicate external effects remain possible. There is no unconditional eventual-delivery or exactly-once effect guarantee.

See [architecture-overview.md](architecture-overview.md) for responsibilities and [execution-lifecycle.md](execution-lifecycle.md) for transitions.
