# 2. Architecture

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it answers |
| --- | --- |
| [architecture-overview.md](architecture-overview.md) | What architectural style this is and how the code proves it; the layer diagram; the dependency rules; the cross-cutting invariants |
| [module-architecture.md](module-architecture.md) | The eleven Maven modules: purpose, dependencies, consumers, notes, what a consumer declares |
| [package-architecture.md](package-architecture.md) | Package-by-package contracts, the acyclic public-API graph, the compatibility contract |
| [domain-model.md](domain-model.md) | Aggregates, value objects, domain events, invariants, business rules |
| [execution-lifecycle.md](execution-lifecycle.md) | The state machine, the transaction map, the tick, the complete failure-mode catalogue |
| [clustering-and-liveness.md](clustering-and-liveness.md) | Node identity, the node lease, fencing, sharding, the reaper, the failure detector's properties |
| [boundaries-and-fitness-functions.md](boundaries-and-fitness-functions.md) | Every architectural rule that is executable, and the gaps each mechanism admits |

## The thirty-second version

- **Style**: modular library, ports and adapters. `io.mohs.core` is pure contract; `io.mohs.engine`
  is the application core plus its ports; `io.mohs.store.jdbc` is the driven adapter;
  `io.mohs.autoconfigure` is the composition root and the only package allowed to see internals.
- **Coordination**: no leader, no consensus. Three database-arbitrated decisions — a CAS on the
  trigger, `SKIP LOCKED` on the claim, a fenced `DELETE` on the completion.
- **Liveness**: one heartbeat per node per tick writing a lease promise; peers derive death from the
  promise's age; every write over owned work carries the `(node_id, epoch)` fencing token.
- **Storage**: four hot-path tables split by write profile, so history size does not affect claim
  cost, plus a derived read model so the dashboard can read a cheap advisory column.
- **Guarantee**: at-least-once when `retries > 0` (the default); at-most-once when a job opts into
  `retries = 0`.
