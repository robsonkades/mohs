# 4. Engineering

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

Practices, models and guidelines derived from the actual code. Where a document separates **current
practice** from **recommended practice**, it says so explicitly — recommendations are never
presented as existing behaviour.

| Document | What it covers |
| --- | --- |
| [coding-standards.md](coding-standards.md) | Naming, prose language, comments, immutability, validation, nullness, Effective Java items in use, modern Java usage, tooling gaps |
| [design-patterns.md](design-patterns.md) | Every pattern genuinely present, with intent, location and trade-off — plus the ones deliberately avoided |
| [concurrency.md](concurrency.md) | The full thread inventory, locks, atomics, confinement, safe publication, backpressure, lock ordering, and every explicitly handled race |
| [error-handling.md](error-handling.md) | The exception taxonomy, the five error classes, failure isolation by granularity, severity mapping, the REST error model |
| [transactions.md](transactions.md) | The three transactional postures, the four atomic units, isolation requirements, group commit, accepted anomalies |
| [resilience.md](resilience.md) | Every resilience mechanism with the failure it protects against, plus the chaos scenarios and their recorded results |
| [extensibility.md](extensibility.md) | The three supported extension points, what may not be extended and why, and how to add a dialect |

## The engineering posture, in five rules

Extracted from the code rather than aspiration:

1. **Every wait has a deadline; every queue has a ceiling.** There is no unbounded `join`, `await`,
   `get` or queue anywhere in the tree.
2. **Degradation is monotonic.** A guard that cannot be fully applied is truncated, never switched
   off. A fallback is never worse than the thing it replaces.
3. **The granularity of error handling is the granularity of degradation.** Independent steps get
   independent `try` blocks; the one deliberate exception is the heartbeat, and it says why.
4. **A message names the fix.** Validation messages name the field and the value; warnings name the
   property to change and the consequence of not changing it.
5. **A comment records the incident, not the code.** The rules with measured numbers behind them are
   the ones that survive a refactor.
