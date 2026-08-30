# Mohs Documentation

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (`io.mohs:mohs-parent` 0.0.1-SNAPSHOT)

Mohs is an **embedded job scheduling library for Java 25 / Spring Boot 4**. It is not a server you
deploy: it is a set of jars an application declares, backed by the application's own relational
database. This documentation was reconstructed from the source tree — code, SQL migrations, build
files, tests and benchmark harnesses — and every claim below is traceable to a file in this
repository.

> **Reading rule.** Where this documentation and the code disagree, the code wins. Sections that
> could not be determined from the repository are marked `Unknown` rather than filled in.

---

## Start here

| If you are… | Read, in this order |
| --- | --- |
| New to the project | [Product overview](01-overview/product-overview.md) → [Architecture overview](02-architecture/architecture-overview.md) → [Local development](14-development/local-development.md) |
| Integrating Mohs into an app | [Capabilities](01-overview/capabilities.md) → [Java API](05-api/java-api.md) → [Configuration reference](07-configuration/configuration-reference.md) |
| Operating it in production | [Startup & shutdown](13-operations/startup-and-shutdown.md) → [Runbook](13-operations/runbook.md) → [Troubleshooting](13-operations/troubleshooting.md) → [Security](08-security/security-overview.md) |
| Reviewing the design | [Module architecture](02-architecture/module-architecture.md) → [Design decisions](15-design-decisions/README.md) → [Technical debt](technical-debt.md) |
| Tuning performance | [Performance characteristics](10-performance/performance-characteristics.md) → [Tuning](10-performance/tuning.md) → [Benchmarks](10-performance/benchmarks.md) |

---

## Table of contents

### 1. Overview
- [Product overview](01-overview/product-overview.md) — what Mohs is, what it solves, boundaries
- [Capabilities](01-overview/capabilities.md) — the feature inventory, each with its status
- [System context](01-overview/system-context.md) — actors, external systems, deployment shape
- [Glossary](01-overview/glossary.md) — the project's vocabulary

### 2. Architecture
- [Architecture overview](02-architecture/architecture-overview.md) — style, layers, dependency rules
- [Module architecture](02-architecture/module-architecture.md) — the eleven Maven modules
- [Package architecture](02-architecture/package-architecture.md) — packages and their contracts
- [Domain model](02-architecture/domain-model.md) — aggregates, value objects, invariants
- [Execution lifecycle](02-architecture/execution-lifecycle.md) — the state machine and who owns it
- [Clustering and liveness](02-architecture/clustering-and-liveness.md) — sharding, leases, fencing, the reaper
- [Boundaries and fitness functions](02-architecture/boundaries-and-fitness-functions.md) — the rules enforced by tests

### 3. Functional
- [Job definition and registration](03-functional/job-definition-and-registration.md)
- [Scheduling and triggers](03-functional/scheduling-and-triggers.md)
- [Claim and dispatch](03-functional/claim-and-dispatch.md)
- [Retry and failure handling](03-functional/retry-and-failure.md)
- [Cancellation and timeouts](03-functional/cancellation-and-timeouts.md)
- [Batches](03-functional/batches.md)
- [Rate limits and execution windows](03-functional/rate-limits-and-windows.md)
- [Idempotency](03-functional/idempotency.md)

### 4. Engineering
- [Coding standards](04-engineering/coding-standards.md)
- [Design patterns in use](04-engineering/design-patterns.md)
- [Concurrency model](04-engineering/concurrency.md)
- [Error handling](04-engineering/error-handling.md)
- [Transactions](04-engineering/transactions.md)
- [Resilience](04-engineering/resilience.md)
- [Extensibility](04-engineering/extensibility.md)

### 5. API
- [API overview](05-api/api-overview.md)
- [REST endpoints](05-api/endpoints.md)
- [Error model (RFC 7807)](05-api/error-model.md)
- [Java API](05-api/java-api.md)
- [Dashboard SSE stream](05-api/streaming.md)

### 6. Data
- [Data model](06-data/data-model.md)
- [Schema reference](06-data/schema.md)
- [Indexes](06-data/indexes.md)
- [Migrations](06-data/migrations.md)
- [Dialects](06-data/dialects.md)
- [Data lifecycle and retention](06-data/data-lifecycle.md)

### 7. Configuration
- [Configuration reference](07-configuration/configuration-reference.md) — every `mohs.*` property

### 8. Security
- [Security overview](08-security/security-overview.md) — what exists, and what deliberately does not

### 9. Observability
- [Metrics](09-observability/metrics.md)
- [Logging](09-observability/logging.md)
- [Health and diagnostics](09-observability/health-and-diagnostics.md)

### 10. Performance
- [Performance characteristics](10-performance/performance-characteristics.md)
- [Tuning](10-performance/tuning.md)
- [Benchmarks and harnesses](10-performance/benchmarks.md)

### 11. Testing
- [Testing strategy](11-testing/testing-strategy.md)

### 12. Build
- [Build system](12-build/build-system.md)
- [Modules and publishing](12-build/modules.md)

### 13. Operations
- [Startup and shutdown](13-operations/startup-and-shutdown.md)
- [Deployment](13-operations/deployment.md)
- [Runbook](13-operations/runbook.md)
- [Troubleshooting](13-operations/troubleshooting.md)

### 14. Development
- [Local development](14-development/local-development.md)
- [Contributing](14-development/contributing.md)

### 15. Design decisions
- [Decision records](15-design-decisions/README.md) — reconstructed from code evidence

### 16. Reference
- [Technology stack](16-reference/technology-stack.md)
- [Repository map](16-reference/repository-map.md)

### Cross-cutting
- [Technical debt](technical-debt.md)
- [Documentation audit](documentation-audit.md)

---

## Conventions used in this documentation

Every functional statement carries one of these status markers when its maturity is not obvious:

| Marker | Meaning |
| --- | --- |
| **Implemented** | Code exists, is wired, and is covered by tests |
| **Implemented, not wired** | Code exists and is tested in isolation, but no bean/route exposes it |
| **Partially implemented** | Works for some inputs/paths only; limits are stated |
| **Referenced, not implemented** | A name or attribute exists in the API but has no behaviour behind it |
| **Not present** | Searched for and absent from the repository |
| **Unknown** | Cannot be determined from the repository alone |

Source references use repository-relative paths, e.g. `mohs-engine/src/main/java/io/mohs/engine/Engine.java`.
