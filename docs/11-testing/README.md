# 11. Testing

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [testing-strategy.md](testing-strategy.md) | Test layers, database and REST contracts, deterministic time, dashboard checks, CI reports and operational scenarios |

A normal `./mvnw verify` runs the Java suites, database integration tests and frontend build. The
ten `*Scenario` classes in `mohs-benchmark` are operational harnesses and must be selected by name.
GitHub Actions runs the full reactor on pushes and pull requests to `main` and uploads Surefire XML
reports. JaCoCo produces per-module reports without a repository-wide threshold.
