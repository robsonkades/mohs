# 11. Testing

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [testing-strategy.md](testing-strategy.md) | The full inventory, what is tested at each level, the principles the codebase applies, what is **not** covered, how to run the suite, and how to test your own jobs |

## The numbers

**109 files under `src/test`**, of which **102 contain at least one `@Test` or `@ParameterizedTest`**,
totalling **764 test methods** (counted 2026-09-04). Ten of those 102 are `*Scenario` classes invoked
by name only, so **92 classes run in the normal suite** — plus three source-scan guards in
`mohs-store-jdbc`.

JaCoCo (0.8.14) produces a per-module report under `target/site/jacoco`; there is no aggregation
across modules and no threshold, so no single coverage figure is asserted anywhere in this
documentation.

## Two things to know before running it

1. **Docker is required** for `mohs-store-jdbc` and `mohs-benchmark`. Without it, container-backed
   tests fail on `Could not initialize class *TestSupport` — an environment failure, not a
   regression. Use `./mvnw verify -Dskip.frontend=true` for a backend-only run without Node.
2. **There is no CI.** Every test in this repository is run manually; nothing gates a commit.
