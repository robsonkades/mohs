# 11. Testing

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [testing-strategy.md](testing-strategy.md) | The full inventory, what is tested at each level, the principles the codebase applies, what is **not** covered, how to run the suite, and how to test your own jobs |

## The numbers

**104 files under `src/test`**, of which **98 contain at least one `@Test`**, totalling **711 `@Test`
methods**. Nine of those 98 are `*Scenario` classes invoked by name only, so **89 classes run in the
normal suite** — plus 12 executable architecture rules.

**No coverage report is produced by the build** — there is no JaCoCo and no threshold. Any coverage
percentage would be unsubstantiated, so none is quoted anywhere in this documentation.

## Two things to know before running it

1. **Docker is required** for `mohs-store-jdbc` and `mohs-benchmark`. Without it, container-backed
   tests fail on `Could not initialize class *TestSupport` — an environment failure, not a
   regression. Use `./mvnw verify -Dskip.frontend=true` for a backend-only run without Node.
2. **There is no CI.** Every test in this repository is run manually; nothing gates a commit.
