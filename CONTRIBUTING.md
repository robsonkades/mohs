# Contributing to Mohs

Thank you for considering a contribution. This file is the short version, kept at the root because
that is where GitHub looks for it; the long version — how to get the build running, what the modules
are, how the tests are organised — lives in
[`docs/14-development/contributing.md`](docs/14-development/contributing.md) and
[`docs/14-development/local-development.md`](docs/14-development/local-development.md).

## Before you write code

Open an issue first for anything that changes behaviour, the public API, or the database schema.
Mohs is a library other people's jobs run on, so those three carry a cost a pull request cannot
absorb on its own. Bug fixes and documentation need no preamble.

## The build

```bash
./mvnw clean verify                  # everything, the dashboard bundle included
./mvnw verify -Dskip.frontend=true   # backend only — never for a published jar
./mvnw test -pl mohs-engine -Dtest=ShardsTest
```

**Docker is required.** `mohs-store-jdbc` and `mohs-benchmark` run their tests against real
PostgreSQL, MySQL and SQL Server through Testcontainers. Without a running engine they fail on
`Could not initialize class *TestSupport` — that is the environment, not your change.

CI runs `./mvnw clean verify` on every push, without `-Dskip.frontend=true`.

## What a change is expected to carry

- **A test.** New behaviour is pinned by a test that fails without the change. A concurrency test
  synchronises with latches, `CompletableFuture` or Awaitility — never `Thread.sleep`.
- **A green suite.** Never weaken, `@Disabled` or delete a test to make a build pass. If a test
  looks wrong, say so in the pull request instead of changing it.
- **A reason.** Comments and commit messages explain *why*; the code already says what. Commit
  messages are in English, one subject per commit.
- **A number, for any performance claim.** Before and after, against
  [`docs/10-performance/benchmarks.md`](docs/10-performance/benchmarks.md). Without a measurement it
  is not an optimisation.

## House rules that will come up in review

- Every "when" comes from the injected `Clock`; every duration from `System.nanoTime`. An ArchUnit
  rule enforces the first.
- Every generated primary key is UUIDv7. No `IDENTITY`, `SERIAL`, `AUTO_INCREMENT` or `SEQUENCE`,
  on any dialect.
- Production code is `@NullMarked` (JSpecify): non-null is the default and `@Nullable` marks the
  exception — never on a local variable.
- I/O-bound work uses virtual threads; CPU-bound work uses a bounded platform pool. Concurrency is
  limited with a `Semaphore`, never through pool size.
- New prose in the code — Javadoc, comments, `package-info` — is written in English.
- Architecture decisions are recorded in
  [`docs/15-design-decisions/`](docs/15-design-decisions/README.md), and the code cites the
  *argument*, never the record's number.

## Reporting a bug

Include the dialect and database version, the Mohs version, the relevant `mohs.*` configuration, and
what you expected instead. For anything involving timing or clustering, the number of nodes matters
— say how many.

Security issues do not go in an issue: see [SECURITY.md](SECURITY.md).

## Licence

Contributions are accepted under the [Apache License 2.0](LICENSE), the licence the project is
distributed under. By opening a pull request you agree that your contribution may be distributed
under it.
