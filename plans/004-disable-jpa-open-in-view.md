# Plan 004: Disable `spring.jpa.open-in-view`

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: this repository has no commits yet. Open
> `src/main/resources/application.yaml` and confirm it still matches the
> excerpt under "Current state" below. If it doesn't, treat that as a STOP
> condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: correctness / performance
- **Planned at**: no commits yet (pre-initial-commit); working tree as of 2026-08-13

## Why this matters

Spring Boot's default for `spring.jpa.open-in-view` is `true`, and this
project has never overridden it. Confirmed live: running `./mvnw test`
against the current codebase logs this WARN on every context start:
```
JpaBaseConfiguration$JpaWebConfiguration : spring.jpa.open-in-view is
enabled by default. Therefore, database queries may be performed during
view rendering. Explicitly configure spring.jpa.open-in-view to disable
this warning
```
Open Session/EntityManager In View keeps a JDBC connection checked out of
the pool for the entire duration of request handling (not just the
transactional service call), and it silently allows lazy-loading queries to
fire outside the original transaction, during view rendering — a classic
source of hidden N+1 queries. This sits directly against this project's own
stated performance discipline (`CLAUDE.md`'s concurrency section: HikariCP
sized for virtual threads with a *low* `connectionTimeout`, and "backpressure
and limits at every edge... never OOM or unbounded wait"). With many virtual
threads doing I/O concurrently, connections held open for longer than
necessary compound pool pressure exactly where the project has committed to
being disciplined. There's no reason to keep the default: this codebase
doesn't yet have any view-layer (Thymeleaf/JSP) rendering that OSIV exists to
support in the first place.

## Current state

`src/main/resources/application.yaml`, in full:
```yaml
spring:
  application:
    name: mohs
```

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Test (also boots the Spring context, which is where the WARN appears) | `./mvnw test` | `BUILD SUCCESS`; the `open-in-view` WARN line no longer appears anywhere in the output |

## Scope

**In scope**:
- `src/main/resources/application.yaml` — add `spring.jpa.open-in-view: false`.

**Out of scope** (do NOT touch):
- Any other `application.yaml` key — this plan is scoped to this one
  setting only.
- No profile-specific override files exist yet (`application-*.yaml`); do
  not create any as part of this plan.

## Git workflow

This repository has no commits yet (or follows whatever convention prior
plans' commits established, if the operator committed them). Do not commit
unless the operator explicitly asks you to.

## Steps

### Step 1: Disable open-in-view

Edit `src/main/resources/application.yaml` to:
```yaml
spring:
  application:
    name: mohs
  jpa:
    open-in-view: false
```
(Note the merged `spring:` key — YAML requires one `spring:` root per file;
add `jpa:` as a sibling of `application:` under it, not a second `spring:`
block.)

**Verify**: Run `./mvnw test` and inspect the output. The line containing
`spring.jpa.open-in-view is enabled by default` must no longer appear. The
existing `contextLoads` test (and `ArchitectureTest`, if plan 002 has
already landed) must still pass — disabling OSIV changes connection-holding
behavior around view rendering, not context startup, so no test should be
affected by this change at this stage (no controllers or views exist yet to
exercise the difference).

## Test plan

No new tests are needed: there is no view-rendering code in the repository
yet for OSIV's absence to change behavior around. The verification is
negative-evidence (the WARN disappears) plus the existing test suite
continuing to pass unmodified.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `src/main/resources/application.yaml` contains
      `spring.jpa.open-in-view: false` (as a nested `jpa.open-in-view: false`
      under the single `spring:` root)
- [ ] `./mvnw test` exits 0
- [ ] The string `open-in-view is enabled by default` does not appear
      anywhere in `./mvnw test` output
- [ ] No files outside `src/main/resources/application.yaml` are modified
      (`git status`)
- [ ] `plans/README.md` status row for plan 004 updated

## STOP conditions

Stop and report back (do not improvise) if:

- `application.yaml` already has a `jpa:` key with different content by the
  time you start (someone else added JPA config) — merge carefully rather
  than overwriting, and report what was already there.
- The `open-in-view` WARN still appears after the change — this would mean
  the YAML wasn't parsed as expected (e.g. indentation/nesting mistake);
  don't suppress the warning by other means (like log-level filtering),
  fix the YAML.

## Maintenance notes

- If a future milestone (M2/M3, REST controllers) ever needs
  lazy-association access from a view/serialization layer that OSIV used to
  paper over, the correct fix is to fetch what's needed inside the
  transactional service method (or use a projection/DTO), not to re-enable
  OSIV. This is a one-way decision worth keeping.
