# Plan 003: Mark `spring-boot-starter-webmvc` as an optional dependency

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `E.md`.
>
> **Drift check (run first)**: this repository has no commits yet. Open
> `../../../pom.xml` and confirm the `spring-boot-starter-webmvc` dependency still
> looks like the excerpt under "Current state" below. If it doesn't, treat
> that as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none (independent of plans 001/002, but sequenced after
  them since they touch the same file's dependency block)
- **Category**: tech-debt / config
- **Planned at**: no commits yet (pre-initial-commit); working tree as of 2026-08-13

## Why this matters

`docs/MOHS-DOCUMENTO-MESTRE.md` §4, point 2 (lines 176-178), decided:

> "Web opcional: `spring-web` como `<optional>`; REST/dashboard ativam via
> `@ConditionalOnClass` + `mohs.api.enabled` (padrão actuator). Teste de
> contrato: app sem web no classpath sobe."

`API-DESIGN.md` repeats the same decision under "Empacotamento — módulo
único, full Spring Boot [DECIDIDO]" (point 2, around line 607): "dependências
de `spring-web` marcadas `<optional>`". Right now `../../../pom.xml` declares
`spring-boot-starter-webmvc` as a plain, non-optional dependency — the
opposite of the decided packaging model. This matters because `optional`
only has an effect on how *consumers* of the `io.mohs:mohs` artifact resolve
transitive dependencies (it does not affect this repo's own local build at
all) — so fixing it now, before there's any consumer to get it wrong for,
costs nothing and closes the gap between the decided ADR and the pom.

Note: this plan only fixes the Maven dependency declaration. The actual
runtime conditional wiring (`@ConditionalOnClass`, `mohs.api.enabled`, the
"app sem web no classpath sobe" contract test) is `io.mohs.autoconfigure`
and `io.mohs.rest` work that belongs to milestone M3, once there's an
autoconfigure module and REST controllers to gate — see Maintenance notes.

## Current state

`../../../pom.xml`, the web dependency (no optional flag):
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webmvc</artifactId>
</dependency>
```

The existing `<optional>true</optional>` pattern already used in the same
file for `spring-boot-devtools` (immediately below it), which this plan
should match:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-devtools</artifactId>
    <scope>runtime</scope>
    <optional>true</optional>
</dependency>
```

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Compile | `./mvnw compile` | `BUILD SUCCESS` |
| Test | `./mvnw test` | `BUILD SUCCESS`, same test count/result as before this change |

## Scope

**In scope**:
- `../../../pom.xml` — add `<optional>true</optional>` to the
  `spring-boot-starter-webmvc` dependency only.

**Out of scope** (do NOT touch):
- `spring-boot-starter-webmvc-test` (test-scope dependency) — leave as is;
  the docs' "web optional" decision is about the production/runtime web
  starter, not the test-scope MVC test support.
- Any `@ConditionalOnClass` / `mohs.api.enabled` wiring — that's M3
  autoconfigure work, not part of this plan.
- `spring-boot-h2console`, `spring-boot-starter-data-jpa`, and the other
  dependencies — unrelated to this finding.

## Git workflow

This repository has no commits yet (or follows whatever convention prior
plans' commits established, if the operator committed them). Do not commit
unless the operator explicitly asks you to.

## Steps

### Step 1: Mark the web starter optional

In `../../../pom.xml`, change:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webmvc</artifactId>
</dependency>
```
to:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webmvc</artifactId>
    <optional>true</optional>
</dependency>
```

**Verify**: `./mvnw compile && ./mvnw test` both succeed, with identical
test results to before this change (this app is not currently a dependency
of anything else, so `optional` has no observable effect on the local
build — the verification here is simply "nothing broke").

## Test plan

No new tests — this is a packaging-metadata-only change with no runtime
behavior difference in this repo today. The existing test suite
(`MohsApplicationTests`, and `ArchitectureTest` if plan 002 has already
landed) is the regression check.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `../../../pom.xml`'s `spring-boot-starter-webmvc` dependency has
      `<optional>true</optional>`
- [ ] `./mvnw compile` exits 0
- [ ] `./mvnw test` exits 0 with the same pass count as immediately before
      this change
- [ ] No files outside `../../../pom.xml` are modified (`git status`)
- [ ] `E.md` status row for plan 003 updated

## STOP conditions

Stop and report back (do not improvise) if:

- `../../../pom.xml` no longer contains a `spring-boot-starter-webmvc` dependency at
  all (it may have been renamed or removed by other work) — report rather
  than guessing which dependency replaced it.
- Adding `<optional>true</optional>` causes a build or test failure — this
  would be unexpected (optional has no effect on the declaring module's own
  build) and worth investigating rather than reverting silently.

## Maintenance notes

- When M3 (autoconfigure) is implemented, the actual conditional-activation
  behavior described in the ADR (`@ConditionalOnClass(DispatcherServlet.class)`-style
  gating plus `mohs.api.enabled`, defaulting to the actuator pattern) still
  needs to be built in `io.mohs.autoconfigure` and `io.mohs.rest`. This plan
  only fixes the Maven metadata half of the decision; the runtime half is
  future work, not a gap introduced by this plan.
- The "app sem web no classpath sobe" contract test mentioned in the ADR
  should be written alongside that M3 autoconfigure work, not here — there's
  no `io.mohs.rest` code yet for such a test to exercise.
