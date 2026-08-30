# Plan 002: M0 package skeleton + ArchUnit boundary tests

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `E.md`.
>
> **Drift check (run first)**: this repository has no commits yet. There is
> no SHA to diff against. Instead, confirm plan 001 has already landed —
> `../../../pom.xml` groupId must be `io.mohs` and
> `src/main/java/io/mohs/MohsApplication.java` must exist — before starting
> this plan. If plan 001 hasn't landed, STOP: this plan assumes it has.

## Status

- **Priority**: P1
- **Effort**: S–M
- **Risk**: LOW
- **Depends on**: `roupid-and-package-rename.md`
- **Category**: tech-debt / architecture
- **Planned at**: no commits yet (pre-initial-commit); working tree as of 2026-08-13, assuming plan 001 already applied

## Why this matters

`docs/MOHS-DOCUMENTO-MESTRE.md` §9, milestone **M0 — Bootstrap** (lines
580-586), is explicit about sequencing:

> "esqueleto de pacotes (`io.mohs`, `io.mohs.engine`, `io.mohs.jdbc`,
> `io.mohs.autoconfigure`, `io.mohs.rest`, `io.mohs.test`) com ArchUnit já
> testando a fronteira antes de M1 escrever o primeiro tipo público (§4).
> Sem isso, M1 não tem onde morar."

The whole single-module packaging decision (ADR 0001, §4 of the master doc)
rests on package boundaries being enforced by ArchUnit rather than by
separate Maven modules: "interno não vaza para a API; `rest` só enxerga a
API pública; `test` não vaza para produção." None of that exists yet — there
is exactly one package (`io.mohs`, containing only `MohsApplication`) and no
ArchUnit dependency at all. This plan creates the five sibling packages and
the three boundary rules the docs call for, before any real M1 types exist
to violate them — matching the documented intent exactly.

## Current state

- `../../../pom.xml` — no ArchUnit dependency present (confirmed: no
  `com.tngtech.archunit` groupId anywhere in the file).
- `src/main/java/io/mohs/MohsApplication.java` — the only class in the only
  package that currently exists.
- Design source for the required boundary rules,
  `docs/MOHS-DOCUMENTO-MESTRE.md:171-175` (§4, point 1):
  > "Fronteira por pacote, guardada por ArchUnit: `io.mohs` (API pública:
  > annotations, `Mohs`, `JobRef`, specs, eventos) · `io.mohs.engine` e
  > `io.mohs.jdbc` (internos, `@Internal`) · `io.mohs.autoconfigure` ·
  > `io.mohs.rest` · `io.mohs.test`. Regras no build: interno não vaza para
  > a API; `rest` só enxerga a API pública; `test` não vaza para produção."

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Compile | `./mvnw compile` | `BUILD SUCCESS` |
| Test (includes new ArchUnit rules) | `./mvnw test` | `BUILD SUCCESS`, 4 tests run (1 existing `contextLoads` + 3 new ArchUnit rules), 0 failures |

## Scope

**In scope** (create these files; do not create anything else):
- `../../../pom.xml` — add the ArchUnit test dependency.
- `src/main/java/io/mohs/engine/package-info.java` (new)
- `src/main/java/io/mohs/jdbc/package-info.java` (new)
- `src/main/java/io/mohs/autoconfigure/package-info.java` (new)
- `src/main/java/io/mohs/rest/package-info.java` (new)
- `src/main/java/io/mohs/test/package-info.java` (new)
- `src/test/java/io/mohs/ArchitectureTest.java` (new)

**Out of scope** (do NOT touch, even though they look related):
- Do not write any real engine, JDBC, autoconfigure, REST, or test-kit
  logic. This plan is the empty skeleton only — M1/M2/M3 (per the master
  doc's milestone plan) add actual types later.
- Do not create an `@Internal` marker annotation. The docs mention `@Internal`
  as a documentation label for `io.mohs.engine`/`io.mohs.jdbc`, but it is not
  required to make the ArchUnit package-boundary rules below work (those key
  off package names, not annotations). Adding it now would be speculative —
  defer it to M1, where the first real internal types are written and can
  decide whether an annotation earns its place. Leave a note in
  `E.md`'s dependency notes if you want it tracked.
- Do not modify `src/main/java/io/mohs/MohsApplication.java` or the test in
  `src/test/java/io/mohs/MohsApplicationTests.java`.

## Git workflow

This repository has no commits yet (or, if plan 001 was committed by the
operator, follow whatever convention that commit established). Do not
commit unless the operator explicitly asks you to.

## Steps

### Step 1: Add the ArchUnit JUnit 5 dependency

In `../../../pom.xml`, inside the existing `<dependencies>` block, add (version
confirmed current on Maven Central as of this plan's writing — if
resolution fails because a newer version exists, that's fine, bump the
version and note it in your completion report):
```xml
<dependency>
    <groupId>com.tngtech.archunit</groupId>
    <artifactId>archunit-junit5</artifactId>
    <version>1.4.1</version>
    <scope>test</scope>
</dependency>
```
Place it after the existing `spring-boot-starter-webmvc-test` dependency
(end of the dependencies list).

**Verify**: `./mvnw -q dependency:resolve` (or just proceed to Step 4's
build, which will fail fast if resolution fails).

### Step 2: Create the five package markers

Create a `package-info.java` in each of the five new packages. Each file is
a one-line Javadoc stating the package's role, matching the boundary
language from `docs/MOHS-DOCUMENTO-MESTRE.md:171-175`. Use exactly this
content per file:

`src/main/java/io/mohs/engine/package-info.java`:
```java
/**
 * Internal execution engine (claim, dispatch, retry, misfire). Not part of
 * the public API — see {@code io.mohs} for the public contracts.
 */
package io.mohs.engine;
```

`src/main/java/io/mohs/jdbc/package-info.java`:
```java
/**
 * Internal JDBC persistence for jobs, executions and queues. Not part of
 * the public API — see {@code io.mohs} for the public contracts.
 */
package io.mohs.jdbc;
```

`src/main/java/io/mohs/autoconfigure/package-info.java`:
```java
/**
 * Spring Boot auto-configuration, {@code mohs.*} properties and boot-time
 * validations.
 */
package io.mohs.autoconfigure;
```

`src/main/java/io/mohs/rest/package-info.java`:
```java
/**
 * Operational REST API. Depends only on the public API in {@code io.mohs} —
 * never directly on {@code io.mohs.engine} or {@code io.mohs.jdbc}.
 */
package io.mohs.rest;
```

`src/main/java/io/mohs/test/package-info.java`:
```java
/**
 * Test kit shipped in the main jar for consumers testing their own job
 * handlers.
 */
package io.mohs.test;
```

**Verify**: `./mvnw compile` succeeds (package-info-only packages compile
to a `package-info.class` per package with no other content).

### Step 3: Write the ArchUnit boundary test

Create `src/test/java/io/mohs/ArchitectureTest.java`:
```java
package io.mohs;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(packages = "io.mohs", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule internal_packages_do_not_leak_into_public_api =
        noClasses().that().resideInAPackage("io.mohs")
            .should().dependOnClassesThat().resideInAnyPackage("io.mohs.engine..", "io.mohs.jdbc..");

    @ArchTest
    static final ArchRule rest_only_sees_public_api =
        noClasses().that().resideInAPackage("io.mohs.rest..")
            .should().dependOnClassesThat().resideInAnyPackage("io.mohs.engine..", "io.mohs.jdbc..");

    @ArchTest
    static final ArchRule test_kit_does_not_leak_into_production =
        noClasses().that().resideOutsideOfPackage("io.mohs.test..")
            .should().dependOnClassesThat().resideInAPackage("io.mohs.test..");
}
```

These three rules are the three sentences from
`docs/MOHS-DOCUMENTO-MESTRE.md:174-175` translated directly into code:
"interno não vaza para a API" (rule 1), "`rest` só enxerga a API pública"
(rule 2), "`test` não vaza para produção" (rule 3). With only
`package-info.java` files in each package so far, all three rules pass
vacuously — that's expected and matches the documented intent ("ArchUnit já
testando a fronteira antes de M1 escrever o primeiro tipo público").

**Verify**: `./mvnw test` → `BUILD SUCCESS`, test output shows
`io.mohs.ArchitectureTest` with 3 tests run, 0 failures, alongside the
existing `io.mohs.MohsApplicationTests` (1 test) — 4 tests total, 0
failures.

## Test plan

The three `@ArchTest` rules in `ArchitectureTest.java` (Step 3) are the
tests this plan adds — there is no separate unit-test file, since ArchUnit
rules are themselves the tests. No existing test is modified.

Verification: `./mvnw test` → all 4 tests pass (1 pre-existing +
3 new).

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `../../../pom.xml` contains the `com.tngtech.archunit:archunit-junit5` test
      dependency
- [ ] The five `package-info.java` files exist at the paths listed in Scope
- [ ] `src/test/java/io/mohs/ArchitectureTest.java` exists with the three
      `@ArchTest` rules shown above
- [ ] `./mvnw compile` exits 0
- [ ] `./mvnw test` exits 0, 4 tests run, 0 failures
- [ ] No files outside the in-scope list are modified (`git status`)
- [ ] `E.md` status row for plan 002 updated

## STOP conditions

Stop and report back (do not improvise) if:

- Plan 001 has not landed (groupId is still `io.github.robsonkades` or
  `MohsApplication.java` is not at `src/main/java/io/mohs/`).
- ArchUnit version `1.4.1` fails to resolve from Maven Central and no newer
  `archunit-junit5` version is resolvable either — report the dependency
  resolution error rather than substituting an unrelated testing library.
- Any of the three ArchUnit rules fails against the empty skeleton — this
  would mean either the rule is written wrong (most likely) or something
  unexpected already exists in one of these packages; do not "fix" the
  wiring by weakening or deleting a rule, report the failure.

## Maintenance notes

- M1 (per `docs/MOHS-DOCUMENTO-MESTRE.md` §9) is the next milestone: it adds
  `JobKey`, `JobRef<T>`, `JobDefinition`/`@MohsJob`, `Schedule`,
  `Execution`/`Attempt`, `ExecutionListener`/`ExecutionInterceptor`,
  `MohsRunner`/`JobQueue`/`ExecutionWindow`, and the `Mohs` facade — all
  directly in `io.mohs`. As those land, the `internal_packages_do_not_leak_into_public_api`
  rule in `ArchitectureTest.java` becomes load-bearing for the first time
  (currently it passes vacuously). Whoever writes M1 should keep running
  `./mvnw test` after every new public type.
- Revisit the deferred `@Internal` marker annotation once `io.mohs.engine`
  or `io.mohs.jdbc` gets its first real class (M3) — decide then whether it
  earns its place over the plain package-boundary rule already in place.
- The `io.mohs.rest` boundary rule will need a companion rule once M2 adds
  REST DTOs/controllers, confirming they depend on `io.mohs` public types
  and not on JDBC/engine internals directly — the current rule already
  covers this, no change needed, just noting it as the first real test of
  that rule.
