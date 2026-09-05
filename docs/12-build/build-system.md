# Build system

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository (`pom.xml`)

## Shape

| Aspect | Value |
| --- | --- |
| Tool | Maven, with the wrapper (`./mvnw`, `mvnw.cmd`) |
| Root | `io.github.robsonkades:mohs-parent:0.0.1-SNAPSHOT`, packaging `pom` |
| Modules | 11 |
| Java release | **25** |
| Spring Boot | 4.1.1, **imported as a BOM**, not inherited as a parent |
| Encoding | UTF-8 |
| License | Apache 2.0, declared in the POM |

## No `spring-boot-starter-parent`

A deliberate choice, stated in the POM's own comment:

> This reactor is a set of **libraries**, not an application. Importing the BOM aligns the versions
> Spring Boot manages without inheriting the starter-parent's application defaults
> (`spring-boot-maven-plugin`'s repackage, `application.yaml` filtering). The only module that *is*
> an application — `mohs-demo` — declares the repackage plugin itself.

Two consequences that had to be handled explicitly:

| Lost default | Restored by |
| --- | --- |
| `-parameters` on the compiler (Spring MVC resolves `@RequestParam`/`@PathVariable` names reflectively) | Declared in `pluginManagement` |
| A pinned `maven-jar-plugin` version | Pinned to 3.4.2 — without it, the version came from the super-POM of whatever Maven was installed, so a **published jar would change plugin depending on the machine that built it** |

## Dependency management

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-dependencies</artifactId>
      <version>4.1.1</version>
      <type>pom</type><scope>import</scope>
    </dependency>
    <!-- every io.mohs module at ${project.version} -->
    <!-- io.github.robsonkades:uuidv7:1.2.0 -->
  </dependencies>
</dependencyManagement>
```

Only two versions are pinned outside the Spring Boot BOM: `uuidv7` and the `frontend-maven-plugin`
(in `mohs-ui`).

## Dependencies every module inherits

```xml
<dependencies>
  <dependency>
    <groupId>org.jspecify</groupId>
    <artifactId>jspecify</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>
```

JSpecify is declared here once because **every production `package-info.java` carries
`@NullMarked`** and `mohs-cron` depends on no Spring artifact at all, so it cannot arrive
transitively. The test starter gives every module JUnit 5, AssertJ and Mockito.

## LICENSE and NOTICE in every jar

```xml
<resources>
  <resource><directory>src/main/resources</directory></resource>
  <resource>
    <directory>${maven.multiModuleProjectDirectory}</directory>
    <targetPath>META-INF</targetPath>
    <includes><include>LICENSE</include><include>NOTICE</include></includes>
  </resource>
</resources>
```

This is an **Apache 2.0 §4(d) obligation, not a formality**: the obligation is to include the notices
in the **distributed work**, and what is distributed is the jar, not the repository. Files at the
repository root do not satisfy it.

The reason the obligation exists at all: `io.mohs.cron` is a derivative work of
`org.springframework.scheduling.support`, and a derivative work that is redistributed carries the
notices. `NOTICE` names the adaptation and its one functional divergence from upstream. The
`<licenses>` block is also a Maven Central prerequisite.

## Plugins

| Plugin | Version | Where | Purpose |
| --- | --- | --- | --- |
| `maven-compiler-plugin` | 3.15.0 | Managed | Release 25, `-parameters` |
| `maven-surefire-plugin` | 3.5.2 | Managed | Tests. Its default include pattern is what keeps `*Scenario` classes out of the normal run |
| `maven-jar-plugin` | 3.4.2 | Managed; used by four modules | Stamps `Automatic-Module-Name` |
| `spring-boot-maven-plugin` | 4.1.1 | Managed; used by `mohs-demo` | Repackage |
| `frontend-maven-plugin` | 1.15.1 | `mohs-ui` | Installs Node v22.12.0, runs `npm ci` and `npm run build` |
| `maven-resources-plugin` | 3.3.1 | `mohs-ui` | Copies `frontend/dist` to `target/classes/mohs-ui-webapp` |
| `spring-boot-configuration-processor` | Managed | `mohs-spring-boot-starter` | IDE metadata for `mohs.*` |

## Commands

```bash
./mvnw clean verify                    # the whole reactor, from the root
./mvnw test                            # tests only
./mvnw verify -Dskip.frontend=true     # backend only: skips Node, npm ci and the bundle
./mvnw verify -pl mohs-store-jdbc      # one module
./mvnw verify -pl mohs-store-jdbc -am  # plus what it depends on
./mvnw verify -rf :mohs-rest           # resume the reactor from a module
./mvnw test -pl mohs-engine -Dtest=ShardsTest
./mvnw -pl mohs-benchmark test -Dtest=NodeChurnScenario
```

### `-Dskip.frontend=true`

Skips the entire Node toolchain, including the `dist` copy.

**Why it exists**: the Java suite does not need the bundle, and `npm ci` fails with `EPERM` while a
`npm run dev` is holding esbuild/rollup binaries in `node_modules` — without the flag, every
backend-only build would be hostage to whether the dev server happens to be running.

> **A published jar must NEVER be built with it.** `mohs-ui` would ship empty.

### Docker

`mohs-store-jdbc` and `mohs-benchmark` use Testcontainers. Without Docker, those tests fail on
`Could not initialize class *TestSupport` — an **environment** failure, not a regression.

## The frontend build

`mohs-ui` has **no Java source at all**. The chain, all in `generate-resources`:

1. `frontend-maven-plugin:install-node-and-npm` — Node v22.12.0, pinned.
2. `npm ci` — the reproducible install.
3. `npm run build` — `tsc -b && vite build`, output to `frontend/dist`.
4. `maven-resources-plugin` copies `dist` to `target/classes/mohs-ui-webapp`.

The output goes to a dedicated classpath location and **deliberately not to
`classpath:/static`**, which Spring Boot serves at `/`: the dashboard is served only under
`/mohs-ui`, so it never collides with what the host application already serves at the root.

Only `frontend/` (the source) is in git; `node/`, `node_modules/` and `dist/` are build output.

## Build reproducibility

| Property | Status |
| --- | --- |
| Plugin versions pinned | **Yes** — including `maven-jar-plugin`, specifically so a published jar does not vary by build machine |
| Node version pinned | Yes, v22.12.0 |
| npm lockfile | Yes, `package-lock.json` with `npm ci` |
| Dependency versions | Managed by the Spring Boot BOM plus three explicit pins |
| Maven Wrapper | Yes |
| `maven-enforcer-plugin` | **Not present** — no dependency-convergence check |
| Reproducible-build timestamp | Configured through `project.build.outputTimestamp` |

## Quality gates in the build

| Gate | Status |
| --- | --- |
| Compilation | Yes |
| Tests | Yes |
| Architecture rules | Partly — the ArchUnit suite went away with `mohs-demo/src/test`; three source scans in `mohs-store-jdbc` remain, as ordinary tests |
| Schema round-trip equivalence | Yes — ordinary tests in `mohs-store-jdbc` |
| Source scans | Yes |
| TypeScript type checking | Yes — `tsc -b` runs before `vite build` |
| **Code coverage** | Reported per module by JaCoCo (0.8.14) under `target/site/jacoco`; no aggregation, no threshold |
| **Static analysis** | **No** — no Checkstyle, Spotless, PMD, SpotBugs or ErrorProne |
| **Dependency vulnerability scanning** | **No** |
| **Dependency convergence** | **No enforcer** |
| **Frontend linting or tests** | **No** — `package.json` defines only `dev`, `build`, `typecheck`, `preview` |
