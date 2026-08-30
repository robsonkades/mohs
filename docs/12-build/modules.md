# Modules and publishing

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

## What is published, and what is not

| Module | Published | Notes |
| --- | --- | --- |
| `mohs-cron` | Yes | |
| `mohs-api` | Yes | Carries `module-info.java` |
| `mohs-engine` | Yes | Carries `module-info.java` |
| `mohs-store-jdbc` | Yes | `Automatic-Module-Name` only — see below |
| `mohs-rest` | Yes | `Automatic-Module-Name` only |
| `mohs-ui` | Yes | Resources only; no Java |
| `mohs-test` | Yes | `Automatic-Module-Name` only |
| `mohs-spring-boot-starter` | Yes | |
| `mohs-bom` | Yes | The bill of materials |
| `mohs-demo` | **No** | A development application |
| `mohs-benchmark` | **No** | Load and chaos harnesses |

## JPMS: two module systems, deliberately

| Module | Form | Reason |
| --- | --- | --- |
| `mohs-cron`, `mohs-api`, `mohs-engine` | Real `module-info.java` | |
| `mohs-store-jdbc`, `mohs-test`, `mohs-rest` | `Automatic-Module-Name` in the manifest | Each has a recorded reason |

`mohs-api`'s `module-info` states the point of the exercise in its own comment: the module is 100%
contract, so it exports everything — **the value is the other side**, establishing that the internal
modules stop exporting what is `public` today only because the language offered no alternative.

The three modules with a manifest entry only:

| Module | Why no `module-info` |
| --- | --- |
| `mohs-store-jdbc` | A **split package**: four engine test classes live under `io.mohs.engine` in this module's test sources, and JPMS forbids one package spanning two modules |
| `mohs-test` | The same |
| `mohs-rest` | Nothing internal to hide — it is all controllers and DTOs, and Spring must reach everything reflectively. Also, the servlet API arrives as an *optional transitive* dependency, which the compiler does not put on the module path |

In all three cases the name is **fixed in the manifest rather than derived from the jar's filename**,
so a consumer using JPMS gets a stable module name.

## The BOM has no parent, on purpose

`mohs-bom` is the only module in the reactor without `<parent>`, and that **is** the point:

> A BOM is imported with `<scope>import</scope>`, and the import resolves the **effective** model —
> so everything it inherited from `mohs-parent` would travel with it, including the parent's own
> import of `spring-boot-dependencies`. An application importing `mohs-bom` **before** Spring Boot's
> own BOM would have its versions pinned to ours, silently. A BOM manages the artifacts it names and
> nothing else.

The price is a repeated version literal: bumping the reactor's version requires touching the BOM too.

## Consuming Mohs

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.mohs</groupId>
      <artifactId>mohs-bom</artifactId>
      <version>${mohs.version}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.mohs</groupId>
    <artifactId>mohs-spring-boot-starter</artifactId>
  </dependency>

  <!-- optional: the dashboard bundle -->
  <dependency>
    <groupId>io.mohs</groupId>
    <artifactId>mohs-ui</artifactId>
  </dependency>

  <!-- optional: MutableClock / InMemoryJobStore for your own tests -->
  <dependency>
    <groupId>io.mohs</groupId>
    <artifactId>mohs-test</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>
```

The starter pulls in `mohs-engine`, `mohs-store-jdbc`, `mohs-rest` and their transitive
dependencies. **`spring-boot-starter-webmvc` is `<optional>`** in both `mohs-rest` and the starter —
the actuator pattern. An application without it gets the engine and no controllers, and
`MohsRestAutoConfiguration` never loads.

You also need a JDBC driver for your dialect; Mohs declares none at compile scope.

## The transitive footprint

What a consumer inherits from the starter:

| Group | Artifacts |
| --- | --- |
| Spring Boot | `spring-boot-starter`, `spring-boot-autoconfigure`, `spring-boot-starter-jdbc`, `spring-boot-starter-jackson` |
| Spring Framework | `spring-core`, `spring-context`, `spring-tx`, `spring-jdbc` |
| Flyway | `flyway-core` plus the postgresql / mysql / sqlserver modules |
| Metrics | `micrometer-core` |
| Logging | `slf4j-api` |
| Annotations | `jspecify` |
| Ids | `io.github.robsonkades:uuidv7` |

**No message broker client, no HTTP client, no cloud SDK, no cache client.** Verified against every
module POM.

## Release process

Version is `0.0.1-SNAPSHOT` everywhere, and nothing has been released yet — but the pipeline exists.

| Prerequisite | Status |
| --- | --- |
| `<licenses>` in the POM | Present — a Maven Central requirement |
| `LICENSE` and `NOTICE` in every jar's `META-INF` | Present |
| `<name>`, `<description>`, `<url>` | Present |
| A BOM | Present |
| Stable module names | Present |
| `<scm>`, `<developers>`, `<issueManagement>` | Present, in the parent **and** in `mohs-bom` (which has no parent to inherit from) |
| `distributionManagement` | Present — the Central Portal's snapshot repository |
| Sources and Javadoc jars | Present, attached on **every** build |
| GPG signing | Present, behind `gpg.skip` (default `true`) |
| Reproducible-build timestamp | Present — `project.build.outputTimestamp` |
| Upload | `central-publishing-maven-plugin`, `autoPublish=false` |
| CI | `.github/workflows/maven.yml`, full `verify` with the frontend |

Sources and Javadoc are attached on every build rather than behind a profile on purpose: an artifact
that only gets its sibling jars during the release run is an artifact whose release run is the first
time anyone discovers the Javadoc does not compile.

### Releasing

`.github/workflows/release.yml`, triggered manually with the version. It sets the version
(`versions:set -DprocessAllModules=true` — without that flag `mohs-bom`, which has no `<parent>`,
keeps the previous version and the published BOM points at artifacts that do not exist), builds and
tests, commits and tags, deploys signed with `-pl '!mohs-demo,!mohs-benchmark'`, and opens the GitHub
release with the jars attached.

Four repository secrets: `CENTRAL_USERNAME`, `CENTRAL_PASSWORD`, `MAVEN_GPG_PRIVATE_KEY`,
`MAVEN_GPG_PASSPHRASE`.

### What is still missing, and is not in this repository

1. The `io.mohs` namespace verified in the Central Portal, against the DNS of mohs.io. OSSRH was
   retired in June 2025, so the Portal is the only route.
2. A GPG key published to a keyserver.
3. Bumping `project.build.outputTimestamp` at each release — it is the release's date, and leaving
   it stale is the only way it can be wrong.

## Versioning policy

Not documented in the repository. What **is** documented is the **compatibility contract**, which is
the part a consumer needs:

| Element | Contract |
| --- | --- |
| `Mohs`, `MohsLifecycle`, `ScheduleCommand`, `Batch`, `BatchBuilder`, `JobContext` | **May gain methods in minor releases.** Do not implement them |
| `ExecutionListener`, `ExecutionInterceptor`, `ActorResolver` | Stable extension points |
| Sealed types | May grow freely — nothing outside can implement them, so a new method is always binary-compatible |
| The REST API | Versioned in the path (`/v1`) |
| The database schema | Forward-migrated by Flyway; `V2` explicitly preserved a column to support rollback to the previous jar |
| The shard hash | **Contract, pinned by literal values in a test.** Changing it requires a data migration |
| Metric names **and label values** | Contract — the first saved dashboard freezes that vocabulary |

One deprecation exists: `JobDefinition`'s 13-argument constructor, `@Deprecated(since = "0.1",
forRemoval = true)`.
