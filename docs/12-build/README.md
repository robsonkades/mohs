# 12. Build

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [build-system.md](build-system.md) | Maven layout, why there is no `spring-boot-starter-parent`, plugins, commands, the frontend build, reproducibility, quality gates present and absent |
| [modules.md](modules.md) | What is published, JPMS naming, the parentless BOM, what a consumer declares, the transitive footprint, and what a release process would need |

## The commands you need

```bash
./mvnw clean verify                    # everything, including the frontend
./mvnw verify -Dskip.frontend=true     # backend only — never for a published jar
./mvnw test -pl mohs-engine -Dtest=ShardsTest
./mvnw -pl mohs-benchmark test -Dtest=NodeChurnScenario
```

## Three facts about this build

1. **No `spring-boot-starter-parent`.** This is a set of libraries, not an application. The BOM is
   imported instead, and the two defaults that mattered — `-parameters` and a pinned
   `maven-jar-plugin` — are declared explicitly.
2. **`LICENSE` and `NOTICE` are packaged into every jar's `META-INF`.** Apache 2.0 §4(d) applies to
   the *distributed work*, and what is distributed is the jar. `io.mohs.cron` is a derivative work of
   Spring's cron support, so this is an obligation rather than a formality.
3. **There is no CI, no coverage gate, no static analysis and no release process.** Everything is
   run by hand today.
