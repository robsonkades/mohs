# Security Policy

## Supported versions

Mohs has not had a release yet. Until `0.1.0` ships, the only supported version is `main`.

| Version | Supported |
| --- | --- |
| `main` | Yes |
| `0.0.1-SNAPSHOT` | Yes, as `main` |

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's private reporting — *Security* → *Report a
vulnerability* on <https://github.com/robsonkades/mohs/security/advisories/new>, which opens a
draft advisory visible only to the maintainers.

Please include the affected version or commit, the dialect and database involved if the issue
touches persistence, and the smallest reproduction you have. You will get an acknowledgement within
a few days; a fix and an advisory follow once the impact is confirmed.

## What is already known, and is not a vulnerability

Two properties of Mohs are deliberate, documented, and off by default. Reports about them will be
closed with a pointer here:

- **The operational REST API has no authentication of its own.** It is disabled by default
  (`mohs.api.enabled=false`), and enabling it logs a WARN naming exactly what it can do — cancel,
  retry and pause jobs, drain nodes, change rate limits. Authentication is the host application's:
  put a `SecurityFilterChain` in front of `/api/mohs/**` and `/mohs-ui/**`. See
  [security overview](docs/08-security/security-overview.md).
- **`Idempotency-Key` and actor attribution are declarative.** `HeaderActorResolver` records who
  *claims* to have acted; it does not authenticate them. Replace `ActorResolver` with an
  implementation backed by your security context if attribution has to be trustworthy.

A finding that the API is reachable without authentication *after the host exposed it* is a
deployment issue, not a Mohs vulnerability. A way to reach it while `mohs.api.enabled=false`, or to
bypass a `SecurityFilterChain` the host installed, is very much one.

## Scope

In scope: the published artefacts (`mohs-api`, `mohs-engine`, `mohs-store-jdbc`, `mohs-rest`,
`mohs-ui`, `mohs-test`, `mohs-spring-boot-starter`, `mohs-cron`, `mohs-bom`), the SQL they issue,
the migrations they apply, and the dashboard bundle.

Out of scope: `mohs-demo` and `mohs-benchmark`, which are never published and exist for development.
