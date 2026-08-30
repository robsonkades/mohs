# Documentation audit

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (`io.mohs:mohs-parent:0.0.1-SNAPSHOT`)

This is the self-assessment of the documentation set in `docs/`. It records what was covered, what
could not be determined, what is likely to go stale, and what would require information the
repository does not hold.

## Method

| Phase | What was done |
| --- | --- |
| Discovery | Full repository inspection: 11 module POMs, ~290 Java files, 27 SQL files, ~90 TypeScript files, 3 PowerShell scripts, the build configuration |
| Architecture reconstruction | Traced the enqueue → fire → claim → admit → dispatch → complete path end to end, plus recovery, shutdown and boot |
| Design | The structure below was derived from the actual architecture; template sections with no counterpart in this project were **not** created |
| Generation | 90 documents plus the root `README.md` |
| Cross validation | Constants, defaults, endpoint shapes, DTO fields, table columns, index definitions, metric names, test counts and log messages re-verified against source. Internal links checked mechanically |
| Quality review | Re-read as architect, developer, operator, SRE, security engineer and QA |

**Rule applied throughout**: no behaviour was asserted that could not be traced to a file. Where
something could not be determined, it is marked rather than guessed.

## Coverage

| Area | Status | Coverage | Notes |
| --- | --- | --- | --- |
| Product overview and capabilities | Complete | High | Every feature carries an implementation-status marker |
| Architecture | Complete | High | Style justified from code; all 12 fitness functions enumerated, including their declared gaps |
| Module architecture | Complete | High | All 11 modules: purpose, dependencies, consumers, JPMS form |
| Domain model | Complete | High | Two aggregates, all value objects, all 8 events, invariants with their messages |
| Execution lifecycle | Complete | **Very high** | State machine, transaction map, tick order, full failure-mode catalogue |
| Clustering and liveness | Complete | **Very high** | Lease, fence, sharding, reaper, stray-lease reconcile, detector properties |
| Functional features | Complete | High | Eight documents; every feature area has one |
| Engineering practice | Complete | High | Concurrency inventory is exhaustive (every thread, lock, atomic, confinement, race) |
| REST API | Complete | High | All 18 routes; parameters, bodies, statuses, semantics |
| Error model | Complete | High | Full exception → status mapping with rationale |
| Java API | Complete | High | Facade, chain, context, extension beans, compatibility contract |
| SSE streaming | Complete | High | Frames, cadence, conflation, structured read, shutdown ordering |
| Data model and schema | Complete | High | All 9 tables, every column, per-dialect type map |
| Indexes | Complete | **Very high** | Every index with the query it serves and the measurement behind it |
| Migrations | Complete | High | V1–V8, idempotency discipline, `V5`'s operational cost |
| Dialects | Complete | High | Tiers and every divergence |
| Data lifecycle | Complete | High | **The retention gap documented prominently** |
| Configuration | Complete | **Very high** | Every property, validated relationships, boot warnings, absent properties |
| Security | Complete | High | Trust model, exposure, hardening, input validation |
| Observability | Complete | High | Every metric and label; every message that matters; the gaps |
| Performance | Complete | Medium–High | Measured numbers with their environment caveat; structural properties separated from measurements |
| Testing | Complete | High | Inventory verified by count; the "not covered" list is explicit |
| Build | Complete | High | Including quality gates that are **absent** |
| Operations | Complete | High | Startup, shutdown, runbook, troubleshooting |
| Development | Complete | High | Prerequisites through to the end-of-change checklist |
| Design decisions | Complete | High | 16 records reconstructed from code evidence |
| Technical debt | Complete | High | 20 items, each with evidence and impact |
| **Deployment** | **Partial — by necessity** | Low | The repository contains **no** deployment artefacts. Documented as guidance for the host, with that stated plainly |
| **CI/CD** | **Not applicable** | — | **No pipeline exists.** Recorded as TD-05 rather than given a section of its own |
| **Messaging** | **Not applicable** | — | **No broker exists.** Verified against every module POM. No section created |
| **Integrations** | **Not applicable** | — | The only runtime dependency is the database, covered under data. No section created |

## Sections deliberately not created

Creating an empty section is worse than omitting it: it implies something exists.

| Template section | Why it was omitted |
| --- | --- |
| `08-messaging/` | No message broker anywhere in the reactor |
| `07-integrations/` | One external system — the database — covered under `06-data` |
| `15-deployment/` (as a full section) | No Dockerfile, manifest, chart or IaC. Folded into `13-operations/deployment.md` as host guidance |
| `16-ci-cd/` | No pipeline of any kind exists. Recorded as technical debt |
| A separate `authentication.md` / `authorization.md` | Neither exists. Both are covered — as absences — in `08-security` |
| A separate `caching.md` | There is no cache. The three bounded in-memory maps are covered where they live |
| A separate `secrets.md` | Mohs reads no secret configuration at all |

## Unknowns

Things that genuinely could not be determined from the repository:

| Unknown | What would resolve it |
| --- | --- |
| **The intended release cadence and versioning policy** | No `CHANGELOG`, no release profile, no `SemVer` statement. The *compatibility* contract **is** documented, because the code states it |
| **The date for the Portuguese → English prose migration** | Recorded in the repository as "deferred, date undefined" |
| **Whether `@OnExecution` and `retryPolicy` are planned or abandoned** | Both are published API with no implementation. The code says "not yet"; nothing says when |
| **Production performance characteristics** | Every measurement is from one local machine with Docker containers. Real hardware numbers do not exist in the repository, and none were invented |
| **Whether the SQL Server clustered-key trade is net positive** | The migration's own header states the balance has not been measured |
| **Intended cluster sizes and workload profiles** | The 64-node ceiling is documented; whether anyone approaches it is unknown |
| **Who maintains the project** | No `<developers>`, no `CODEOWNERS`, no contact information |
| **Support commitments for each database tier** | Tiers are named in code; what a tier *promises* is not written down |
| **The original rationale for a small number of micro-decisions** | Where a comment gives no reason and none is inferable, this documentation does not invent one |

## Inconsistencies found between code, tests, documentation and configuration

Reported rather than fixed, since the task was documentation. All are in
[technical debt](technical-debt.md) with evidence.

| # | Inconsistency |
| --- | --- |
| 1 | **`BatchesController` is implemented and contract-tested, but no bean registers it.** `MohsRestAutoConfiguration`'s Javadoc still claims it "remains a contract with no implementation behind it" — stale. The route does not exist at runtime (TD-01) |
| 2 | **`HistoryStore#pruneIdempotencyBefore` is described as "called by housekeeping"** and has an index added specifically for it — and has no production caller (TD-03) |
| 3 | **`OverviewStreamBroadcaster`'s Javadoc records that the overview counts stopped using `lockFreeReadHint`**, reintroducing shared locks on SQL Server without RCSI. Verified: the only caller of that method is the idle-gate probe (TD-06) |
| 4 | **`RuntimePatchResponse.BOOT_REVERSION_NOTICE` is in Portuguese** while every other user-facing API string is English — and it crosses the wire on every `PATCH` (TD-13) |
| 5 | **Mixed prose language inside single files** (e.g. `Misfire`, `MohsJob`, `RecurringJob`), against the project's own stated principle (TD-12) |
| 6 | **`NodeStore`'s Javadoc references `mohs.engine.node-heartbeat-interval`** as configuration "that does not exist yet" — it still does not; the cadence is derived from `node-lease-ttl` |
| 7 | **`DatabaseClock` documents a SQL Server correctness gap** in `database` time mode, and nothing prevents that combination (TD-14) |

## Documentation risks — what will go stale first

| Risk | Likelihood | Mitigation applied |
| --- | --- | --- |
| **Measured performance numbers** | High | Every number carries its date and an explicit non-portability caveat; structural properties are separated from measurements |
| **Configuration defaults** | Medium | The reference is a table generated by reading `MohsProperties`; verify it when that record changes |
| **Endpoint shapes** | Medium | Verified field-by-field against the DTO records; contract tests exist and would catch a change the docs would not |
| **Internal constants** (500, 1,440, 10,000, 256/5 ms, 64) | Medium | Each is named alongside its constant so a reader can grep for it |
| **Line counts and file counts** | High | Only two are quoted — `Engine.java`'s 1,768 lines and the test-file counts. Both are re-derivable in one command |
| **Log message texts** | Medium | Quoted where the wording is itself a design artefact; paraphrased elsewhere |
| **The technical-debt list** | High by design | It should shrink. Each item names the fix |
| **The `docs/old/` reference** | Low | Mentioned once, in the repository map, as provenance |

## Statistics

| Metric | Value |
| --- | --- |
| Documentation files created | **90** in `docs/`, plus the root `README.md` = **91** |
| Sections | 16, plus two cross-cutting documents |
| Decision records written | **16** (`DR-001` … `DR-016`) |
| Mermaid diagrams | **30** |
| Modules analysed | **11** |
| Java source files read or scanned | ~290 (78 API, 47 engine, 52 store, 66 REST, 20 starter, 10 cron, 6 test kit, 8 demo, 10 benchmark) |
| SQL files analysed | **27** (4 schema files + 23 migrations) |
| REST endpoints documented | **18** across 7 resource areas |
| Configuration properties documented | **23** scalar properties + 2 map families |
| Database tables documented | **9**, plus Flyway's history table |
| Indexes documented | **15** named indexes plus 9 primary keys |
| Metrics documented | **11** |
| Architecture rules documented | **12** ArchUnit rules + 4 source/schema scans |
| External integrations | **1** (the relational database) |
| Technical-debt items | **20** (1 critical, 5 high, 8 medium, 6 low) |
| Test files verified | **104** under `src/test`; 98 contain `@Test`; **711** `@Test` methods |
| Broken internal links | **0** (checked mechanically) |
| Secrets or credentials exposed | **0** |

## Quality-gate self-assessment

| Gate | Verdict | Basis |
| --- | --- | --- |
| **Accuracy** | Pass | Every behavioural claim traced to a file. Constants, defaults and DTO fields re-verified after writing |
| **Completeness** | Pass, with declared exclusions | Every module, feature, endpoint, table, property and metric is documented. Absent areas are named as absent |
| **Consistency** | Pass | One vocabulary throughout, anchored in the glossary. Terms match the code's identifiers |
| **Traceability** | Pass | Claims cite the class, file or test that supports them |
| **Maintainability** | Pass | One document per concern; the contributing guide names which document a given change must update |
| **Security** | Pass | No credential, token, password, key or connection string appears anywhere. The one dev credential pattern in the repository (`postgres/postgres` in a local run command) is a documented throwaway, and no production secret exists to expose |
| **Usability** | Pass | Role-based entry points in the portal; symptom-first troubleshooting; every section has a README |

## What could not be verified without running the system

Stated so nobody mistakes documentation for validation:

- **The test suite was not executed** as part of this work. No `.java`, `.sql` or configuration file
  was modified — only Markdown was written — so no behavioural verification was warranted, and none
  is claimed.
- **The measured numbers were not reproduced.** They are quoted from the repository's own recorded
  measurements, with their environment and date attached.
- **The chaos scenarios were not re-run.** Their pass criteria and recorded results are quoted.

## Recommended future documentation

Each of these requires information the repository does not currently hold:

| Recommendation | What is needed first |
| --- | --- |
| A capacity-planning guide | Measurements on representative production hardware |
| A migration guide from Quartz / JobRunr / db-scheduler | A mapping decision per concept; the code cites those tools but does not compare feature by feature |
| A release and versioning policy | The maintainers' intent |
| A support matrix per database tier | What a tier commits to |
| A tuning cookbook per workload archetype | Real workload profiles |
| An OpenAPI document | Either springdoc, or a hand-maintained spec — with the risk that it drifts from the controllers |
| A security-hardening checklist for regulated environments | The compliance regime in question |
| A performance-regression baseline in CI | A CI pipeline (TD-05) |
| Frontend architecture documentation | A decision about whether the dashboard is a supported extension surface or an internal artefact |

## Recommended documentation maintenance

| Trigger | Update |
| --- | --- |
| A `mohs.*` property changes | `07-configuration/configuration-reference.md` |
| An endpoint changes | `05-api/endpoints.md` — **and verify a bean registers it** |
| The schema changes | `06-data/{data-model,schema,indexes,migrations}.md` |
| A metric or label changes | `09-observability/metrics.md` — label values are contract |
| A new measurement is taken | `10-performance/performance-characteristics.md`, **with the environment stated** |
| A debt item is fixed | `technical-debt.md`, and the capability table in `01-overview/capabilities.md` |
| An architecturally significant decision is made | A new `DR-nnn` under `15-design-decisions/records/` |
