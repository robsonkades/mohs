# Mohs — Architecture Redesign Plan

**Status:** Proposal · **Author:** Principal Architect review · **Date:** 2026-08-21
**Audience:** the engineering team that will build Mohs v2
**Supersedes:** nothing yet. Every ADR referenced here is still in force until the
corresponding phase of §17 lands.

---

## 0. Executive summary

### 0.1 The one-paragraph thesis

Mohs today is a **well-executed single-table design that has reached its structural
ceiling**. The measured 4.0–4.2k exec/s on one node is not a code-quality problem —
the code is good, the indexes are measured, the ADRs are honest. The ceiling is
*physics*: one table (`mohs_executions`) is simultaneously a work queue, a lease
registry, an audit log and an idempotency index, and each of those four roles wants
opposite storage behaviour. Every additional guarantee the engine bought — per-job
concurrency caps, cluster-wide rate limits, per-execution leases — was paid for with
a **write on a hot row inside the claim transaction**. The redesign is therefore not
"more distributed". It is **less coordinated**: take state out of the hot path, split
the one hot table into four tables with four different physics, and stop paying for
guarantees nobody asked for.

### 0.2 The eight changes that matter

Ranked by measured or arithmetically-derived leverage, not by effort.

| # | Change | Kills | Expected effect |
|---|---|---|---|
| 1 | **Split the hot table by write physics** (§7) | 5 indexes + ~9 tuple versions per execution on one heap | Row versions on the big table: **~9 → 2** |
| 2 | **Node-level lease + epoch fencing** (§6.4) | per-execution `lease_expires_at` renewal | **~20,480 row-UPDATEs/s → ~0.2/s** at the current operating point |
| 3 | **Group-commit the completion, fold `markFired`** (§7.6) | 3 synchronous commits per execution | **3 → ~1.002 commits/execution**; attacks `LWLock:WALWrite`, the measured top wait |
| 4 | **Derive concurrency caps, don't count them** (§5.7) | `running_execution_count` hot row, ADR-0018/0020/0025 machinery | one hot row per capped job removed; a whole leak-repair path deleted |
| 5 | **Lease rate-limit tokens to nodes** (§9) | bucket row locked inside the claim transaction | **~33 rounds/s cluster ceiling → per-node local**; unlimited jobs stop queueing behind it |
| 6 | **Shard the ready set** (§8.3) | `SKIP LOCKED` convoy at N nodes | O(N) skip-scan → O(1); the path from ~8 to ~200 nodes |
| 7 | **Adaptive poll + `NOTIFY` wakeup** (§5.5) | 20 queries/s/node at zero load; poll-interval latency floor | idle DB cost → ~0; p50 dispatch latency **~25 ms → < 5 ms** |
| 8 | **Tier the dialects** (§7.9) | design pinned to the least-capable engine | unlocks partitioning, `NOTIFY`, `DELETE … RETURNING`; −75% tuning matrix |

### 0.3 What does *not* change

Stated up front, because a redesign document that changes everything is a redesign
document nobody can act on.

- **Mohs stays an embedded library**, not a service (§14). No control plane, no
  broker, no separate scheduler process. This is the single most important
  constraint and the redesign strengthens it.
- **At-least-once stays the delivery contract** (§6). We do not promise
  exactly-once; we make effectively-once *achievable* by the handler and cheap to
  reason about.
- **The public vocabulary survives** — `JobKey`, `Schedule`, `Execution`,
  `Attempt`, `MohsRunner`. `docs/API-DESIGN.md` earned it. What changes is what
  each concept *costs at runtime*, not what it is called.
- **The transactional-outbox property of ADR-0003 clause 4 survives** and is
  strengthened (§7.5). It is Mohs's most under-sold differentiator against Quartz,
  JobRunr and db-scheduler.
- **`Clock` injection and UUIDv7 stay.** Both invariants are correct and cheap.

---

## 1. Where the current architecture actually hurts

This section is diagnosis, not criticism. Every claim below is traceable to code in
this repository or to a number already recorded in `docs/performance/BASELINE.md`.

### 1.1 The write-amplification audit

The number that should drive the whole redesign is not throughput — it is **how many
times the database is asked to write per useful execution**. At the documented
operating point (`poll=50ms`, `batch=1000`, `dispatch-concurrency=1024`, Hikari 300,
measured 4.0–4.2k exec/s):

| Step | Statement | Rows touched | Commits |
|---|---|---|---|
| `schedule` | `INSERT INTO mohs_executions` | 1 | **1** |
| claim | `UPDATE … SET state='RUNNING', lease_expires_at, node_id` | 1 (batched 1000/txn) | 1/1000 |
| claim (capped job) | `UPDATE mohs_job_definitions SET running_execution_count = …` | 1 **hot row per job** | in-txn |
| claim (rate-limited) | `UPDATE mohs_rate_limits SET tokens …` | 1 **globally hot row** | in-txn |
| `markFired` | `UPDATE … SET fired_at` | 1 | **1** ← autocommit, its own round trip |
| lease renewal | `UPDATE … SET lease_expires_at` per in-flight row, **every tick** | up to 1024 **per 50 ms** | 1 per tick |
| completion | `UPDATE … SET state=<terminal>` + `INSERT mohs_attempts` (+ job counter, + batch counter) | 2–4 | **1** |
| retention | `DELETE mohs_attempts` + `DELETE mohs_executions` | 2 | later |

Two findings fall straight out of that table.

**Finding A — liveness costs more than the work.** `Engine.renewOwnedLeases()`
issues a JDBC batch of one `UPDATE mohs_executions` per in-flight execution on
**every tick**. At `dispatch-concurrency=1024` and `poll=50ms`, a saturated node
performs up to **20,480 row-updates per second on the hottest table in the system,
purely to say "still alive"** — while delivering 4,000 executions per second. That
is roughly **5 lease-maintenance writes per useful execution**, and each one dirties
a page in the same heap the claim query scans. This cost is not recorded in
BASELINE.md and, as far as the ADR record shows, has never been attributed.

**Finding B — the execution row is rewritten ~9 times.** `INSERT` → claim `UPDATE`
→ `markFired` `UPDATE` → ~5 lease `UPDATE`s → terminal `UPDATE`. On PostgreSQL each
is a new tuple version. Worse, `state` appears in the *predicate* of two partial
indexes (`idx_..._claim`, `idx_..._reaper`), so the claim and terminal updates
**cannot be HOT** — they churn index entries across the table's six index
structures. The partial indexes brilliantly keep the *index* small (measured −95.2%
in DBTUNE-5); they do nothing for heap bloat, and heap bloat is what the claim's
550 heap fetches per round land in.

### 1.2 Every guarantee bought a hot row

This is the structural pattern, and naming it is the most useful thing this document
can do:

| Guarantee | Implementation | Cost |
|---|---|---|
| per-job concurrency cap (ADR-0018/0020) | `running_execution_count` on `mohs_job_definitions` | one hot row **per capped job**, written on claim *and* completion, inside the claim transaction |
| cluster-wide rate limit (ADR-0042) | token bucket row in `mohs_rate_limits` | one **globally** hot row; measured ceiling **~33 claim rounds/s**, flat from 2 clients, *degrading* to 25.9 at 8 |
| execution ownership (ADR-0012) | `lease_expires_at` per execution row | Finding A |
| batch progress (ADR-0043) | counters on `mohs_batches` | one hot row per batch, on the completion path |

Three of the four are counters. Counters in a relational database are the canonical
way to convert a scalable design into a serial one. The rate-limit case is already
documented as pathological in `docs/RATE-LIMIT-EVOLUTION.md`, including the detail
that matters most:

> the node waiting on the bucket **already holds `FOR UPDATE` on up to 1000 rows of
> `mohs_executions`**. During the whole wait those executions are invisible
> (`SKIP LOCKED`) to the entire cluster — **including executions of jobs with no
> limit at all**.

A local feature (one job's rate limit) degrades a global path (everyone's claim).
That is the definition of an architectural defect, and no amount of tuning inside
the current shape removes it.

### 1.3 The tick is a serial pipeline with head-of-line blocking

`Engine.tick()` runs, on one thread, in strict order:

```
heartbeat → signalJobTimeouts → pollCancelRequests → renewOwnedLeases
          → reclaimExpired → purgeStaleNodeRows → fireDueTriggers → claimAndDispatch
```

Seven of the eight steps are I/O. They share a thread, so **claim latency is the sum
of everything upstream of it**. A slow reaper delays claiming; a large lease-renewal
batch delays the reaper; a `purgeStaleNodeRows` on a large node table delays both.
`claimAndDispatch` even carries an explicit `leaseTtl/4` budget whose only reason to
exist is that the tick might otherwise take so long that *this node's own leases
expire and another node reaps its live work*. That guard is correct, and its
necessity is the diagnosis: the design has a loop whose duration can invalidate its
own liveness claim.

The ADR record contains a second symptom of the same cause. ADR-0039 exists because
claiming outran dispatching (56,187 runner rejections and 11,666 duplicate
executions in one 50k drain). The fix — clamp the claim to dispatch headroom — is
right, but it is backpressure implemented by *arithmetic on a shared counter inside
one thread*, where a bounded queue would express it structurally.

### 1.4 Polling is paid for twice

At `poll=50ms`, every node issues ~20 claim rounds/s **whether or not there is
work**. Ten idle nodes = 200 `SELECT … FOR UPDATE SKIP LOCKED` per second against a
table with a partial index, forever. And when there *is* work, the same interval
sets a **latency floor**: an execution scheduled for now waits on average 25 ms
(uniform over the poll window) before anyone looks. The system pays for polling in
the idle case and is limited by it in the loaded case.

### 1.5 `SKIP LOCKED` does not scale to many nodes

Every node runs the *same* query, ordered by `(priority, scheduled_at)`, against the
*same* index tail. Node *k* must skip the ~(k−1)×`batch` entries locked by its
peers. At `batch=1000` and 20 nodes, the last node scans ~20,000 index entries to
find 1,000 free ones — and, worse, all 20 nodes contend for buffer locks on the same
index pages, which is not a scan-cost problem that a bigger `batch` can fix. The
project's own numbers already show the shape: MySQL at 8 concurrent nodes managed
651.9 rows/s against PostgreSQL's 3,305.0.

### 1.6 Four dialects is a design tax, not a feature

`H2`, `MySQL`, `PostgreSQL`, `SQL Server` are supported as peers. The consequences
are in the code:

- **SQL Server has no real `SKIP LOCKED`.** It uses pessimistic table hints and
  produces genuine deadlocks, which `JdbcClaimer` must catch and retry up to three
  times with jittered backoff. A correctness-relevant retry loop exists in the
  hottest path in the system *for one dialect*.
- **H2's `FOR UPDATE SKIP LOCKED` is broken.** The Javadoc of `JdbcClaimer` records
  the measurement: two raw JDBC connections racing the same row, **~33% of the time
  both obtain the lock**. The entire ADR-0018 "correctness comes from guarded CAS,
  never from the lock" design exists largely to survive this. That is a sound
  defensive decision — and it means the architecture is shaped by a database that
  should never run in production.
- Postgres-only capabilities that would each remove a subsystem — declarative
  partitioning (removes the retention delete-batcher), `LISTEN/NOTIFY` (removes the
  latency floor), `DELETE … RETURNING` (makes queue-pop a single statement) — are
  all off the table because the least-capable peer cannot follow.

### 1.7 Smaller, but real

- **`markFired` is a separate autocommit round trip** whose only product is a
  timestamp. BASELINE already lists "fusão do `markFired`" as a deferred lever.
- **Cancellation is polled** (`pollCancelRequests` every tick) — a DB query per node
  per tick to discover an event that happens a handful of times per day.
- **Batch creation is not transactional** and `docs/BATCH-ARCHITECTURE-REVIEW.md`
  item 1 calls it, correctly, *incurável*: a crash mid-loop leaves a batch that can
  never complete, and nothing detects it.
- **`BatchCompletionCallbacks` leaks in a cluster** — (N−1)/N of registered
  callbacks never fire and never get collected (item 3).
- **The reaper discards the batch-closer election** (item 2): a batch closed by the
  reclaim path emits no `BatchCompleted`, ever, with no log line.
- **Node heartbeat is decorative.** ADR-0012 says no claim/reclaim logic consults
  `mohs_nodes`. So the cluster maintains a membership table it does not use for
  membership decisions — while per-execution leases do the real liveness work at
  20,000 writes/s (Finding A). The two facts belong together: **the cheap mechanism
  is idle and the expensive one is load-bearing.**

### 1.8 What is genuinely good and must survive

A redesign that discards these would be a regression:

1. **ADR-0018's "correctness from guarded CAS, not from the lock."** Right for the
   wrong reason (H2's bug), but right. Keep it as a principle.
2. **The transactional outbox (ADR-0003 §4).** Scheduling inside the caller's
   transaction, with the same `DataSource`, is a real differentiator. Quartz,
   JobRunr and db-scheduler do not give you this cleanly.
3. **Partial indexes with predicate-implication reasoning.** DBTUNE-17 — noticing
   that the reaper's partial index was capturing the completion CAS's plan and
   fixing it with `AND lease_expires_at IS NOT NULL` — is expert-level work.
4. **Injected `Clock` + UUIDv7 + ArchUnit-enforced boundaries.**
5. **The documentation discipline itself.** `RATE-LIMIT-EVOLUTION.md`,
   `BATCH-ARCHITECTURE-REVIEW.md` and `CLAIM-GRANULARITY.md` — open items with
   *measurable triggers* — are better than most production systems have. Keep the
   practice; this document is written in the same register.

---

## 2. Assumptions worth challenging

Six load-bearing beliefs. Four should go.

### 2.1 "The execution row is the unit of storage" — **reject**

It is the unit of *identity*, not of storage. Queue membership, ownership and
history have different lifetimes, different write rates and different access
patterns. Storing them in one row forces one physical representation onto three
problems and produces every symptom in §1.1. **Decision:** split them (§7).

### 2.2 "A lease belongs to an execution" — **reject**

An execution's lease exists to answer one question: *is the process that took this
work still alive?* That question is about the **node**, not the execution. A node
with 1,024 in-flight executions does not have 1,024 independent liveness states — it
has one, and dies all at once. Per-execution leases pay 1,024× for one bit.
**Decision:** the lease belongs to the node; ownership is `(node_id, epoch)` (§6.4).

### 2.3 "A cluster-wide limit needs a cluster-wide counter on the hot path" — **reject**

It needs a cluster-wide *budget*. Budgets can be **leased in advance** and spent
locally with zero coordination, which is what every scalable rate limiter does.
ADR-0042 explicitly considered Temporal's per-worker quota and rejected it as
"approximate when nodes are unbalanced" — correct, but the alternative it chose was
measured at a ceiling **between one and two nodes**. A hard-capped token *lease*
gives Temporal's scalability *without* Temporal's approximation, because the leased
amount is deducted from the bucket up front and can never be exceeded (§9).

### 2.4 "Polling at a fixed interval is the scheduling model" — **partially reject**

Polling is right as the *durable* mechanism. It is wrong as the *only* mechanism.
The fix is standard and cheap: adaptive backoff when empty (kills idle cost) plus a
notification for the "due now" case (kills the latency floor). Poll remains the
correctness backstop; notification is a pure latency optimisation that can be lost
without harm (§5.5).

### 2.5 "Four dialects are peers" — **reject**

They are not peers in capability and pretending otherwise costs correctness (SQL
Server deadlock retries), design freedom (no partitioning, no `NOTIFY`) and four
times the tuning work — for a component whose *own* published benchmark ran on
PostgreSQL. **Decision:** tier them (§7.9).

### 2.6 "Mohs is an embedded library" — **accept, and double down**

This is the constraint that keeps Mohs honest, and the redesign should make it
stronger, not weaker. It rules out — correctly — a broker, a control plane, a
scheduler service, and every design that assumes Mohs owns its database. It is also
the strategic position: *the durable scheduler you can join to your own transaction*
is a thing Temporal structurally cannot offer.

---

## 3. Target architecture

### 3.1 The shape

```text
┌───────────────────────────────────────────────────────────────────────────┐
│  HOST APPLICATION JVM  (Spring Boot 4 · Java 25)                          │
│                                                                           │
│   application code ──▶ Mohs facade ─────┐                                 │
│   @Transactional      (mohs-api)        │  ADR-0003 §4: the enqueue joins  │
│                                         │  the caller's transaction        │
│  ┌──────────────────────────────────────▼──────────────────────────────┐  │
│  │                          mohs-engine                                │  │
│  │                                                                     │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌───────────────┐  │  │
│  │  │  TIMER     │  │  CLAIMER   │  │  DISPATCH  │  │  MAINTENANCE  │  │  │
│  │  │            │  │            │  │            │  │               │  │  │
│  │  │ materialise│  │ pop N from │  │ bounded    │  │ node lease    │  │  │
│  │  │ due        │─▶│ owned      │─▶│ queue ──▶  │  │ reaper        │  │  │
│  │  │ triggers   │  │ shards     │  │ runners    │  │ retention     │  │  │
│  │  │            │  │            │  │ (virtual   │  │ (leader only) │  │  │
│  │  │ own thread │  │ own thread │  │  threads)  │  │ own thread    │  │  │
│  │  └────────────┘  └─────┬──────┘  └─────┬──────┘  └───────────────┘  │  │
│  │                        │               │                            │  │
│  │                  ┌─────▼───────────────▼─────┐                       │  │
│  │                  │  TOKEN LEASES (local)     │  rate limits spent    │  │
│  │                  │  SLOT TABLE   (local)     │  with zero coordination│ │
│  │                  └───────────────────────────┘                       │  │
│  │                        │               │                            │  │
│  │                  ┌─────▼───────────────▼─────┐                       │  │
│  │                  │  COMMIT BATCHER            │  group-commit both    │  │
│  │                  │  (claims in / results out) │  directions           │  │
│  │                  └─────────────┬──────────────┘                      │  │
│  └────────────────────────────────┼─────────────────────────────────────┘  │
│                                   │                                        │
│  ┌────────────────────────────────▼─────────────────────────────────────┐  │
│  │                     mohs-store  (JDBC · Tier-1 = PostgreSQL)         │  │
│  └────────────────────────────────┬─────────────────────────────────────┘  │
│                                   │                                        │
│  mohs-rest  ──▶ /api/mohs  ──▶ mohs-ui (React, served from classpath)      │
└───────────────────────────────────┼────────────────────────────────────────┘
                                    │
        ┌───────────────────────────▼────────────────────────────┐
        │  HOST DATABASE   —   four tables, four physics          │
        │                                                          │
        │  mohs_ready     ▸ QUEUE     small · INSERT+DELETE · hot  │
        │  mohs_lease     ▸ OWNERSHIP tiny  · INSERT+DELETE · hot  │
        │  mohs_execution ▸ HISTORY   huge  · INSERT+1 UPDATE      │
        │                             · PARTITIONED BY TIME        │
        │  mohs_attempt   ▸ HISTORY   huge  · INSERT ONLY          │
        │                             · PARTITIONED BY TIME        │
        │  mohs_idempotency ▸ DEDUP   small · INSERT-only · pruned │
        │                             (unpartitioned — §7.2)        │
        │                                                          │
        │  mohs_job · mohs_trigger · mohs_node · mohs_rate_limit    │
        │      ▸ CONTROL   small · cached in memory · cold writes   │
        └──────────────────────────────────────────────────────────┘
```

### 3.2 The five rules the shape encodes

1. **No table serves two write profiles.** Queue churn never touches the history
   heap; history growth never touches the claim index.
2. **Nothing hot is a counter.** Concurrency caps and progress are *derived* from
   small tables; rate budgets are *leased*, not decremented in place.
3. **The hot path commits in groups, never per item.** One commit amortises a
   thousand claims or a thousand completions.
4. **Liveness is per node, not per work item**, and is expressed as a monotonic
   epoch so a returning zombie is fenced by construction.
5. **The engine is four independent loops with bounded queues between them**, not
   one serial tick. Backpressure is structural (a full queue), not arithmetic.

### 3.3 Why this is superior — stated as falsifiable claims

Each is an experiment in §16, not a belief.

| Claim | Mechanism | Falsified if |
|---|---|---|
| Row versions on the big table drop ~9 → 2 | §7 split | `pg_stat_user_tables.n_tup_upd` per execution > 3 |
| Lease-maintenance writes drop ~5/exec → ~0 | §6.4 node lease | any per-execution write on the liveness path remains |
| Commits per execution drop 3 → ~1 | §7.6 group commit | `xact_commit` delta / executions > 1.5 |
| Single-node throughput ≥ 12k exec/s | above three, combined | < 8k on the same hardware as the 4.2k run |
| Rate limits stop capping the cluster | §9 token lease | aggregate claim rounds/s still flat past 2 nodes |
| Node scaling is near-linear to 32 | §8.3 sharding | aggregate throughput sub-linear before 16 nodes |
| p50 dispatch latency < 5 ms | §5.5 notify | p50 still ≈ poll/2 under light load |

---
## 4. Domain model

### 4.1 The concepts that survive, and the ones that die

| Concept | Verdict | Reason |
|---|---|---|
| **Job** | keep | The definition. Cold, small, cached in memory, versioned. |
| **Schedule** | keep, merge into Job | A `Schedule` has no identity apart from its job. Keeping it as a separate concept in storage bought nothing. |
| **Trigger** | keep, **promote to its own row** | Today `next_fire_at` is a column on the definition, so the scheduler scan and the definition read contend on one table. A trigger is *mutable, hot, per-recurring-job state*; a definition is *cold config*. Different physics ⇒ different table. |
| **Execution** | keep | The unit of identity and of the history record. |
| **Attempt** | keep | Immutable outcome record. Append-only. |
| **Ready entry** | **new** | The queue membership of an execution. Exists only while pending. |
| **Lease** | **new (replaces columns)** | Ownership of an execution by `(node, epoch)`. Exists only while running. |
| **Node** | keep, **promote to load-bearing** | Today decorative (ADR-0012). Becomes the liveness authority and shard-assignment input. |
| **Runner** | keep | A node-local pool spec. Correct concept, under-used. |
| **Rate limit** | keep, change mechanism | Budget leased, not counter decremented. |
| **Execution window** | keep | Node-local predicate, zero storage cost. Cheapest guarantee in the system. |
| **Batch** | **demote to a tag** | See §10. A batch is not an aggregate; it is a correlation id plus a derived progress view. |
| **Worker** | **does not exist** | There is no Worker distinct from a Runner and a virtual thread. Introducing one would be ceremony. |
| **Job Instance** | **does not exist** | The prompt's "Job Instance" is our `Execution`. Two names for one thing is how schedulers get confusing. |
| **Tenant** | **new, optional** | §13. A column, not a subsystem. |

### 4.2 Aggregates

Three aggregates, chosen by *transaction boundary*, which is the only criterion that
survives contact with a hot path.

```text
┌─ JOB ────────────────────────────────────────────┐
│  Job (root)                                       │
│    ├─ Schedule       (value object, sealed)       │
│    ├─ RetryPolicy    (value object)               │
│    ├─ Limits         (value object: concurrency,  │
│    │                  timeout, runner, window,    │
│    │                  rateLimit, priority)        │
│    └─ Trigger        (separate ROW, same          │
│                       aggregate — armed/disarmed) │
│  Invariant: a job with a recurring Schedule has    │
│  exactly one armed Trigger.                        │
│  Written: registration, pause/resume, reschedule.  │
│  Never written on the hot path.                    │
└───────────────────────────────────────────────────┘

┌─ EXECUTION ──────────────────────────────────────┐
│  Execution (root)                                 │
│    ├─ Attempt*       (append-only children)       │
│    ├─ ReadyEntry?    (0..1 — exists ⟺ pending)    │
│    └─ Lease?         (0..1 — exists ⟺ running)    │
│  Invariant: ReadyEntry and Lease are mutually     │
│  exclusive. Enforced by the claim being a single  │
│  atomic DELETE-and-INSERT.                        │
│  Written: schedule, claim, complete.               │
│  THE hot path.                                     │
└───────────────────────────────────────────────────┘

┌─ NODE ───────────────────────────────────────────┐
│  Node (root): id, epoch, state, leaseExpiresAt,   │
│               shards, capacity, version           │
│  Invariant: epoch is strictly monotonic per node. │
│  Written: heartbeat (1 row / 5 s / node).          │
└───────────────────────────────────────────────────┘
```

`RateLimit` and `Runner` are deliberately **not** aggregates. `RateLimit` is a budget
with a leasing protocol; `Runner` is node-local configuration with no persistent
state. Forcing DDD ceremony onto either would add a repository and buy nothing —
exactly the "excessive DDD ceremony" the brief warns against.

### 4.3 State: one explicit machine, no implicit transitions

`ExecutionState` today has six values and the real transition rules live spread
across `Dispatcher`, `JdbcExecutionStore` and `Reaper`. Make the machine explicit
and make **every transition a named, guarded operation**.

```text
                       schedule()
                           │
                           ▼
                    ┌─────────────┐
      ┌────────────▶│   PENDING   │◀───────────────┐
      │             └──────┬──────┘                │
      │                    │ claim  (ready→lease)  │
      │                    ▼                       │
      │             ┌─────────────┐                │
      │             │   RUNNING   │                │
      │             └──┬───┬───┬──┘                │
      │      success   │   │   │   failure         │
      │        ┌───────┘   │   └───────┐           │
      │        ▼           │           ▼           │
      │  ┌───────────┐     │     ┌───────────┐     │
      │  │ SUCCEEDED │     │     │  budget?  │     │
      │  └───────────┘     │     └──┬─────┬──┘     │
      │                    │    yes │     │ no     │
      │                    │        ▼     ▼        │
      │                    │  ┌─────────┐ ┌──────┐ │
      └────────────────────┼──┤ RETRY_  │ │FAILED│ │
        backoff elapsed    │  │ WAITING │ └──────┘ │
                           │  └─────────┘          │
                           │                       │
              cancel ──────┤                       │
                           ▼                       │
                    ┌─────────────┐                │
                    │  CANCELLED  │                │
                    └─────────────┘                │
                                                   │
        node lease expired ── reaper ──────────────┘
        (treated exactly as "failure": budget decides)
```

Two deliberate simplifications versus today:

- **`RETRY_SCHEDULED` is renamed `RETRY_WAITING` and stops being claimable
  directly.** Today the claim predicate is `state IN ('ENQUEUED','RETRY_SCHEDULED')`,
  which forced a two-value `IN` into the claim predicate. On **MySQL** that cost a
  measured 3× regression (BASELINE: 3,060 → 988 rows/s; p99 45.6 → 90.8 ms — the two
  ranges of the composite index stop concatenating in `ORDER BY` order and the
  optimizer falls back to scan + sort). On **PostgreSQL** the partial index followed
  the predicate and there was **no regression** — so on Tier 1 this is a
  simplification argument, and the measured recovery applies to Tier 2 only. In the
  new model a retry simply **re-inserts a ready entry** with a future `visible_at`.
  The queue has exactly one membership rule — `visible_at <= now` — and the index
  predicate is a single equality again, on every dialect.
- **The reaper does not have a state of its own.** A node whose lease expired has its
  leases deleted and its ready entries re-inserted. Recovery and retry are the same
  code path, which is why the reaper can no longer "fail terminally without asking
  the retry budget" — the defect ADR-0026 had to work around.

### 4.4 The public API's vocabulary is preserved

`JobKey`, `JobRef`, `Schedule`/`CronSpec`/`IntervalSpec`/`OnDemandSpec`, `Misfire`,
`JobDefinition`, `JobSpec`, `Execution`, `Attempt`, `ExecutionId`, `JobContext`,
`Priority`, `ExecutionEvent`, `MohsRunner`, `RateLimit`, `ExecutionWindow` all keep
their names and their meaning. `docs/API-DESIGN.md` did this work well and the
redesign is beneath it, not across it. The breaking changes to the *public* API are
few and listed in §16.3.

---

## 5. Scheduling architecture

### 5.1 The two problems the scheduler actually solves

Conflating them is the root of most scheduler designs going wrong.

1. **Materialisation** — "the cron said 03:00; make an execution exist." Low rate
   (bounded by number of recurring jobs), needs exactly-once-ish semantics per
   occurrence, tolerant of seconds of delay.
2. **Dispatch** — "an execution is due; get it onto a thread." High rate (the
   throughput target), needs low latency, tolerant of duplicates (at-least-once).

Today one tick does both, so the cheap-and-careful problem shares a thread and a
transaction budget with the fast-and-loose one. **Decision: separate loops,
separate tables, separate tuning.**

### 5.2 Materialisation — decision: DB-backed timer with CAS advance, leader-free

`mohs_trigger` holds one row per armed recurring job: `job_key`, `next_fire_at`,
`shard`, `version`. The Timer loop:

```sql
SELECT job_key, next_fire_at, version, misfire, cron, zone, interval_spec
FROM mohs_trigger
WHERE next_fire_at <= :now AND shard = ANY(:ownedShards)
ORDER BY next_fire_at
LIMIT :fireLimit
```

(E2's multi-shard-ordering lesson was weighed here and does not apply: `mohs_trigger`
is tiny — one row per armed recurring job — and the Timer tolerates seconds; a sort
over the due slice is irrelevant at this table's scale.)

then, per trigger, one guarded advance that is **the same statement that inserts the
occurrences**, in one transaction:

```sql
UPDATE mohs_trigger
   SET next_fire_at = :computedNext, version = version + 1
 WHERE job_key = :key AND version = :observedVersion
```

Winning the CAS licenses the insert of the occurrence rows; losing it is routine
(another node got there first) and silent. This is ADR-0035's `TriggerFirer`
mechanism, kept intact — it is correct — but moved off the definitions table and
onto its own hot-ish, tiny table, and made shard-scoped so nodes do not all scan the
same rows.

**Why not leader election for materialisation?** Because CAS already gives
mutual exclusion per trigger at zero infrastructure cost, and a leader adds a
failure mode (leader death ⇒ no materialisation until re-election) that the CAS
design does not have. Leader election is reserved for exactly one thing in this
architecture — retention (§15.4) — where the work is a slow bulk operation that
genuinely should not run N times.

**Misfire** keeps ADR-0035's policies (`FIRE_NOW`, `SKIP`, `FIRE_ALL`) and its
`FiringPlanner`. That design is right and the redesign does not touch it.

### 5.3 The `mohs_ready` queue — decision: rows are *deleted* on claim, not updated

This is the single most consequential storage decision in the document.

```sql
CREATE TABLE mohs_ready (
    execution_id  UUID        NOT NULL PRIMARY KEY,
    job_key       TEXT        NOT NULL,
    shard         SMALLINT    NOT NULL,
    priority      SMALLINT    NOT NULL,
    attempt       SMALLINT    NOT NULL,   -- the attempt this entry will become
    visible_at    TIMESTAMPTZ NOT NULL,
    tenant_id     TEXT
);
CREATE INDEX ix_ready_claim ON mohs_ready (shard, priority, visible_at)
    INCLUDE (execution_id, job_key, attempt);
```

Properties that follow:

- The table's size is **the backlog**, not the history. An idle system has an empty
  table and an empty index; a system with a 50k backlog has a ~5 MB table that lives
  entirely in `shared_buffers`.
- Claim is `DELETE … RETURNING`, which on PostgreSQL is a single statement, takes
  the row lock and removes the entry in one pass — no `state` column to update, no
  index-predicate churn, no dead tuple accumulating in a heap shared with history.
- **Retry needs no new state.** A retry is an `INSERT` with `visible_at = now +
  backoff`. So is a delayed job, a rescheduled occurrence, and a reaper's
  re-queue. One mechanism, four features. The entry carries `attempt` — the
  attempt number it will become (1 on first enqueue, previous + 1 on retry or
  re-queue) — so the claim copies it straight into `mohs_lease.attempt_number`
  and nothing ever counts attempts on the hot path.
- Priority is a real global ordering *within a shard*, which is what priority means
  operationally.
- `INCLUDE (execution_id, job_key)` makes the candidate scan index-only.

**The one thing this costs:** a crash between `DELETE FROM mohs_ready` and the
lease's durability would lose the work. It cannot happen, because the delete and the
lease insert are **the same transaction** (§6.2). That is the whole reason `mohs_lease`
exists as a table rather than as columns.

### 5.4 The claim statement — decision: one statement, no join, no counter

```sql
WITH picked AS (
    SELECT execution_id, job_key, attempt
      FROM mohs_ready
     WHERE shard = :shard          -- ONE shard per statement (E2 — see below)
       AND visible_at <= :now
       AND job_key <> ALL(:inadmissible)
     ORDER BY priority, visible_at
     LIMIT :n
       FOR UPDATE SKIP LOCKED
),
gone AS (
    DELETE FROM mohs_ready r USING picked p
     WHERE r.execution_id = p.execution_id
 RETURNING r.execution_id, r.job_key, r.attempt
)
INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, claimed_at)
SELECT execution_id, job_key, :nodeId, :epoch, attempt, :now FROM gone
RETURNING execution_id, job_key;
```

**One shard per statement — measured, not stylistic.** The first draft claimed with
`shard = ANY(:ownedShards)`, and E2 retired it: with a multi-shard predicate the
`(shard, priority, visible_at)` index cannot supply the `ORDER BY` (PG 16 planner —
`= ANY` index scans do not preserve order), so every round full-scans the eligible
set and external-sorts it — 25.5 ms/round against 0.43 ms for the single-shard
probe on the same seed (EXPLAIN in BASELINE). At the proposed ownership (8/64
shards, 8 claimers) the `ANY` form measured 1.01× of the current engine before the
methodology fixes and 1.44× after — above the kill line but **~2× worse than the
single-shard form in the same cell (2.21×–2.91×)**, so it dies by dominance and
plan instability, not by the raw criterion. The claimer round-robins its owned
shards, one statement each; a full lap of empty probes ends the round ("round" =
lap from here on, §5.7). Priority stays global within a shard — the §8.3 trade
already accepted — and the starved probe (§5.6) uses this same single-shard shape.

Compare with today's claim, which is a `SELECT … JOIN mohs_job_definitions …
FOR UPDATE SKIP LOCKED`, then per-candidate window and rate filtering, then a job
counter `UPDATE` per capped candidate, then a batched `UPDATE … RETURNING`, then a
rate-limit `UPDATE`.

**What happened to the guards?** They moved to where they belong:

| Guard | Today | New |
|---|---|---|
| `j.retired = FALSE` | join column in the hot query | **retirement deletes the ready entries** — retired work cannot be in the queue |
| window exclusion | filter after the SELECT | closed-window jobs go on the **`:inadmissible` list**, computed from the in-memory job cache |
| `allow_concurrent / max_concurrent` | join + counter `UPDATE` | **derived from `mohs_lease`** (§5.7); jobs with zero headroom go on the `:inadmissible` list |
| rate limit | bucket `UPDATE` inside the txn | **locally-held token lease** (§9); jobs whose local semaphore is empty go on the `:inadmissible` list |
| handler present | claim burns a retry (`NO_HANDLER_ERROR`) | jobs this node has no handler for are always on the list (§11.1) |

The claim query no longer joins anything, no longer reads a table that other writers
are updating, and no longer writes a counter. Its one predicate beyond the queue
itself is `:inadmissible` — a small, node-local list of job keys currently excluded
(closed window, exhausted cap headroom, empty permit semaphore, no registered
handler), computed in memory before the round from the definition cache. Guards are
evaluated **per job per round**, never per candidate. Its cost is bounded by the
index scan plus two writes to two small tables.

**Admission races, named.** The exclusion list is a snapshot at round start; a guard
can flip mid-round (the last rate permit is spent while the claim statement runs).
The loser is handled at dispatch: an execution the node claimed but cannot admit has
its lease deleted and is **re-inserted into `mohs_ready`** (`visible_at = now`, or
the window's next opening for a window exclusion) in one small transaction. This
churn is bounded by one round's batch per flip, counted
(`mohs.claim.requeued{reason}`), and rare precisely because the exclusion list
already shaped the round.

**Where the payload comes from.** The claim returns identity, not payload — and the
handler needs the payload, which lives in `mohs_execution`, the partitioned history
table. The dispatcher therefore follows each round with **one batched read**
(`WHERE execution_id = ANY(:ids) AND created_at >= :lo AND created_at < :hi`), and
the pruning bounds are free: `execution_id` is UUIDv7, so `created_at` is derived
from the id itself and never travels through the queue. Carrying the payload in
`mohs_ready` instead is rejected — payloads reach 256 KB (§12.6) and would destroy
the property that the queue table lives in `shared_buffers`. This read is part of
the hot path: E2 (§20.3) measures the claim *including* it.

### 5.5 Wake-up — decision: adaptive backoff + `NOTIFY`, poll as the backstop

Three-tier wake-up:

1. **Immediate local hand-off.** An execution scheduled by this JVM with
   `visible_at <= now` is offered directly to the local dispatcher after commit
   (a post-commit hook on the caller's transaction). Zero DB round trips, zero
   latency. Covers the dominant "schedule now from a REST call" case in a
   single-node deployment.
2. **`NOTIFY` for cross-node immediacy (Tier-1 dialect only).** A commit that
   inserts a due ready entry issues `NOTIFY mohs_ready, '<shard>'`. Nodes owning
   that shard wake instantly. **Losing a notification is harmless** — tier 3 catches
   it — which is precisely why this is allowed to be a best-effort, dialect-specific
   optimisation and never a correctness dependency.
3. **Adaptive poll, always.** Interval starts at `minPoll` (default 25 ms) and
   doubles on every empty round up to `maxPoll` (default 2 s), resetting to `minPoll`
   on any non-empty round. An idle 10-node cluster settles at **5 queries/s total**
   instead of 200. A loaded cluster polls at `minPoll` and is then driven by
   backpressure, not by the clock.

**Rejected:** a pure in-memory timing wheel as the primary mechanism. A timing wheel
is the right structure for *millions of pending timers in one process* — it is what
you build when the timers are not durable. Ours are rows in a shared database and
must survive node death, so the wheel would be a cache of the database that has to
be invalidated on every cross-node change. The `visible_at` index *is* our timing
wheel, and the database maintains it for free. We keep an in-memory wheel for exactly
one job: the sub-second local hand-off in tier 1.

### 5.6 Priority — decision: keep the integer, add a starvation floor

`priority SMALLINT` leading the ready index gives strict priority within a shard.
Strict priority starves. The tempting fix — a `CASE` in the claim's `ORDER BY`
promoting anything older than `starvationAge` — does not work: an expression in
`ORDER BY` defeats the index's ordering at **plan** time, every round, whether or
not anything is starved. The planner cannot know at plan time that the promoted set
is empty.

The mechanism is a **starved probe on its own cadence** (default 1 s, not every
round). `Priority` is a small enum, so for each owned `(shard, priority)` pair the
head of `ix_ready_claim` yields the oldest `visible_at` as an index-only probe —
a handful of O(1) lookups. Only when a head exceeds `starvationAge` (default
5 min) does the node issue a targeted claim for that `(shard, priority)` slice,
ordered by `visible_at`, ahead of the normal round. In the healthy case the probes
find nothing and cost near zero — the same "cheap negative" shape BASELINE already
validated for the reaper. No new column, no new index.

The same probe, with the shard filter dropped, is the cross-shard fallback that
handler-aware claiming requires (§11.1). Dropping the shard prefix means the index
serves neither predicate nor order — a full scan, accepted deliberately: the probe
runs at 1 s cadence off the hot path, and E2's lesson (never pay a sort per claim
round) does not license "optimizing" this back into an `ANY` — the fallback's cost
budget is the cadence, not the plan shape.

### 5.7 Concurrency caps — decision: derive from `mohs_lease`, delete the counter

`mohs_lease` contains exactly the running work, cluster-wide, and is bounded by
`nodes × dispatch-concurrency` — thousands of rows, not millions. Since E2 made a
round a *lap* of single-shard statements (§5.4), "once per round" here means **once
per lap**, never per statement — headroom and the `:inadmissible` list are computed
before the lap starts, so the over-admission bound below reads `nodes × 1 lap`,
still corrected on the next lap, same error direction as before. So:

```sql
SELECT job_key, count(*) FROM mohs_lease
 WHERE job_key = ANY(:cappedJobKeys) GROUP BY job_key
```

is an index-only scan over a table that is always in cache. The claimer reads it
**once per round** (not per candidate), computes remaining headroom per capped job,
and admits accordingly. Over-admission is possible when two nodes read the same
count concurrently; it is bounded by `nodes × 1 round` and is corrected on the next
round. If a hard cap is required, the cap becomes a **slot table**:

```sql
CREATE TABLE mohs_slot (job_key TEXT, slot SMALLINT, execution_id UUID NULL,
                        PRIMARY KEY (job_key, slot));
```

— claim takes a free slot with `UPDATE … WHERE execution_id IS NULL … RETURNING`,
spreading contention across `max_concurrent` rows instead of concentrating it on one.

**Decision: ship the derived count; keep the slot table as the escape hatch behind a
measured trigger.** The derived count removes ADR-0018/0020's increment/decrement,
removes ADR-0025 entirely (the reaper no longer has a slot to release — deleting the
lease *is* releasing it), and removes a whole class of leak.

### 5.8 Recurring vs. delayed vs. on-demand — one representation

| Kind | Representation |
|---|---|
| recurring (cron/interval) | `mohs_trigger` row; Timer materialises occurrences into `mohs_ready` |
| delayed / `at` / `delay` | `mohs_ready` row with future `visible_at` |
| immediate | `mohs_ready` row with `visible_at = now` (+ local hand-off) |
| retry | `mohs_ready` row with `visible_at = now + backoff` |
| on-demand | no trigger; only ever produces ready rows on explicit `schedule()` |

One queue, one visibility rule. `RETRY_SCHEDULED` as a *state* disappears (§4.3).

### 5.9 Job dependencies — decision: do not build

Explicitly out of scope, and this is a decision, not an omission. A dependency graph
turns a scheduler into a workflow engine: you need DAG storage, cycle detection,
partial-failure propagation, fan-in barriers, and a versioning story for graphs
in flight. That is Temporal's product, and Temporal is very good at it. Mohs's
differentiator is *durable scheduling that joins your transaction*, not
orchestration. The composable primitive we *do* offer is `Batch` completion (§10),
from which a caller can chain by scheduling in the completion callback. If demand
for real DAGs appears, the answer is an integration with a workflow engine, not a
DAG inside the scheduler.

---

## 6. Execution model and delivery guarantees

### 6.1 The contract, stated precisely

> **Mohs guarantees at-least-once execution of every accepted execution, with
> at-most-one-concurrent attempt per execution under a healthy cluster, and
> at-most-`retries + 1` attempts in total unless a node dies, in which case the
> reaper's re-queue consumes budget exactly as a failure would.**

Everything in this section defends that sentence. Note what it does *not* say: it
does not say "exactly once", and it does not say attempts can never overlap. Both
would be lies, and §6.6 explains why any scheduler claiming otherwise is also lying.

### 6.2 Ownership: the claim is one transaction with three effects

```
BEGIN
  DELETE FROM mohs_ready WHERE …          -- queue membership released
  INSERT INTO mohs_lease (…, node, epoch) -- ownership taken
COMMIT
```

Because both are in one transaction, there is no interval in which an execution is
neither queued nor owned. This is the invariant of §4.2 and it is enforced by the
storage engine, not by application ordering.

The `mohs_execution` row is **not** written here. Its `state` column is advisory for
queries (§7.3) and is updated once, at terminal, in the completion batch. If a node
dies mid-flight, the row still reads `PENDING` while a lease exists — a discrepancy
that is *resolved by the lease*, which is the authority. Reads that need truth join
the lease table; reads that need speed (the dashboard) use the column and are
allowed to be a second stale. This is a deliberate, bounded, documented staleness —
not an accident.

### 6.3 Fencing: `(node_id, epoch)` and why it beats a lease timestamp

`epoch` is a monotonically increasing counter per node, incremented on **every
process start and every successful lease re-acquisition after expiry**. Every write
a node performs on work it owns is guarded:

```sql
… WHERE execution_id = :id AND node_id = :nodeId AND epoch = :epoch
```

A node that was reaped and comes back — the classic GC-pause / network-partition
zombie — carries a stale epoch and **every one of its writes fails the guard**. It
cannot corrupt the retry's record, cannot double-complete, cannot resurrect its
lease. This is Kleppmann's fencing token, implemented at the only place it can be
enforced: the storage engine.

Today's design approximates this with a lease-value CAS (`lease_expires_at =
:expectedLease`, ADR-0033's anti-ABA guard). That works but requires reasoning about
timestamp equality and ABA windows in each statement. A monotonic epoch is
strictly simpler to verify: it is a version number, and version numbers are the
textbook answer.

### 6.4 Liveness: one lease per node, not one per execution

```sql
CREATE TABLE mohs_node (
    node_id     TEXT PRIMARY KEY,
    epoch       BIGINT      NOT NULL,
    state       TEXT        NOT NULL,     -- STARTING|RUNNING|PAUSED|DRAINING|STOPPED
    expires_at  TIMESTAMPTZ NOT NULL,
    shards      INT[]       NOT NULL,
    capacity    INT         NOT NULL,
    in_flight   INT         NOT NULL,
    version     TEXT        NOT NULL,     -- build version, for rolling-update visibility
    started_at  TIMESTAMPTZ NOT NULL
);
```

Heartbeat = **one `UPDATE` per node per `heartbeatInterval` (default 5 s)**. Reaping
is then a two-statement bulk operation, not a per-row scan:

```sql
-- 1. find dead nodes (index scan over a table with as many rows as there are nodes)
SELECT node_id, epoch FROM mohs_node WHERE expires_at < :now AND state <> 'STOPPED'

-- 2. re-queue everything they owned, in one statement per dead node
WITH orphaned AS (
    DELETE FROM mohs_lease WHERE node_id = :dead AND epoch = :deadEpoch
 RETURNING execution_id, job_key, attempt_number
)
INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at, tenant_id)
SELECT o.execution_id, o.job_key, e.shard, e.priority, o.attempt_number + 1, :now, e.tenant_id
  FROM orphaned o JOIN mohs_execution e USING (execution_id)
```

**The arithmetic.** Current design at the documented operating point: up to 1,024
lease `UPDATE`s every 50 ms per node ⇒ **~20,480 writes/s/node**. New design: 1
write per 5 s per node ⇒ **0.2 writes/s/node**. That is the single largest write
reduction available anywhere in this system, and it is available because the
question being answered ("is this process alive?") was being asked once per work
item instead of once per process.

**What is lost.** Per-execution lease granularity — the ability to say "this
*execution* is stuck although its node is fine". That capability is **already
provided elsewhere and better**: `JobDefinition.timeout` (in-memory deadline,
ADR-0034) handles the stuck handler, and the Watchdog Bound handles the handler that
ignores interrupts. ADR-0012's own reasoning for a cluster-wide lease TTL ("a
cluster-wide value larger than the app's loosest timeout plus margin covers it well")
concedes the point. The per-execution lease column was paying 20,000 writes/s for a
capability the design had already decided to get elsewhere.

**Retry budget on reclaim.** The re-queue above records an attempt with outcome
`ABANDONED` in the same transaction, so budget accounting is identical to a normal
failure. ADR-0026's constraint (reaper fails terminally because retry scheduling
does not exist) is gone: retry scheduling is now just "insert a ready row".

### 6.5 Cancellation and timeout — decision: push, don't poll

Today `pollCancelRequests()` runs a query every tick on every node to discover an
event that happens a few times a day. Replace with:

- **Local fast path.** `cancel(id)` on a node that owns the execution flips the
  in-memory `CancellationSignal` immediately. The `mohs_lease` row already tells the
  API layer *which node* owns it, so this is a direct lookup.
- **Cross-node.** Set `cancel_requested` on the lease row and `NOTIFY mohs_cancel,
  '<node_id>'`. The owning node reacts in milliseconds instead of up to one poll.
- **Backstop.** The owning node re-reads its own lease rows once per heartbeat
  (5 s) — one query per node per 5 s, versus one per node per 50 ms today.
- **Not owned by anyone** (still pending): `cancel` deletes the ready entry and
  writes the terminal row. Nothing to signal.

`timeout` stays exactly as ADR-0034 designed it — in-memory deadline, cooperative
flag plus interrupt, outcome recorded when the handler actually stops. That design
is correct and cheap; do not touch it.

### 6.6 Failure analysis — the eight scenarios, answered

| # | Scenario | What happens | Guarantee held |
|---|---|---|---|
| 1 | **Node crashes before committing execution state** (after `schedule()` returned) | The `schedule()` commit is what makes the execution exist; if it did not commit, the caller's transaction did not commit either (ADR-0003 §4). Nothing is lost because nothing was promised. | atomic with caller |
| 2 | **Node crashes after committing execution state**, before claiming | Ready entry is durable; any node claims it on the next round. | at-least-once |
| 3 | **Node crashes while executing** | Lease survives; node lease expires within `heartbeatInterval × missTolerance` (default 15 s); reaper re-queues; another node runs it. The handler **may have completed its side effects**. | at-least-once; duplicate possible |
| 4 | **Database temporarily unavailable** | Claim/complete throw; the engine enters `DEGRADED`, stops claiming, keeps in-flight handlers running, and **buffers completions in memory with a bounded queue**. On reconnect, buffered completions are flushed (they are idempotent CAS writes). If the buffer fills or the outage exceeds the node lease, the node self-fences: it stops renewing, marks itself `STOPPED`, and lets the cluster reap it. **A node that cannot prove it holds its lease must stop doing work.** |  safety over liveness |
| 5 | **Network connectivity lost** (to DB) | Identical to #4. There is no other network to lose — Mohs has no node-to-node traffic. This is a *feature* of the architecture. | — |
| 6 | **Node partitioned** (DB reachable by peers, not by it) | Its lease expires; peers reap and re-run its work. When it returns, its epoch is stale and every write it attempts is fenced (§6.3). The duplicate attempt's terminal write loses; the re-run's record stands. | at-least-once + fencing |
| 7 | **Job completes but the acknowledgement is lost** | The completion transaction either committed or it did not. If it did, the lease is gone and no reaper will touch it. If it did not, the lease expires and the job re-runs. There is **no third state**, because completion is a single transaction that both deletes the lease and inserts the attempt. | at-least-once |
| 8 | **Two nodes believe they own the same execution** | Impossible for a *lease*: `mohs_lease.execution_id` is the primary key, so a second `INSERT` fails. Possible for *execution in the world* (a reaped-but-alive zombie, #6) — bounded by fencing on every write and acknowledged in the contract. | at-most-one owner; at-least-once execution |

### 6.7 What "exactly once" would actually require — and why we do not promise it

Exactly-once *execution* requires the handler's side effect and the state change to
commit atomically. Mohs cannot do that in general because the side effect is
arbitrary — an SMTP call, an S3 put, a third-party charge. What Mohs *can* and does
provide is the two halves that let a handler achieve **effectively-once**:

1. **A stable, unique attempt identity** — `(executionId, attemptNumber)` — passed
   in `JobContext`, usable as an idempotency key against the downstream system.
2. **The transactional outbox** — a handler whose side effect is a database write
   *in the host's database* can perform its write and Mohs's completion in the same
   transaction, making that specific case genuinely exactly-once.

We should say this explicitly in the docs and in the `JobContext` Javadoc, because
it is a real, defensible, differentiated guarantee, and because vagueness here is how
users end up double-charging customers.

### 6.8 Consistency model

- **Within the aggregate (execution + lease + attempt):** linearizable — every
  transition is a single-statement CAS or a single transaction, on one database.
- **Across aggregates (cluster-wide caps, rate budgets, node membership):**
  bounded-staleness eventual consistency, with the bound stated per mechanism and
  enforced by design (concurrency cap: over-admission ≤ nodes × 1 round; rate limit:
  never over-delivers, may under-deliver by the unspent lease, §9.4).
- **Read model (dashboard, `GET /executions`):** read-committed against the history
  tables with the `state` column allowed to lag the lease by at most one completion
  batch (~5 ms). Documented, not accidental.

---
## 7. Database architecture

### 7.1 Should the database do scheduling, coordination, queueing and locking at all?

The brief says not to assume the answer is yes. Answering honestly, per role:

| Role | Verdict | Reason |
|---|---|---|
| **Durable execution state** | **yes, unambiguously** | It is the only durable store Mohs is allowed to require, and it is the host's own database — which is what makes the transactional outbox possible. |
| **Work queue** | **yes, with a purpose-built table** | A relational table with `SKIP LOCKED` is a fine queue up to ~10⁴–10⁵ ops/s *provided the table is a queue and only a queue*. Today's problem is not "the DB is the queue"; it is "the queue is also the history". |
| **Coordination / membership** | **yes** | One row per node, one write per 5 s. Cheaper and simpler than any consensus system, and correct because the DB is already the consistency authority we depend on. |
| **Distributed locks** | **no — and we have none** | There is not a single `SELECT FOR UPDATE` used as a mutex in the target design. Every exclusion is a guarded CAS or a primary-key conflict. |
| **Rate budgets** | **no, not on the hot path** | Leased out to nodes; the DB holds the budget, not the accounting. §9. |
| **Timers** | **yes** | An index on `visible_at` is a durable timing wheel maintained by code that is better than anything we would write. |

So: the database stays, and the redesign's job is to stop asking it to do the one
thing it is bad at — high-frequency small updates to shared rows.

### 7.2 Schema (PostgreSQL, Tier 1 — the reference definition)

```sql
-- ─── CONTROL PLANE ──────────────────────────────────────────────────────────
-- Cold. Small. Read into memory at boot and invalidated by version bump.

CREATE TABLE mohs_job (
    job_key        TEXT        PRIMARY KEY,
    tenant_id      TEXT        NOT NULL DEFAULT '',
    name           TEXT,
    handler_type   TEXT        NOT NULL,
    schedule       JSONB       NOT NULL,   -- discriminated: CRON | INTERVAL | ON_DEMAND
    misfire        TEXT        NOT NULL,
    limits         JSONB       NOT NULL,   -- runner, window, rateLimit, maxConcurrent,
                                           -- timeout, retries, retryPolicy, priority
    source         TEXT        NOT NULL,   -- ANNOTATION | PROGRAMMATIC
    paused         BOOLEAN     NOT NULL DEFAULT FALSE,
    orphaned       BOOLEAN     NOT NULL DEFAULT FALSE,
    retired        BOOLEAN     NOT NULL DEFAULT FALSE,
    definition_version BIGINT  NOT NULL DEFAULT 1,   -- bumps on any change → cache invalidation
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE mohs_trigger (                 -- armed recurring jobs ONLY
    job_key       TEXT        PRIMARY KEY REFERENCES mohs_job(job_key) ON DELETE CASCADE,
    shard         SMALLINT    NOT NULL,
    next_fire_at  TIMESTAMPTZ NOT NULL,
    version       BIGINT      NOT NULL
);
CREATE INDEX ix_trigger_due ON mohs_trigger (shard, next_fire_at);

CREATE TABLE mohs_node (                    -- see §6.4
    node_id TEXT PRIMARY KEY, epoch BIGINT NOT NULL, state TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL, shards INT[] NOT NULL,
    capacity INT NOT NULL, in_flight INT NOT NULL, version TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_node_expiry ON mohs_node (expires_at) WHERE state <> 'STOPPED';

CREATE TABLE mohs_rate_limit (              -- the BUDGET, not the accounting (§9)
    name        TEXT PRIMARY KEY,
    tenant_id   TEXT        NOT NULL DEFAULT '',
    max_count   INT         NOT NULL,
    window_ms   BIGINT      NOT NULL,
    tokens      INT         NOT NULL,
    refilled_at TIMESTAMPTZ NOT NULL
);

-- ─── HOT PATH ───────────────────────────────────────────────────────────────
-- Small, high-churn, cache-resident, aggressively autovacuumed.

CREATE TABLE mohs_ready (                   -- THE QUEUE. §5.3
    execution_id UUID        PRIMARY KEY,
    job_key      TEXT        NOT NULL,
    shard        SMALLINT    NOT NULL,
    priority     SMALLINT    NOT NULL,
    attempt      SMALLINT    NOT NULL,      -- the attempt this entry will become
    visible_at   TIMESTAMPTZ NOT NULL,
    tenant_id    TEXT        NOT NULL DEFAULT ''
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);
CREATE INDEX ix_ready_claim ON mohs_ready (shard, priority, visible_at)
    INCLUDE (execution_id, job_key, attempt);

CREATE TABLE mohs_lease (                   -- OWNERSHIP. §6.2
    execution_id     UUID        PRIMARY KEY,
    job_key          TEXT        NOT NULL,
    node_id          TEXT        NOT NULL,
    epoch            BIGINT      NOT NULL,
    attempt_number   SMALLINT    NOT NULL,
    claimed_at       TIMESTAMPTZ NOT NULL,
    cancel_requested BOOLEAN     NOT NULL DEFAULT FALSE
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);
CREATE INDEX ix_lease_node ON mohs_lease (node_id, epoch);   -- reaper + cancel backstop
CREATE INDEX ix_lease_job  ON mohs_lease (job_key);          -- derived concurrency cap §5.7

-- ─── HISTORY ────────────────────────────────────────────────────────────────
-- Huge, append-mostly, PARTITIONED BY TIME. Retention = DROP PARTITION.

CREATE TABLE mohs_execution (
    execution_id    UUID        NOT NULL,
    job_key         TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL DEFAULT '',
    shard           SMALLINT    NOT NULL,
    priority        SMALLINT    NOT NULL,
    state           TEXT        NOT NULL,   -- advisory read model (§6.2)
    scheduled_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    actor           TEXT        NOT NULL,
    correlation_id  TEXT,                   -- was: batch_id (§10)
    idempotency_key TEXT,
    trace_id        TEXT,                   -- §12.6 — W3C traceparent captured at enqueue
    payload         JSONB       NOT NULL,
    payload_type    TEXT        NOT NULL,
    PRIMARY KEY (created_at, execution_id)
) PARTITION BY RANGE (created_at);

CREATE TABLE mohs_attempt (
    execution_id UUID        NOT NULL,
    number       SMALLINT    NOT NULL,
    node_id      TEXT        NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ NOT NULL,
    outcome      TEXT        NOT NULL,   -- SUCCEEDED|FAILED|CANCELLED|TIMED_OUT|ABANDONED
    error_type   TEXT,
    error        TEXT,
    PRIMARY KEY (finished_at, execution_id, number)
) PARTITION BY RANGE (finished_at);

-- indexes on the history, deliberately few
CREATE INDEX ix_exec_id     ON mohs_execution (execution_id);
CREATE INDEX ix_exec_job    ON mohs_execution (job_key, created_at DESC);
CREATE INDEX ix_exec_corr   ON mohs_execution (correlation_id) WHERE correlation_id IS NOT NULL;
-- Idempotency CANNOT be a unique index here: PostgreSQL requires every unique
-- index on a partitioned table to include the partition key, and including
-- created_at would let the same key coexist in different partitions. The dedup
-- authority is a small, UNPARTITIONED table, written in the enqueue transaction;
-- its PK conflict on insert IS the dedup check:
CREATE TABLE mohs_idempotency (
    job_key         TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    execution_id    UUID        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);
-- pruned by the retention job after the idempotency window (its own, shorter
-- setting — default 24 h), which also bounds the table's size.
CREATE INDEX ix_attempt_tp  ON mohs_attempt (finished_at, outcome);  -- throughput panel
```

### 7.3 Why each physical choice

- **Partitioning by time on the two big tables.** Retention becomes
  `DROP TABLE mohs_execution_2026w32` — O(1), no delete churn, no lock escalation,
  no batch-size tuning against SQL Server's escalation threshold, no vacuum
  aftermath. This alone deletes most of ADR-0032's mechanism while keeping its
  policy. **This is the single strongest argument for tiering the dialects**, because
  it is not available in a portable form.
- **`(created_at, execution_id)` as PK.** Partition key must lead the PK. `ix_exec_id`
  restores point lookup by id. UUIDv7 keeps insert locality *within* a partition —
  the ADR-0040 invariant is preserved and, in fact, strengthened, because the
  index tail is now per-partition and small.
- **`fillfactor = 70` on the two hot tables.** Leaves in-page room so `UPDATE`s on
  `mohs_lease.cancel_requested` stay HOT. Listed in BASELINE as an unexplored lever;
  here it is applied where it can actually pay, which is a small table, not a
  100 M-row heap.
- **Aggressive autovacuum on the hot tables only.** `mohs_ready` and `mohs_lease`
  have extreme churn (every row is inserted and deleted) but tiny absolute size, so
  scale-factor-based autovacuum (the default 20% of a *large* table) is exactly wrong
  for them and exactly right for the history. Splitting the tables is what makes it
  possible to give each the right policy — a benefit that has nothing to do with
  query plans and everything to do with operability at 3 a.m.
- **`JSONB` for `schedule` and `limits`.** These are read once at boot into memory,
  never queried by their contents, and grow a field every few releases. Sixteen
  nullable columns that change shape with every feature is the *actual* Primitive
  Obsession here. `payload` likewise becomes `JSONB` — queryable when someone needs
  it, and the ADR-0011/0029 isolation of the payload mapper is unaffected.
- **`error_type` split from `error`.** Grouping failures by exception class is the
  single most-requested operational query in every scheduler, and today it requires
  parsing a text blob.
- **`mohs_idempotency` as its own, unpartitioned table.** It carries the one
  uniqueness guarantee that must span partitions, so it cannot live on the
  partitioned table (the partition key would have to join the unique index,
  destroying the semantics). The PK conflict on insert *is* the dedup check —
  the same primitive as every other exclusion in this design — and its rows
  expire on the idempotency window, not the history window, so it stays tiny.
- **`trace_id` on the execution.** §12.6.

### 7.4 The write path, side by side

| Per execution | Today | Target |
|---|---|---|
| tuple versions on the big table | ~9 | **2** (insert + terminal update) |
| non-HOT updates churning ≥ 4 indexes | 2 (claim, terminal) | **0** on the big table |
| rows touched on small hot tables | 0 | 4 (ready ins/del, lease ins/del) |
| hot-row counter updates | 2–6 | **0** |
| **synchronous commits** | **3** | **~1.002** |
| lease-maintenance writes | ~5 | **~0** |

### 7.5 Transaction boundaries — three, all named

1. **Enqueue** — `INSERT mohs_execution` + `INSERT mohs_ready` (+ `INSERT
   mohs_idempotency` when a key is present). **Joins the caller's transaction when
   one is active** (ADR-0003 §4 preserved and now *strengthened*:
   all inserts are in the same unit of work, so the "batch created with M < N
   members" failure of `BATCH-ARCHITECTURE-REVIEW` item 1 becomes structurally
   impossible). Auto-commit otherwise. This is two-to-three inserts in the
   caller's transaction where today it is one; the enqueue p99 target (§20.1)
   is set against that shape.
2. **Claim** — `DELETE mohs_ready` + `INSERT mohs_lease`, one statement (§5.4), one
   transaction, N executions.
3. **Complete** — `DELETE mohs_lease` (fenced on `node_id, epoch`) + `INSERT
   mohs_attempt` + `UPDATE mohs_execution SET state, finished_at`, one transaction,
   N executions.

Nothing else opens a transaction on the hot path. In particular the rate limiter
does not (§9), the concurrency cap does not (§5.7), and liveness does not (§6.4).
That is the entire point.

### 7.6 Group commit — the mechanism

A `CompletionBatcher` with a bounded queue and two flush triggers, whichever comes
first: **N results (default 256)** or **T elapsed (default 5 ms)**. On flush, one
transaction issues three multi-row statements via `unnest()`:

```sql
DELETE FROM mohs_lease l USING unnest(:ids::uuid[]) AS x(id)
 WHERE l.execution_id = x.id AND l.node_id = :node AND l.epoch = :epoch
RETURNING l.execution_id;                     -- fenced; tells us who we actually owned

INSERT INTO mohs_attempt (…) SELECT * FROM unnest(:cols…);

UPDATE mohs_execution e SET state = x.state, finished_at = x.finished_at
  FROM unnest(:ids, :states, :times) AS x(id, state, finished_at)
 WHERE e.execution_id = x.id AND e.created_at = x.created_at;   -- partition-pruned
```

(`created_at` in the pruning predicate is derived from the UUIDv7 `execution_id` —
§5.4 — so the batcher never has to carry it in memory from claim to completion.)

**Semantic cost, stated honestly:** the window between "handler returned" and
"result durable" grows from ~1 ms to ≤ 5 ms. A crash inside that window re-runs the
job. The contract is already at-least-once, so *the guarantee does not change* — only
the probability of a duplicate, from ~10⁻⁶ to ~5×10⁻⁶ per execution per crash. Given
that a node crash already re-runs everything in flight (up to 1,024 executions),
adding at most 256 more is not a meaningful change in exposure. **The `RETURNING`
clause is what makes this safe**: it tells us exactly which completions we still
owned, so a completion whose lease was reaped mid-batch is detected rather than
silently lost.

`flushOnEveryResult = true` is available for users who want the old behaviour, and
is the *only* configuration knob added by this section, because it trades a real,
measurable property.

### 7.7 Isolation levels

`READ COMMITTED` everywhere, explicitly set (today's DBTUNE-4 fix, kept — MySQL's
`REPEATABLE READ` default was a real divergence). Nothing in the design needs more:
every exclusion is a PK conflict or a guarded CAS, both of which are atomic at
`READ COMMITTED`. **No `SERIALIZABLE`, ever** — it would convert `SKIP LOCKED`
throughput into serialization failures.

### 7.8 Connection pool

Hikari sized ≥ `claimConcurrency + completionFlushConcurrency + restConcurrency +
maintenance + headroom`. With virtual threads the pool is the *real* concurrency
limit and must be treated as such (never sized by "threads"). Concretely: two
dedicated pools —

- **`mohsEnginePool`** (default 32): claim, complete, timer, maintenance. Small and
  *fixed*, because the engine's DB concurrency is now bounded by design, not by
  in-flight handler count. Today's Hikari 300 is a symptom of per-execution writes.
- **the host's pool**: everything else, untouched.

`connectionTimeout` < 3 s, `leakDetectionThreshold` on. Splitting the pool means a
saturated host application cannot starve the engine's liveness writes — a failure
mode that today's shared pool has and nobody has noticed yet.

### 7.9 Dialect tiering — decision

| Tier | Engines | Commitment |
|---|---|---|
| **1 — reference** | PostgreSQL 14+ | Full feature set, all performance work, BASELINE runs here, partitioning + `NOTIFY` + `DELETE … RETURNING` + CTE claim. |
| **2 — supported** | SQL Server 2019+, MySQL 8.0+ | Correct and tested on the full suite. May lack partition-based retention (falls back to batched delete) and `NOTIFY` (poll only). Performance measured but not co-optimised. |
| **3 — test only** | H2 | **Not a supported production backend.** Documented as such. Used for fast unit tests and nothing else. |

Justification for demoting H2 is already in this repo: its `FOR UPDATE SKIP LOCKED`
lets two connections lock the same row ~33% of the time under contention. Shipping
a scheduler whose correctness story includes "and on this backend the lock is
decorative" is not defensible, and pretending otherwise forced the whole ADR-0018
design to be lock-independent. We *keep* the lock-independent design — it is good —
but we stop paying for H2 as a peer.

The dialect abstraction shrinks accordingly: instead of four full claim templates,
there is a `SqlDialect` with **three** capability flags (`supportsSkipLocked`,
`supportsPartitioning`, `supportsNotify`) and one fallback claim template for Tier 2
engines that lack the CTE form. The SQL Server deadlock-retry loop stays, scoped to
Tier 2, where it belongs.

---

## 8. Scalability

### 8.1 Scaling by job count

| Scheduled jobs (definitions) | Behaviour | Bottleneck |
|---|---|---|
| 10 | trivial; `mohs_trigger` fits in one page | none |
| 1,000 | in-memory definition cache ~2 MB; trigger scan is an index range | none |
| 100,000 | cache ~200 MB — **too big to hold whole**; trigger scan still an index range over 100 k rows | definition cache |
| 1,000,000 | trigger table 1 M rows; a 1-second timer tick scans only what is due | cache + timer fan-out |
| 10,000,000+ | trigger table dominates; `next_fire_at` index ~400 MB | timer materialisation rate |

**Decision at ≥ 100 k definitions:** the definition cache becomes an LRU with a
bounded size (default 10,000 entries) plus a `definition_version` check, instead of
a full map. Above ~1 M, materialisation is sharded across nodes by
`mohs_trigger.shard` (already in the schema) so each node scans only its own slice.
Both mechanisms are in the design from day one; only the LRU bound is a knob.

The design is *deliberately* insensitive to total definition count on the dispatch
path, because the claim query no longer joins `mohs_job` at all (§5.4). That is a
direct consequence of the redesign and is worth naming: **today, adding definitions
makes claiming slower; in the target, it does not.**

### 8.2 Scaling by throughput

| exec/s | Architecture | Limiting factor |
|---|---|---|
| ≤ 100 | one node, default config | nothing |
| 1,000 | one node | commit latency (was: 3 commits/exec) |
| 10,000 | one node, group commit | WAL bandwidth; handler CPU |
| 50,000 | 4–8 nodes, sharded ready set | `mohs_ready` index page contention |
| 200,000 | 16–32 nodes, sharded, `synchronous_commit` tuning | WAL fsync throughput on the primary |
| > 200,000 | **the database is the wall** — see §8.5 | single-writer PostgreSQL |

### 8.3 Sharding — the mechanism that unlocks node count

`shard SMALLINT` on `mohs_ready` and `mohs_trigger`, assigned at insert as
`hash(execution_id) % SHARD_COUNT` with `SHARD_COUNT = 64` fixed (not configurable —
64 divides evenly into every plausible node count and 64 shards on one node costs
nothing).

Assignment is **derived, not negotiated**: each node reads `mohs_node` at heartbeat,
sorts live node ids, takes its index `i` of `n`, and owns shards
`{ s : s mod n == i }`. No coordination protocol, no leader, no lock. Two nodes may
briefly disagree during membership change — which is harmless, because overlapping
ownership degrades exactly to today's behaviour (`SKIP LOCKED` sorts it out) and
resolves within one heartbeat.

**Why hash on `execution_id` and not `job_key`:** hashing on `job_key` would give
per-job locality (nice for the derived concurrency count) but creates hot shards when
one job dominates the workload — which is the common case. Uniform distribution of
work beats locality here.

**What sharding costs:** priority becomes strictly global only *within* a shard.
With 64 shards and uniform hashing, a `HIGH` priority execution is at the head of
its shard's queue and is claimed by whichever node owns that shard on its next
round — so the practical latency effect is bounded by one poll interval, not by the
backlog. This is the same trade `docs/CLAIM-GRANULARITY.md` analysed for option B,
resolved in favour of "shard by work, not by runner", which avoids that document's
central objection (per-runner claim makes priority meaningless *across* runners,
which operators actually notice; per-shard does not, because shards are invisible).

Sharding also interacts with handler-aware claiming: a shard owner that lacks the
handler for some job must not strand that job's entries. §11.1 defines the
cross-shard fallback that closes this, and it is a correctness requirement of
ADR-F, not an optimisation.

### 8.4 What scales how

| Dimension | Scaling | Note |
|---|---|---|
| nodes ↑ | **linear to ~32** with sharding | without sharding, sub-linear from ~4 (measured shape: MySQL 651 rows/s at 8 clients) |
| in-flight per node ↑ | linear until DB pool or handler CPU | virtual threads make this cheap; the pool is the real limit |
| backlog size ↑ | **flat** | `mohs_ready` index is `(shard, priority, visible_at)`; claim reads the head regardless of depth |
| history size ↑ | **flat on the hot path**, log on history queries | the split is what buys this; today history size degrades claiming |
| definitions ↑ | flat on hot path, linear on timer | §8.1 |
| rate-limited jobs ↑ | **flat** | token leases are per-node-per-limit; §9 |
| capped jobs ↑ | linear in the derived-count query, but over a tiny table | one extra index-only scan per round |
| tenants ↑ | flat | a column, §13 |

### 8.5 The wall, named

Beyond roughly **200 k exec/s or 32 nodes**, the limit is the single PostgreSQL
primary's WAL fsync. The honest answer is: **that is where Mohs stops, and we say
so.** The alternatives are (a) tell the user to shard their database — which we
support trivially, because Mohs is embedded and each host instance already points at
its own datasource; or (b) become a distributed log, which is a different product.
Documenting a ceiling with a number is worth more than an aspiration without one.

---

## 9. Rate limiting

### 9.1 The defect being fixed

Measured, from `docs/RATE-LIMIT-EVOLUTION.md`: aggregate claim rounds **freeze at
~33/s from two clients and degrade to 25.9/s at eight**, with round latency reaching
309 ms — six times the poll interval. And the node waiting on the bucket holds
`FOR UPDATE` on up to 1,000 execution rows, hiding them from the whole cluster,
*including rows belonging to jobs with no rate limit*.

That is not a tuning problem. A design in which the hot path's transaction contains
a globally-shared row lock has a cluster-wide ceiling of `1 / lockHoldTime`, and no
amount of moving the lock within the transaction changes the shape — the already-
applied "charge at the end" fix bought 2.3–3.5× and left the ceiling at ~90/s.

### 9.2 Decision: hard-capped token leasing

**The bucket is a budget the node borrows from, not a counter the node decrements.**

```
Node startup / every leaseRefreshInterval (default 1 s), in its OWN short transaction,
outside any claim:

    UPDATE mohs_rate_limit
       SET tokens = tokens - :take, refilled_at = <advanced by whole tokens>
     WHERE name = :name AND tokens >= :take
 RETURNING tokens;

    take = min( ceil(max * leaseRefreshInterval / window * overshootFactor),
                tokensAvailable,
                perNodeCap )
```

The node holds `take` permits in an in-memory `Semaphore`. A job whose semaphore is
empty goes on the claim's `:inadmissible` list (§5.4), so its entries are simply not
claimed and wait in `mohs_ready`. Dispatch still `tryAcquire`s one permit per
claimed execution to close the mid-round race; a claimed execution that finds no
permit is re-queued (§5.4, admission races). **Zero database interaction on the
per-execution path.**

### 9.3 Why this is strictly better than both alternatives

| | shared bucket (today) | Temporal per-worker quota | **token lease (ours)** |
|---|---|---|---|
| hot-path DB writes | 1 per claim round, in-txn | 0 | **0** |
| cluster ceiling | ~33 rounds/s (measured) | none | **none** |
| can over-deliver? | no | **yes** (quota is `max/n`, so a node with spare quota does not stop a busy one from... — no: it under-delivers; but rebalancing lag can over-deliver on scale-down) | **no — mathematically impossible**: a node cannot spend what it did not first deduct from the bucket |
| can under-deliver? | no | yes, when nodes are unbalanced | yes, bounded by unspent leases; mitigated by short lease + return-on-idle |
| affects unrelated jobs | **yes** (the documented defect) | no | **no** |

The critical property: **`tokens` is decremented *before* the permits exist**, so the
sum of all permits held by all nodes plus all permits still in the bucket is always
≤ `max`. Over-delivery is impossible by construction, which is the *only* guarantee a
rate limit exists to provide.

### 9.4 Under-delivery, bounded and mitigated

A node that leases 50 permits and then goes idle strands them for up to
`leaseRefreshInterval`. Mitigations, in order:

1. **Short lease window** (1 s default) — strands at most 1 s of budget.
2. **Return on idle**: a node whose claim round comes back empty returns unspent
   permits in its next heartbeat's transaction — capped, because the bucket may
   have refilled while the lease was out:
   `tokens = LEAST(max_count, tokens + :unspent)`. Without the cap, return plus
   refill could push the next window above `max` — over-delivery through the back
   door, contradicting §9.3. E4 exercises this path explicitly.
3. **Demand-proportional leasing**: `take` is scaled by the node's recent
   consumption rate, so idle nodes converge to leasing ~0.

Worst case, stated in the docs: with `n` nodes and a 1 s lease, effective throughput
is ≥ `max × (1 − n × leaseWindow / window)`. For `max=100/min` and 10 nodes, that is
≥ 83% of nominal in the pathological case and ~100% in steady state. **We trade a
bounded, documented under-delivery for the removal of a cluster-wide ceiling** —
and under-delivery is the safe direction for a rate limit, which is exactly why it
is acceptable and over-delivery would not be.

### 9.5 The limit taxonomy

| Scope | Mechanism | Where enforced |
|---|---|---|
| **global throughput** (per named limit) | token lease | before dispatch, node-local |
| **per-job throughput** | a named limit bound to one job (today's model, kept) | same |
| **per-tenant throughput** | a named limit with `tenant_id`; auto-created per tenant | same |
| **per-key throughput** (e.g. per customer) | `rateLimitKey` derived from payload → in-memory bucket per key, node-local, *not* cluster-wide | dispatch |
| **concurrency (not throughput)** | derived count over `mohs_lease` (§5.7) | claim |
| **node-local concurrency** | `MohsRunner.maxConcurrent` semaphore | runner |

**Decision on where enforcement happens: at claim/dispatch, never at admission.**
ADR-0003 §5 got this right and it does not change. Admission that waits on capacity
turns a scheduler into a synchronous queue and destroys the p99 of the enqueue path.

**Fairness.** Within a limit, permits are handed out in claim order, which is
priority order within a shard. Across tenants sharing a limit, interleaving is
deferred until multi-tenancy is actually used (§13), with the trigger stated
there — and when it lands, E2's lesson applies: a window function
(`ROW_NUMBER() OVER (PARTITION BY tenant_id …)`) inside the claim scan defeats
the index order exactly like the retired `ANY` form did. Interleave must come
from per-tenant admission across rounds, never from a sort in the hot scan.

---

## 10. Batch

### 10.1 Diagnosis

`docs/BATCH-ARCHITECTURE-REVIEW.md` documents four defects, two of them *incurable
by design*: non-transactional creation with no reconciliation (item 1) and the
reaper silently discarding the closer election (item 2), plus a cluster-wide callback
leak (item 3) and a lock-ordering deadlock class (item 4). Four serious defects in
one small feature is a signal that the *concept* is wrong, not the implementation.

The wrongness: **`Batch` was modelled as an aggregate with mutable counters when it
is a correlation group with a derived progress view.** Counters forced a hot row, a
closer election, an ordering constraint, and a callback registry.

### 10.2 Decision: a batch is a correlation id plus a derived view

- `mohs_batches` **is deleted**. `batch_id` becomes `correlation_id` on
  `mohs_execution` — a plain, indexed, nullable column (§7.2). Any set of executions
  can share one; there is no separate row, no FK, no counter, no ordering constraint,
  and therefore **no lock-ordering deadlock (item 4 dissolves)**.
- Creation atomicity (**item 1 dissolves**): `Mohs.batch(...)` inserts all members in
  **one transaction** — which is now natural, because §7.5 already made enqueue a
  single unit of work joining the caller's transaction. `total` is not a stored
  number that can disagree with reality; it is `count(*)`.
- Progress is a query, not state:

```sql
SELECT count(*) FILTER (WHERE state = 'SUCCEEDED')  AS succeeded,
       count(*) FILTER (WHERE state = 'FAILED')     AS failed,
       count(*) FILTER (WHERE state = 'CANCELLED')  AS cancelled,
       count(*)                                     AS total
  FROM mohs_execution WHERE correlation_id = :id
```

  over `ix_exec_corr`. For a 1,000-member batch this is an index scan of 1,000
  entries — sub-millisecond, and it is *always right*, which the counter never was.

- Completion detection (**item 2 dissolves**): the completion batcher, after each
  flush, checks whether any `correlation_id` in the flush is now complete
  (`count(*) FILTER (WHERE state NOT IN terminal) = 0`) and, if so, performs a
  one-shot `INSERT INTO mohs_batch_closed (correlation_id) ON CONFLICT DO NOTHING`.
  **The insert conflict *is* the election** — exactly one node in the entire cluster
  wins, deterministically, whether the closing completion came from a dispatcher or
  a reaper. No discarded return value, no path that can silently lose the event.
- Callbacks (**item 3 dissolves**): `onCompletion` is no longer an in-memory map.
  It registers a follow-up job, scheduled by whoever wins the election insert. It
  therefore survives node death, does not leak across a cluster, and cannot be
  dropped by a saturated event executor. If the caller wants an in-process callback,
  they subscribe to the `BatchCompleted` event as today — but the *durable*
  mechanism is now the default and the in-process one is explicitly documented as
  best-effort.

### 10.3 Should batch share the execution engine?

**Yes, and more so than today.** A batch member is an ordinary execution: it
retries, respects concurrency caps and rate limits, is claimable by any node, and
appears in the same history. A separate batch engine would duplicate every one of
those. What batch adds over a bare loop of `schedule()` calls is exactly three
things — atomic creation, a correlation id, and completion detection — and all
three are now one column plus one query plus one conflict-insert.

**Rejected: checkpointing, partitioning, and item-level parallelism controls.**
Those are Spring Batch's problem domain (chunk-oriented processing, `ItemReader`/
`ItemWriter`, restart-from-checkpoint) and Mohs should not grow into it. A Mohs
batch is *N independent executions that someone wants to observe together*. If a
user needs chunked, restartable, stateful item processing, the correct answer is
"run Spring Batch inside a Mohs job", and we should say so in the docs.

**Backpressure and cancellation** come free: members sit in `mohs_ready` and are
claimed at whatever rate the cluster can sustain; cancelling a batch is
`DELETE FROM mohs_ready WHERE execution_id IN (…)` plus cancel-signalling the leased
ones.

---

## 11. Node architecture

### 11.1 What a node advertises

`mohs_node` (§6.4) carries `capacity`, `in_flight`, `shards`, `version`, `state`.
That is the complete list, and each field has exactly one consumer:

| Field | Consumer |
|---|---|
| `expires_at`, `epoch` | reaper (liveness + fencing) |
| `shards` | derived shard assignment (§8.3) |
| `capacity`, `in_flight` | `GET /nodes`, dashboard, autoscaler signal |
| `state` | rolling-update visibility; `DRAINING` excludes the node from shard assignment |
| `version` | rolling-update visibility — "which build is running what" |

**Rejected: capability tags, affinity rules, CPU/memory advertisement,
capability-based routing.** Every one of these is a distributed-scheduling feature
that requires a *placement decision*, and Mohs has no placement decision to make:
work is claimed by whoever is free, which is the pull model, which is why the
system has no scheduler bottleneck. Adding affinity would mean adding a matcher,
which means adding a decision point, which means adding a thing that can be wrong
and a thing that can be a bottleneck. The one legitimate use case — "this job may
only run on nodes that have the GPU/the VPN/the file mount" — is already served by
`MohsRunner`: a node that does not register the runner does not register the
handler, and does not claim work for a job it cannot run. **We should make that
explicit rather than build a tag system: the claim filters by "job keys this node
has a handler for."** One predicate, zero new concepts, and it also fixes the
rolling-update case where a node claims a job whose handler it does not yet have
(today handled by burning a retry, per `Dispatcher`'s `NO_HANDLER_ERROR` path).

**The predicate has a failure mode that must be closed: starvation by shard
ownership.** Shards are assigned by node index (§8.3), not by handler set — so an
execution of `legacy-job` can land in a shard whose exclusive owner has no handler
for it, and *nobody* claims it: the owner filters it out, and the nodes that could
run it do not own the shard. The valve is the starvation floor (§5.6) widened one
step: an entry older than `starvationAge` is claimable **cross-shard** by any node
that has its handler. In steady state (every node registers every handler) the
probe finds nothing and costs near zero; during a rolling update it *is* the drain
path, with latency bounded by `starvationAge`. Handler-aware claiming without this
fallback would be a correctness bug, not an optimisation.

### 11.2 Lifecycle

```text
 STARTING ──▶ RUNNING ⇄ PAUSED
                 │
                 ▼
             DRAINING ──▶ STOPPED
```

- **`STARTING`**: registers node row (epoch++), loads definition cache, registers
  handlers, does *not* claim. Readiness probe green only after this.
- **`RUNNING`**: claims, dispatches, heartbeats, materialises triggers for its
  shards, reaps.
- **`PAUSED`**: heartbeats, renews nothing (nothing to renew), keeps in-flight work,
  does not claim. Same as today's ADR-0007.
- **`DRAINING`**: removed from shard assignment on the next heartbeat, so peers pick
  up its shards immediately; stops claiming; waits for in-flight up to
  `gracePeriod`; signals cancellation with reason `SHUTDOWN` at the deadline.
- **`STOPPED`**: final heartbeat writes `state='STOPPED'`; peers skip it and, on
  the next reap pass, re-queue anything it left. Writing `STOPPED` explicitly is
  what makes a clean shutdown recover in **milliseconds instead of one lease TTL** —
  today's `purgeStaleNodeRows` handles the row but not the fast re-queue.

### 11.3 Rolling updates

The scenario that breaks schedulers: v2 nodes come up while v1 nodes hold work, and
a job's handler signature changed.

1. `DRAINING` publishes shard release **before** the node stops working, so there is
   no gap where nobody owns a shard.
2. **Handler-aware claiming** (§11.1): a v2 node that no longer registers
   `legacy-job` never claims it, so v1 drains it — directly for shards v1 still
   owns, and via the cross-shard valve (§11.1) for `legacy-job` entries whose
   shard owner is a v2 node, within `starvationAge`. A v1 node never claims a new
   `v2-only-job` for the same reason. **This removes the retry-burning path
   entirely** — today a claim by a node without the handler costs one attempt of the
   retry budget, which under a slow rollout can exhaust it.
3. `mohs_node.version` makes "which builds are live" a dashboard fact, so an operator
   can see a stuck rollout instead of inferring it.

### 11.4 Work reassignment

There is no reassignment protocol, and that is a feature. Work returns to
`mohs_ready` and is claimed by whoever is free. Reassignment protocols need a
decider, a handoff acknowledgement, and a story for a decider that dies mid-handoff.
Re-queueing needs none of those.

---
## 12. Modern Java, the JVM, and the concurrency model

### 12.1 The four models, decided per component

The brief asks for an explicit comparison rather than a default. Here it is, applied
to *this* system rather than in the abstract.

| Component | Model | Why |
|---|---|---|
| **Handler execution** (I/O-bound, the common case) | **virtual threads**, `Executors.newVirtualThreadPerTaskExecutor()` | Thread-per-execution with blocking JDBC/HTTP is the simplest correct model, and Loom removed its cost. 10,000 in-flight handlers ≈ 10,000 virtual threads ≈ ~10 MB of stacks, versus 10,000 platform threads ≈ 10 GB. |
| **Handler execution** (CPU-bound) | **bounded platform pool** (`MohsRunner` CPU mode, kept) | Virtual threads do not create CPU. A CPU-bound job on virtual threads just moves the queue into the carrier pool where it is invisible. An explicit bounded pool with a visible queue is *more* operable. |
| **Claim loop** | **one platform thread per node** | It must never be descheduled behind a carrier-starving task, it is latency-critical, and there is exactly one. A named platform thread also shows up correctly in every profiler and thread dump — which matters at 3 a.m. |
| **Timer loop** | one platform thread | Same reasoning; also it must be immune to dispatch saturation. |
| **Completion batcher** | one platform thread + `ArrayBlockingQueue` | The queue *is* the backpressure. A full queue blocks the completing virtual thread, which is exactly the signal we want to propagate. |
| **Maintenance** (reap, retention, node purge) | virtual threads on a scheduled executor | Rare, I/O-bound, latency-insensitive. |
| **Event publication** | virtual threads, **bounded queue** | Today it *drops* events on saturation, which for `BatchCompleted` was structurally unsafe (§10 fixes the cause). Bounded queue + drop-with-counter, and the counter is a metric. |
| **REST layer** | virtual threads (`spring.threads.virtual.enabled=true`) | Boot 4 default posture for a servlet stack. |
| **Reactive (WebFlux / R2DBC)** | **not used anywhere** | See §12.2. |

### 12.2 Why there is no reactive code in this design

Reactive programming buys one thing: high concurrency without a thread per request.
Virtual threads buy the same thing without the costs, which for a *library that
embeds in someone else's application* are decisive:

1. **It would infect the host.** A reactive Mohs would force reactive handlers, or a
   bridge on every boundary. A library must not dictate the host's concurrency model.
2. **JDBC is blocking**, and the host's `DataSource` is the thing we must join for
   the transactional outbox (ADR-0003 §4). R2DBC cannot participate in a JDBC
   transaction — adopting it would **destroy the single best property of the
   product**.
3. **Debuggability.** A stack trace through a reactive chain is unreadable; a stack
   trace through a virtual thread is a normal stack trace. Design for 3 a.m.

**The one place reactive-style would be defensible** — SSE fan-out for the dashboard —
is served fine by servlet async with virtual threads at the dashboard's actual scale
(a handful of operator browsers). Introducing WebFlux for that would be the textbook
case of a technology looking for a problem.

### 12.3 Structured concurrency

`StructuredTaskScope` (JEP 505) remains preview on JDK 25, and `--enable-preview`
pins the *host* application to an exact JDK — unacceptable for an embedded library.
CLAUDE.md already states this correctly.

**Decision: design in the structured shape now, adopt the API when it finalises.**
Concretely, that means the claim→dispatch cycle is written as one logical scope per
round with cancellation flowing downward through `JobContext.cancellationRequested()`,
implemented today with `ExecutorService` + bounded queue + `Future.get(timeout)`. The
migration is then mechanical. This is exactly the posture CLAUDE.md prescribes and
the redesign does not change it.

### 12.4 `ScopedValue` for execution context

`JobContext` (job key, execution id, attempt number, tenant, trace id) is bound as a
`ScopedValue` (final in Java 25) around the handler invocation, replacing any
`ThreadLocal` use. It inherits correctly into structured subtasks when we migrate,
and it is immutable by construction. An MDC bridge writes the same values into SLF4J
so host logging picks them up (§12.7).

### 12.5 Allocation, GC and JIT

- **Allocation on the hot path is the thing to watch**, not heap size. The claim
  round today allocates: a `Candidate` per row, a `MapSqlParameterSource` per
  statement, several intermediate `List`s per filter stage, plus a `HashMap` per
  round. At 4 k exec/s with `batch=1000` that is real garbage. Target: **one
  primitive-array-backed candidate buffer reused per round**, and `unnest()`-based
  batch statements that take arrays rather than N parameter sources. This is a
  measurable, allocation-profile-verifiable goal (§16.3), not a micro-optimisation
  belief.
- **GC: G1 by default, ZGC generational when p99 pauses matter.** Mohs is a library —
  it does not choose the host's collector. What it *must* do is not create pressure
  that forces the host to change theirs. That is an obligation, and the way to hold
  it is an allocation-rate budget in the benchmark suite.
- **JIT warm-up**: the claim path must reach C2 quickly. A cold node claiming at
  interpreter speed for its first seconds is a real, observable latency spike during
  rolling updates. Mitigation: the node stays `STARTING` (claims nothing) until the
  definition cache is warm, and AppCDS/AOT-cache guidance goes in the docs for hosts
  that care. **This is stated as a documented behaviour, not a feature to build.**

### 12.6 Serialization

`payload` is `JSONB` with an explicit `payload_type`. ADR-0011/0029's isolation of
the payload mapper from the host's `ObjectMapper` is correct and preserved — a
scheduler that breaks because the host reconfigured Jackson is a support nightmare.
**Decision: keep the isolated mapper, and add a size guard** (default 256 KB, hard
error at enqueue with an actionable message). Today an unbounded payload is a
denial-of-service against your own `mohs_execution` heap.

### 12.7 Efficient database access

No ORM anywhere on the hot path — plain JDBC with explicit SQL, as today. This is
right and needs no change. What changes:

- `unnest()`-based multi-row DML instead of `batchUpdate` with N parameter sources
  on Tier 1 (one statement, one parse, one plan, N rows).
- Prepared-statement caching enabled explicitly (`prepareThreshold`, Hikari
  `cachePrepStmts`), because at 20 rounds/s × 3 statements the parse cost is real.
- `setFetchSize` on the candidate scan so the driver does not round-trip per row.

---

## 13. Multi-tenancy

### 13.1 Decision: yes, as a column — not as a subsystem

Mohs is embedded, so "tenant" means the *host application's* tenant, and the host
already has a tenant concept. Our job is to carry it, isolate on it, and report on
it — not to invent it.

- `tenant_id TEXT NOT NULL DEFAULT ''` on `mohs_job`, `mohs_execution`,
  `mohs_ready`, `mohs_rate_limit`. Empty string = single-tenant, which is the
  default and costs nothing.
- The tenant is resolved by a `TenantResolver` SPI (one method, defaults to a
  constant) and captured **at enqueue**, never derived later.
- It flows into `JobContext` as a `ScopedValue`, into every log line, and into
  every metric as a **bounded** label (see §14.4 on cardinality).

### 13.2 Isolation, concretely

| Concern | Mechanism |
|---|---|
| **data isolation** | every REST query is filtered by the caller's tenant; a `WHERE tenant_id = :t` that the controller cannot omit because the store's API takes a `TenantId`, not a string. Optionally PostgreSQL RLS for hosts that want defence in depth. |
| **quota** | max in-flight executions per tenant, enforced as a derived count over `mohs_lease` (same mechanism as §5.7 — one query, one more `GROUP BY` key) |
| **rate** | a named rate limit per tenant (§9.5) |
| **scheduling fairness** | round-robin interleave in the ready scan via `ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY priority, visible_at)`, admitting at most `k` per tenant per round |
| **noisy neighbour** | the three above, plus per-tenant `MohsRunner` assignment when hard CPU isolation is needed |
| **observability** | tenant as a metric label and a dashboard filter |

### 13.3 What we do *not* build

Schema-per-tenant, database-per-tenant, or a tenant provisioning API. Mohs shares
the host's database by design; if the host does database-per-tenant, it instantiates
one Mohs per datasource and everything works with zero Mohs code. **Physical
isolation is the host's decision and Mohs should stay out of it.**

**Fairness interleaving is designed but not shipped in phase 1** — trigger: the
first deployment with more than one non-empty `tenant_id` and a measured
head-of-line-blocking complaint. Until then it is a documented `ORDER BY` change,
not code.

---

## 14. Architecture style, messaging, caching, observability

### 14.1 Style — decision: **in-process modular monolith, embedded as a library**

Not microservices. Not a service at all, by default.

**Why this is the right answer and not a compromise:**

1. The transactional outbox (ADR-0003 §4) is only possible in-process, sharing the
   host's `DataSource`. Extract Mohs into a service and you *lose the product's best
   property*.
2. There is no node-to-node network traffic in the entire design. The only shared
   dependency is a database the host already runs. That eliminates service discovery,
   mTLS between components, retry storms, and an entire category of partial failure.
3. Nodes are already independent and horizontally scalable — the pull model means
   adding a node adds capacity with no reconfiguration anywhere.

**The one concession:** ship an *optional* standalone runtime — the same jars with a
`main()`, a REST API, and handlers loaded over an SPI — for teams that want a
dedicated job tier. It is a packaging choice, not an architecture change, and it must
never become the primary target, because the moment it does, someone will optimise
for it and break clause 1.

### 14.2 Messaging — decision: **no broker**

| Option | Verdict |
|---|---|
| **DB-backed queue (ours)** | **chosen.** Transactional with the host's writes, one dependency, exact-fetch semantics via `SKIP LOCKED`, ordering by priority, dead-lettering as a terminal state, backpressure as a full queue. Ceiling ~200 k/s (§8.5). |
| **Kafka** | rejected. Delivery is at-least-once like ours, but *delayed* delivery (our core feature) is not a Kafka primitive; per-message scheduling requires a compaction-and-replay scheme or an external timer — i.e. rebuilding our timer *plus* running Kafka. Adds an operational dependency the host may not have, and breaks the outbox. |
| **RabbitMQ** | rejected. Delayed messages via a plugin, per-message TTL quirks, and no query surface — "show me all executions of job X in the last hour" is not a thing a queue answers. We need a *database*; we happen to also need a queue. |
| **Redis** | rejected as the store of record (not durable enough for scheduling), and rejected as a coordination sidecar (a second stateful dependency to buy something one `UPDATE` per 5 s already buys). |
| **Cloud queues (SQS et al.)** | rejected. 15-minute delay ceiling on SQS alone disqualifies it for a scheduler; plus vendor lock-in in a library. |

The one thing a broker would genuinely buy is fan-out beyond ~200 k/s. That is past
our declared wall, and at that point the right answer is multiple Mohs deployments on
multiple databases, not one Mohs on Kafka.

**Outbound events** (webhooks, "publish to Kafka when a job finishes") are a
different question and the answer is: **Mohs is a perfect outbox producer, not a
broker client.** Provide `ExecutionListener` and let the host publish. Building a
Kafka producer into Mohs would add a dependency for a job the host can do in four
lines.

### 14.3 Caching — decision: cache exactly two things

| Cached | Where | Invalidation | Why |
|---|---|---|---|
| **Job definitions** | node-local LRU (bounded, §8.1) | `definition_version` compared on read of a *changed* job; broadcast by `NOTIFY mohs_definition` (Tier 1) or a 30 s TTL sweep (Tier 2) | The claim path must not join `mohs_job`. This is what makes §5.4 possible. |
| **Rate-limit permits** | node-local `Semaphore` | lease expiry (§9) | The whole of §9. |

**Explicitly not cached:** execution state (it is the truth, and a stale read here is
a correctness bug); node membership (read at heartbeat, which *is* the cache);
`GET /overview` counts (they are already cheap by construction, and a cache would
make the dashboard lie about the thing operators trust it for); query results
(pagination + cursors make them useless).

**No distributed cache.** A distributed cache is a second consistency domain. We have
a database; adding Redis in front of it to avoid queries we have already made cheap
would be complexity with negative return.

### 14.4 Observability — designed, not bolted on

**Metrics** (Micrometer, all with the `mohs.` prefix). The rule: **every number here
must be actionable, and every label must be bounded.**

| Metric | Type | Labels | The question it answers |
|---|---|---|---|
| `mohs.schedule.latency` | timer | job, tenant | how long does enqueue take? (the outbox contract's p99) |
| `mohs.queue.depth` | gauge | shard, priority | is the backlog growing? |
| `mohs.queue.oldest.age` | gauge | shard | **the single most important number in the system** — how late is the latest work? |
| `mohs.claim.latency` | timer | — | is the claim round healthy? (309 ms was the rate-limit smell) |
| `mohs.claim.batch.size` | histogram | — | are we claim-bound or dispatch-bound? |
| `mohs.dispatch.latency` | timer | job | scheduled → handler start (the user-visible SLO) |
| `mohs.execution.duration` | timer | job, outcome | handler time |
| `mohs.execution.total` | counter | job, outcome, tenant | throughput and failure rate |
| `mohs.attempt.total` | counter | job, outcome | retry rate — `attempts/executions` is the health ratio |
| `mohs.lease.reclaimed` | counter | reason | **any non-zero value here means a node died or stalled** |
| `mohs.node.inflight` / `.capacity` | gauge | node | utilisation, autoscaling signal |
| `mohs.ratelimit.permits.held` / `.exhausted` | gauge / counter | limit | is a limit throttling, and is it the intended one? |
| `mohs.db.commit.batch.size` | histogram | kind | is group commit actually amortising? |
| `mohs.clock.skew` | gauge | node | §15.6 |

Cardinality guard: `job` is bounded by definition count and is allowed; `tenant` is
allowed but capped with a configurable allow-list; `execution_id` is **never** a
label. This must be enforced in code, not by convention, because unbounded label
cardinality is the most common way a metrics stack is destroyed.

**Logs** — structured, one event per line, with a fixed key set:
`mohs.execution_id`, `mohs.job_key`, `mohs.attempt`, `mohs.node_id`,
`mohs.tenant_id`, `trace_id`, `span_id`. Bound via `ScopedValue` + MDC bridge so the
host's existing appenders pick them up with no configuration. Levels, decided:
`INFO` for state transitions an operator would ask about (fired, reclaimed, retried,
limit exhausted for > 1 round); `WARN` for anything requiring a human eventually
(misfire applied, drain grace exceeded, event dropped); `ERROR` only for a failure of
Mohs itself, never for a failure of a *job* — a failing job is data, not an incident,
and today's log volume conflates them.

**Tracing** (OpenTelemetry): the span boundaries are decided as —

```
[host span: HTTP POST /orders]
   └── mohs.schedule            (client kind; captures traceparent into mohs_execution.trace_id)
                    ⋮  (durable gap — minutes, hours, days)
[new trace, LINKED to the above]
   └── mohs.execution           (server kind; links to the enqueue span)
         ├── mohs.attempt (1)
         │     └── [host's own handler spans]
         └── mohs.attempt (2)
```

The **link, not parent-child**, across the durable gap is the important decision: a
job scheduled for next Tuesday must not extend a trace for four days. Storing
`trace_id` on the execution (§7.2) is what makes the link possible, and it also makes
"show me the request that scheduled this" a dashboard feature.

**Dashboards** operators actually need — four, not twenty:

1. **Is it keeping up?** `queue.oldest.age` + `queue.depth` + throughput vs. arrival.
2. **Is it healthy?** failure rate, retry ratio, `lease.reclaimed`, node states/versions.
3. **What is slow?** `dispatch.latency` and `execution.duration` heat maps by job.
4. **What is throttling?** rate-limit saturation, concurrency-cap denials, runner
   queue depth.

**Health probes:** `liveness` = the JVM is up and the claim thread is not wedged
(last-tick age < 3 × poll). `readiness` = definition cache loaded, handlers
registered, DB reachable, node row written. **The database being down must not fail
liveness** — that is the classic mistake where a DB blip restarts every pod and turns
an outage into an outage plus a cold-start storm.

---

## 15. Failure engineering and data lifecycle

### 15.1 Component-by-component failure matrix

| Component | Fails how | System behaviour | Recovery |
|---|---|---|---|
| **Database down** | connections refused | engine → `DEGRADED`: stops claiming, in-flight handlers keep running, completions buffer (bounded). No exception storms — one log line per state change, not per attempt. | auto on reconnect; buffered completions flush idempotently |
| **Database slow** | p99 climbs | adaptive poll backs off; claim batch shrinks (measured round latency feeds the next round's size); backpressure reaches the enqueue path last | self-correcting |
| **Claim thread wedged** | no rounds | liveness probe fails (last-tick age); node lease expires; peers take the shards and reclaim the work | pod restart + reap |
| **Dispatcher saturated** | bounded queue full | claim blocks → claims nothing → work stays in `mohs_ready` → **another node claims it**. This is the good failure mode and it is structural, not arithmetic (contrast ADR-0039's counter clamp). | automatic |
| **Handler hangs** | never returns | `timeout` signals cancel + interrupt (ADR-0034); Watchdog Bound stops renewal; node lease covers total node death | as designed today |
| **Handler leaks threads/memory** | node OOM | node dies; lease expires; work re-queued | reap |
| **Node partitioned** | see §6.6 #6 | fenced by epoch | reap + fence |
| **Clock jumps backwards** | `visible_at` in the future | nothing is claimed until real time catches up; `mohs.clock.skew` gauge alarms; **durations are all `System.nanoTime`**, so leases and timeouts are unaffected | §15.6 |
| **Corrupted payload** | deserialisation throws | terminal `FAILED` with `error_type` = the deserialisation exception, at attempt 1, **no retry** (a poison payload retried is wasted work) | manual |
| **Poison job** (fails every time, fast) | burns retry budget then FAILS | retry backoff is exponential with jitter and a cap; a job whose failure rate is 100% over a window trips a per-job **circuit breaker** that pauses claiming for that job and raises an event. **New, and it is the one genuinely missing safety feature today**: nothing currently stops a job failing 4,000 times a second. |
| **Retention job dies mid-run** | partitions not dropped | idempotent; next run drops them | automatic |
| **Rolling deploy mid-execution** | v1 draining, v2 claiming | §11.3 | automatic |
| **Duplicate/delayed message** | n/a | there are no messages; there is a database | — |

### 15.2 The circuit breaker, specified

Per job: a sliding window of the last `N` (default 20) attempts. If the failure rate
exceeds `threshold` (default 100%) *and* mean duration is below `fastFailFloor`
(default 1 s), the job is auto-paused with a `JobCircuitOpened` event, a `WARN` log
naming the dominant `error_type`, and a dashboard badge. Auto-probes one execution
every `probeInterval` (default 5 min) and closes on success. This is the difference
between "a bad deploy costs one alert" and "a bad deploy costs 14 million rows of
failure history overnight."

### 15.3 Data lifecycle

| Stage | Duration | Storage | Mechanism |
|---|---|---|---|
| **hot** | now → completion | `mohs_ready` / `mohs_lease` | rows deleted at transition |
| **warm** | completion → retention window (default 7 d) | current + recent `mohs_execution` / `mohs_attempt` partitions | queryable at full fidelity by the dashboard |
| **cold** | beyond the window | nothing — **dropped** | `DROP TABLE mohs_execution_<period>` |
| **aggregate** | forever | `mohs_execution_daily` (job, day, tenant, outcome, count, p50/p95/p99 duration) | rolled up by the retention job *before* the drop |

Two decisions here:

- **Retention stays delete-not-archive** (ADR-0032's reasoning is sound), but the
  *mechanism* becomes partition drop. That removes the batch-size tuning against SQL
  Server's lock-escalation threshold, the UUIDv7 frontier arithmetic, the vacuum
  aftermath, and the "execution that finishes long after it was created is measured
  by creation" edge case — because with partitions the edge case simply means the row
  lives in an older partition, which is fine.
- **A daily rollup is added**, and it is the piece today's design is missing. Without
  it, retention destroys the only record of long-term trend. One small table, written
  once a day, that answers "has this job been getting slower over six months" —
  which is the question retention currently makes unanswerable.

At **billions of rows**: partitions are ~7 days × arrival rate; a 10 k/s system
produces ~6 billion rows per week, which means daily or hourly partitions and a
retention window measured in days. The design holds because *no hot-path query ever
touches the history tables*, and history queries are partition-pruned by their time
predicate. The dashboard's default range must therefore always carry a time bound —
enforced in the API, not left to the client (§16.5).

### 15.4 The one place leader election is used

Retention and rollup: slow bulk operations that must not run N times. Election via
the same conflict-insert primitive as §10.2 (`INSERT INTO mohs_leader (task, node,
expires_at) ON CONFLICT (task) DO UPDATE … WHERE mohs_leader.expires_at < now()`).
No ZooKeeper, no etcd, no Raft — one row, one statement, and a lease that expires.

### 15.5 Schema evolution

`PENDENCIAS.md` item 10 is the open issue: `CREATE TABLE IF NOT EXISTS` means an
existing database never gains a new column. **Decision: adopt Flyway with an
expand/contract discipline before the first external user**, with migrations shipped
inside the jar under a Mohs-owned history table (`mohs_schema_history`) so the host's
own Flyway is untouched. Every migration must be online-safe: add nullable, backfill
in batches, switch reads, then drop. This is not optional for a library — a library
that requires downtime to upgrade will not be adopted.

### 15.6 Clocks

Keep everything ADR-0008 decided: injected `Clock`, `DatabaseSyncedClock`, NTP-style
offset sampling, ArchUnit enforcement that nothing reads `Instant.now()` directly.
Add two things:

- **All durations already use `System.nanoTime`** — make it an ArchUnit rule, not a
  convention, so a lease or timeout can never be computed from wall-clock and
  therefore can never be corrupted by a backwards jump.
- **`mohs.clock.skew` as an alerting gauge**, with a documented threshold, because
  the failure mode of undetected skew in a scheduler is "jobs fire at the wrong time
  and nobody knows why."

Known open defect to carry forward: `JdbcTimestamps` computes the wrong bucket/lease
during a DST gap. In the target design timestamps are `TIMESTAMPTZ` and all arithmetic
is UTC-based, which **removes the defect by construction** rather than fixing it.

---

## 16. API and frontend

### 16.1 REST — principles, decided

Keep `docs/REST-API-DESIGN.md`'s good decisions (cursor pagination, RFC 7807
`ProblemDetail`, `Idempotency-Key`, `202` for invocation, `/overview` cheap by
construction). Change these:

| Area | Decision |
|---|---|
| **Versioning** | `/api/mohs/v1` in the path. Media-type versioning is more elegant and less operable; a path version is greppable in an access log at 3 a.m. |
| **Time bounds** | `GET /executions` **requires** a time window (default: last 24 h, max configurable). Unbounded history queries against a partitioned billion-row table are how a dashboard takes down a production database. This is a hard API rule, not a client convention. |
| **Bulk operations** | `POST /executions:cancel`, `POST /jobs:pause` taking id lists, because operators act on sets during incidents and N round trips is how an incident gets longer. |
| **Optimistic concurrency** | `If-Match` / `ETag` on `PATCH /jobs/{key}` carrying `definition_version`. Two operators editing a schedule concurrently currently last-write-wins silently. |
| **Error model** | RFC 7807 with a **stable `type` URI per error class** and a machine-readable `code`. Today's design has the shape; it needs the registry so clients can branch on errors without parsing prose. |
| **Filtering/sorting** | fixed, indexed filter set only (`jobKey`, `state`, `tenant`, `correlationId`, time range). **No generic query language.** Arbitrary filters against a partitioned history table produce arbitrary plans. |

### 16.2 Real-time updates — decision: SSE for the dashboard, webhooks for integration

`ADR-0046` decided *not* to change `/overview/stream` and `docs/DASHBOARD-STREAM-REVIEW.md`
records that two optimisations were implemented and reverted, and that **nobody has
measured the endpoint**. That decision stands and this document does not overturn it
— the correct next step is still measurement, not redesign.

What *is* decided: SSE remains the transport (unidirectional, proxy-friendly,
auto-reconnecting, no protocol upgrade), and WebSocket is rejected — it buys
bidirectionality nothing needs and costs sticky-session handling in every load
balancer. What changes is the *content* once measurement justifies it: the stream
should carry deltas keyed off the completion batcher rather than a full snapshot
poll. That is filed as a triggered item, not a phase-1 change.

**Webhooks** are a genuine gap for integration (not dashboard) use: a `WebhookSink`
that receives terminal events, delivered — inevitably — *as a Mohs job*, which gives
retries, backoff, rate limiting and a dead-letter state for free. That is the
dogfooding payoff of having a good scheduler.

### 16.3 The public Java API — the four breaking changes

Everything else in `docs/API-DESIGN.md` survives verbatim. These four do not:

1. **`Batch` loses `total`/`succeeded`/`failed` as stored counters** and becomes a
   `correlationId` plus a `progress()` query (§10). `onCompletion` changes from an
   in-memory registration to a durable follow-up job.
2. **`Execution.leaseExpiresAt()` disappears.** Ownership is `(nodeId, epoch)`, and
   the useful public question — "who is running this?" — is answered by
   `Execution.owner()`.
3. **`ExecutionState.RETRY_SCHEDULED` → `RETRY_WAITING`**, and it is no longer a
   claimable state (§4.3).
4. **`JobDefinition` gains `tenant`, `circuitBreaker` and `rateLimitKey`**, and its
   many optional fields move behind the existing staged builder rather than growing
   the record's constructor.

### 16.4 Frontend

The dashboard should be organised around the **four operator questions of §14.4**,
not around the table list it currently mirrors. Five pages today
(Overview/Jobs/Executions/RateLimits/Runners) map to backend nouns; operators think
in verbs.

| Page | Answers | Key elements |
|---|---|---|
| **Health** (landing) | *is it keeping up, is it healthy* | oldest-queued-age as the hero number (not throughput — throughput looks fine while a queue drowns), arrival vs. completion rate, failure/retry ratio, node roster with build versions, open circuit breakers |
| **Jobs** | *what is defined, what is misbehaving* | per-job success rate, p95 duration sparkline, next fire, paused/circuit state, one-click pause/run-now/reschedule |
| **Executions** | *what happened to this one* | mandatory time filter, faceted by `error_type` (the new column pays off here), attempt timeline, payload, `trace_id` deep-link to the host's tracing UI |
| **Throughput & limits** | *what is throttling* | rate-limit saturation, concurrency-cap denials, runner queue depth, per-tenant breakdown |
| **Nodes** | *what is the cluster doing* | shards owned, in-flight vs. capacity, build version, state, last heartbeat — the rolling-update view |

Two hard rules: (1) **the frontend never dictates a backend shape** — if a panel
needs an expensive query, the panel changes, per ADR-0045's posture that the
dashboard consumes the public API like any other client; (2) **every page has a
mandatory or defaulted time bound** (§16.1).

---
## 17. Security

Mohs is embedded, so the security posture is mostly **"do not weaken the host"** —
with three places where Mohs must actively do work.

| Concern | Decision |
|---|---|
| **Authentication** | None of Mohs's business. The REST API and dashboard mount inside the host's servlet context and inherit its Spring Security configuration. **Default: the API is disabled** (`mohs.api.enabled=false`, as today) — an operational API that appears unauthenticated because someone added a dependency is unacceptable. |
| **Authorization** | Two roles, enforced in the controllers with `@PreAuthorize` on a configurable authority name: `MOHS_VIEWER` (all `GET`) and `MOHS_OPERATOR` (mutations: pause, cancel, retry, reschedule, rate-limit patch). A read-only dashboard link is the common ask and today there is no way to give one. |
| **Actor attribution** | `ActorResolver` (exists today) stays and becomes **mandatory for mutations** — every state change records who caused it. That is the audit trail, and it needs no separate audit log because `mohs_execution.actor` and the attempt record already carry it. Add `mohs_job_audit` (job_key, actor, action, before, after, at) for definition changes, which are the ones with no execution row to hang off. |
| **Job payload** | Payloads routinely contain PII. Decisions: (a) a size cap (§12.6); (b) `@Sensitive` field marking that redacts in the API response and the dashboard while keeping the value in the database — because a payload you cannot see is a payload you cannot debug, and redaction at the *view* is the right layer; (c) documented guidance that payloads should carry **references, not secrets**; (d) never log a payload, at any level, ever. |
| **Secrets** | Mohs never stores credentials. Handlers resolve their own secrets from the host's mechanism. There is no "job with a password field" feature and there should never be. |
| **Execution isolation** | Handlers run in the host JVM with host privileges. **We state plainly that Mohs provides no sandbox** and is not a multi-tenant *code* execution platform — tenancy (§13) isolates data and capacity, not code. Pretending otherwise would be the most dangerous sentence in the documentation. |
| **SQL injection** | All SQL is static with bound parameters; `job_key` and `correlation_id` are never interpolated. Enforceable by an ArchUnit/static check that no `String.format`/concatenation reaches a JDBC template on a non-constant path — worth having as a test given the `%s` formatting already present in the reaper's dialect assembly. |
| **Denial of service** | Payload cap, enqueue rate limit per tenant, mandatory time bounds on history queries (§16.1), bounded metric cardinality (§14.4). Each of these is a real, reachable DoS against the host today. |

---

## 18. Module architecture

### 18.1 The modules

```text
mohs-cron                 no deps (vendored cron parser)
   ▲
mohs-api                  ◀── the public contract. depends on: nothing but JDK + JSpecify
   ▲                          (today: mohs-core; renamed because "core" invites
   │                           implementation to leak in — and it did not, but the
   │                           name is a standing invitation)
   ├── mohs-engine        ◀── claim/dispatch/timer/reaper. depends on: mohs-api, mohs-cron
   │      ▲                   NO Spring beyond core interfaces. NO JDBC.
   │      │
   │      ├── mohs-store-jdbc      ◀── depends on: mohs-engine (implements its ports),
   │      │                            spring-jdbc. Owns ALL SQL and the dialect tiering.
   │      │
   │      └── mohs-store-jdbc-<t>  ◀── ONLY if Tier-2 divergence exceeds a threshold.
   │                                   Not created up front.
   │
   ├── mohs-rest          ◀── depends on: mohs-api + spring-web. NEVER mohs-engine
   │                          or mohs-store. Today mohs-rest is already clean here;
   │                          the rule is written down so it stays that way.
   │
   ├── mohs-ui            ◀── no Java. React bundle on the classpath. depends on nothing.
   │
   ├── mohs-test          ◀── depends on: mohs-api (+ mohs-engine for the in-memory store)
   │
   └── mohs-spring-boot-starter
          depends on: everything above, optionally. The ONLY module that knows
          about Spring Boot auto-configuration, properties, and conditional wiring.

mohs-bom                  version alignment only
mohs-demo                 app, never published
mohs-benchmark            NEW — JMH + the load harness + the EXPLAIN harness.
                          Today these live scattered in test sources; performance
                          work deserves a module with its own lifecycle.
```

### 18.2 Dependency rules, enforced

| Rule | Enforced by |
|---|---|
| `mohs-api` depends on no Mohs module and no Spring | reactor + ArchUnit |
| `mohs-engine` never imports `java.sql` or `javax.sql` | ArchUnit |
| `mohs-engine` never imports `io.mohs.store.*` | reactor (not on classpath) |
| `mohs-rest` never imports `io.mohs.engine.*` or `io.mohs.store.*` | reactor + ArchUnit |
| No module except the starter imports `org.springframework.boot.autoconfigure` | ArchUnit |
| Nothing reads `Instant.now()`/`System.currentTimeMillis()` outside `Clock` impls | ArchUnit (exists) |
| Every duration measurement uses `System.nanoTime` | ArchUnit (**new**, §15.6) |
| No SQL string reaches a JDBC template via non-constant concatenation | ArchUnit (**new**, §17) |
| Every production package has `@NullMarked` | ArchUnit (exists) |

### 18.3 The rename that matters

`mohs-core` → **`mohs-api`**, and `mohs-jdbc` → **`mohs-store-jdbc`**. Both names are
load-bearing: "core" is where implementation drifts to, and "jdbc" describes a
technology rather than a role. The new names make the dependency direction obvious
from the artifact list alone, which is the cheapest architecture enforcement there is.

`io.mohs.engine`'s ports (`ExecutionStore`, `JobStore`, `Claimer`, `Reaper`,
`NodeStore`, `RateLimitStore`, `BatchStore`, `TriggerFirer`) collapse to **four**,
matching the four tables:

```java
interface WorkQueue    { List<Claimed> claim(ShardSet, int n); void enqueue(List<Enqueue>); void requeue(...); }
interface LeaseStore   { void complete(List<Result>); Set<Owned> ownedBy(NodeId, long epoch); ... }
interface HistoryStore { void record(List<Enqueue>); Page<Execution> query(Query); Progress progress(CorrelationId); }
interface ControlStore { NodeLease heartbeat(...); List<Node> members(); List<Trigger> due(...); boolean advance(...); }
```

Eight ports for four tables was interface proliferation caused by the tables not
matching the concepts. Fixing the schema fixes the ports.

---

## 19. Architecture Decision Records

Ten decisions, in the format the brief requires. These become `docs/adr/0047`+ in the
existing sequence.

### ADR-A · Split `mohs_executions` into queue, lease and history

- **Decision.** Four tables with four write profiles: `mohs_ready` (INSERT/DELETE),
  `mohs_lease` (INSERT/DELETE), `mohs_execution` (INSERT + one UPDATE, partitioned),
  `mohs_attempt` (INSERT only, partitioned).
- **Alternatives.** (a) Keep one table, add more partial indexes — already at the
  limit of what predicate implication can buy (DBTUNE-5/17 exhausted it).
  (b) One table, partitioned by `state` — PostgreSQL cannot partition by a column
  that changes without a row move, and the row move *is* the cost we are removing.
  (c) Separate queue in Redis/Kafka — breaks the outbox (§14.2).
- **Why.** It is the only change that attacks all three of: tuple versions (~9 → 2),
  index churn on the hot path (2 non-HOT updates → 0), and the retention mechanism
  (batched delete → partition drop). Every other optimisation is downstream of it.
- **Trade-offs.** Four tables to keep consistent instead of one row; two extra small
  writes per execution; a read model (`mohs_execution.state`) that lags the truth
  (`mohs_lease`) by up to one flush. Queries that need truth must join.
- **Reversibility.** Low. This is the foundation; changing back means changing
  everything. It should therefore be validated by experiment E1 (§20) **before**
  phase 2 starts.

### ADR-B · The lease belongs to the node, not the execution

- **Decision.** Delete `lease_expires_at` per execution. Ownership is
  `(node_id, epoch)` on `mohs_lease`; liveness is one `mohs_node` row per node,
  heartbeated every 5 s.
- **Alternatives.** (a) Keep per-execution leases but renew less often — widens the
  detection window for *every* execution and still writes per execution. (b) Renew
  only a "youngest lease" sentinel — a clever hack with a subtle failure mode when
  the sentinel completes first.
- **Why.** ~20,480 lease writes/s at the current operating point become 0.2/s. The
  capability being paid for (per-execution stall detection) is already delivered by
  `timeout` and the Watchdog Bound, as ADR-0012's own reasoning concedes.
- **Trade-offs.** Detection granularity is now per-node. A node that is alive but has
  one wedged handler is detected by `timeout`, not by the lease — which is the
  correct tool anyway.
- **Reversibility.** Medium. The column could be re-added, but the epoch fencing
  would have to coexist with the timestamp CAS.

### ADR-C · Group-commit the claim and the completion

- **Decision.** Completions accumulate in a bounded queue and flush on 256 results or
  5 ms, whichever first, as three `unnest()` statements in one transaction with a
  fenced `RETURNING`.
- **Alternatives.** (a) `synchronous_commit = off` — faster and *lies about
  durability*; rejected outright for a scheduler. (b) PostgreSQL `commit_delay` —
  helps, but it is the host's global setting and a library must not require it.
  (c) Async completion with a write-behind log — a second durability mechanism to
  get right; rejected on KISS.
- **Why.** `LWLock:WALWrite` is measured at the top of the wait profile. Commits per
  execution go 3 → ~1.002.
- **Trade-offs.** The at-risk window grows from ~1 ms to ≤ 5 ms. The *guarantee* is
  unchanged (at-least-once); the *probability* of a duplicate after a crash rises
  ~5×, against a baseline where a crash already re-runs up to 1,024 in-flight items.
- **Reversibility.** **High** — `flushOnEveryResult=true` restores the old behaviour
  at runtime. This is why it is safe to ship early.

### ADR-D · Derive concurrency caps from the lease table

- **Decision.** Delete `running_execution_count`. Cap headroom is
  `count(*) GROUP BY job_key` over `mohs_lease`, read once per claim round.
- **Alternatives.** (a) Keep the counter (hot row, leaks, needs ADR-0025's repair).
  (b) Slot table (kept as the escape hatch when a *hard* cap is required).
- **Why.** Removes a hot row per capped job, removes the increment/decrement from
  the claim transaction, and removes ADR-0025 entirely — the reaper no longer has a
  slot to release because deleting the lease *is* releasing it.
- **Trade-offs.** Soft cap: over-admission bounded by `nodes × 1 round`.
- **Reversibility.** High — the slot table is a drop-in for jobs needing hardness.

### ADR-E · Lease rate-limit tokens to nodes

- **Decision.** Nodes deduct a batch of permits from the bucket in their own short
  transaction and spend them from an in-memory `Semaphore`. Zero DB work per
  execution.
- **Alternatives.** (a) Today's shared bucket — measured ceiling ~33 rounds/s, flat
  from 2 nodes, degrading at 8, and it stalls unrelated jobs. (b) Temporal-style
  static per-node quota — no hard cap on scale-down, and under-delivers when nodes
  are unbalanced. (c) Sharded bucket (N rows per limit) — helps contention, keeps
  the write in the claim transaction, and makes `available` a fan-out read.
- **Why.** Over-delivery becomes *mathematically impossible* (permits are deducted
  before they exist) while the hot path becomes coordination-free. It removes the
  documented defect where a rate-limited job stalls jobs with no limit.
- **Trade-offs.** Bounded under-delivery from unspent leases; mitigated by short
  windows, return-on-idle, and demand-proportional sizing. `GET /rate-limits`
  `available` becomes "in the bucket", with permits-held-by-nodes shown separately.
- **Reversibility.** High — the bucket row and its CAS survive; only the caller
  changes.

### ADR-F · Shard the ready set into 64 fixed shards

- **Decision.** `shard = hash(execution_id) % 64`; ownership derived from sorted live
  node ids at heartbeat; the claim probes **one owned shard per statement**,
  round-robin (E2 — §5.4); a full empty lap ends the round.
- **Alternatives.** (a) No sharding — `SKIP LOCKED` convoy from ~4 nodes.
  (b) Claim-per-runner (`CLAIM-GRANULARITY.md` option B) — makes priority meaningless
  *across* runners, which operators notice, and needs a runner count nobody has.
  (c) Core-per-job (option C) — rejected there, correctly, and still rejected.
- **Why.** Turns O(N) skip-scan and shared index-page contention into O(1) per node.
  It is the only mechanism here that changes the node-count ceiling.
- **Trade-offs.** Priority is strictly global only within a shard; practical effect
  bounded by one poll interval. Brief double-ownership during membership change,
  which degrades to today's behaviour and self-heals in one heartbeat.
- **Reversibility.** High — `SHARD_COUNT=1` disables it with no schema change.

### ADR-G · Adaptive poll with `NOTIFY` wake-up, poll as backstop

- **Decision.** Poll interval doubles on empty rounds (25 ms → 2 s), resets on work;
  `NOTIFY` and post-commit local hand-off provide immediacy; neither is required for
  correctness.
- **Alternatives.** (a) Fixed poll — pays when idle and limits when loaded.
  (b) Notification-only — a lost notification means a lost job; unacceptable.
  (c) In-memory timing wheel as primary — a cache of a shared database that must be
  invalidated across nodes (§5.5).
- **Why.** Idle 10-node cluster: 200 queries/s → ~5. p50 dispatch latency:
  ~poll/2 (≈ 25 ms) → < 5 ms.
- **Trade-offs.** Two wake-up paths to reason about; `NOTIFY` is Tier-1 only.
- **Reversibility.** Trivial.

### ADR-H · Tier the dialects; H2 is test-only

- **Decision.** PostgreSQL Tier 1; SQL Server and MySQL Tier 2; H2 explicitly not a
  supported production backend.
- **Alternatives.** (a) Four peers (today) — pins the design to the least-capable
  engine and to a backend whose row lock is measurably broken (~33% double-lock).
  (b) PostgreSQL only — smaller market; SQL Server is a real requirement in this
  ecosystem.
- **Why.** Unlocks partitioning (which deletes the retention machinery), `NOTIFY`,
  `DELETE … RETURNING`, and the CTE claim; cuts the tuning matrix by 75%.
- **Trade-offs.** Tier-2 users get a documented performance gap and batched-delete
  retention. A public tiering statement is a marketing cost.
- **Reversibility.** Medium — Tier-2 parity can be added later per feature.

### ADR-I · A batch is a correlation id, not an aggregate

- **Decision.** Drop `mohs_batches`. `correlation_id` column + derived progress +
  conflict-insert election + durable follow-up job.
- **Alternatives.** Fix the four documented defects in place — items 1 and 2 are
  described as incurable in the current shape, and the fixes for 3 and 4 add
  machinery.
- **Why.** All four defects dissolve rather than get fixed, and the feature gets
  *smaller*.
- **Trade-offs.** `progress()` is a query (sub-ms over an index) rather than a read
  of a counter. Public API changes (§16.3).
- **Reversibility.** Medium.

### ADR-J · Per-job circuit breaker

- **Decision.** A job whose recent attempts fail at ~100% and fail fast is auto-paused
  with an event and a dashboard badge, probed periodically, auto-closed on success.
- **Alternatives.** Rely on retry budgets — they cap attempts *per execution*, not
  per job, so a bad deploy still burns the cluster.
- **Why.** It is the only genuinely missing *safety* feature: today nothing stops a
  job failing 4,000 times a second and filling the history with noise while starving
  healthy work.
- **Trade-offs.** A job can be paused without a human asking. Mitigated by making it
  loud (event + `WARN` + badge) and probe-based rather than sticky.
- **Reversibility.** Trivial (a threshold).

---

## 20. Performance and reliability goals, and the experiments that validate them

### 20.1 Targets

Measured on the same class of hardware as the 2026-08-16 run (single host,
PostgreSQL in Docker, trivial handler), so the numbers are comparable.

| Metric | Today (measured) | Target | Stretch |
|---|---|---|---|
| single-node throughput | 4,023–4,222 exec/s | **12,000** | 20,000 |
| 8-node aggregate | not measured | **60,000** | 100,000 |
| commits / execution | **4.9 measured** (Phase 0: 2 write + ~1.9 read round trips + 1 enqueue) | **≤ 1.05** | 1.01 |
| tuple versions / execution (history table) | **3.9 measured** (Phase 0, trivial handler — see note) | **2** | 2 |
| lease-maintenance writes / execution | **~0 measured** at the trivial-handler operating point; up to ~5 with sustained in-flight (see note) | **0** | 0 |
| dispatch latency p50 (light load) | ≈ 25 ms (poll/2) | **< 5 ms** | < 2 ms |
| dispatch latency p99 (at 80% capacity) | not measured | **< 250 ms** | < 100 ms |
| enqueue p99 (outbox path) | 7.8 ms avg under drain | **< 5 ms p99** | < 2 ms |
| claim round latency p99 | 30 ms (309 ms with a hot limit) | **< 20 ms**, no limit-dependence | < 10 ms |
| API p99 (`GET /overview`) | not measured | **< 100 ms** at 10⁹ history rows | < 50 ms |
| idle DB queries/s (10 nodes) | 200 | **< 10** | < 5 |
| engine heap overhead | not measured | **< 100 MB** at 10 k in-flight | < 50 MB |
| engine allocation rate | not measured | **< 50 MB/s** at 10 k exec/s | < 20 MB/s |

**Phase 0 results (2026-08-21, BASELINE "Write amplification por execução"):**
the three gating numbers now exist, and two §1.1 predictions were corrected by
measurement. Tuple versions are **3.9**, not ~9: lease renewal only touches what
is in flight *at the instant of the tick*, and a trivial handler drains the
pipeline between ticks — Finding A's ~20k writes/s ceiling is real but
**load-dependent** (sustained in-flight × tick rate), so ADR-B's payoff must be
re-measured with slow handlers under E6, where it applies in full. Commits are
**~4.9 end-to-end**, not 3: the two synchronous write commits the 08-16 round
identified, plus ~1.9 previously uncounted autocommit *read* round trips per
execution (attempt loading for dispatch, driver ceremony) — the group-commit
prize (Phase 3) is larger than estimated, and the read round trips are a
separate, additional lever. WAL: **~2.2 KB/execution** clean of FPIs.

Reliability:

| Property | Target |
|---|---|
| node-death detection | < 15 s (3 missed heartbeats) |
| work re-queued after node death | < 20 s p99 |
| zero lost executions | invariant — verified by chaos tests (E6) |
| zero over-delivery on a rate limit | invariant — verified by E4 |
| clean shutdown, no re-execution | < `gracePeriod`, verified by E6 |

### 20.2 Benchmark scenarios (the suite that must exist)

| # | Scenario | Measures | Pass criterion |
|---|---|---|---|
| **S1** | steady state, 1 node, trivial handler, 1 M backlog | throughput ceiling | ≥ 12 k/s |
| **S2** | steady state, 8 nodes, sharded | horizontal scaling | ≥ 6× S1 |
| **S3** | burst: 0 → 100 k enqueued in 10 s | queue drain rate, latency recovery | oldest-age back under 5 s within 60 s |
| **S4** | 10 M scheduled triggers, 1 % due per minute | timer scalability | materialisation lag < 2 s |
| **S5** | 10⁹-row history, dashboard active | API latency under history pressure | `/overview` p99 < 100 ms |
| **S6** | node kill −9 at 80% load | recovery, duplicates | 100% executed; duplicates only for in-flight |
| **S7** | rate limit at 50% of demand, 8 nodes | limit precision + collateral damage | never over-delivers; **unlimited jobs unaffected** |
| **S8** | database pause 30 s | degradation and recovery | no data loss; no exception storm; recovery < 10 s |
| **S9** | rolling update, 4→4 nodes, changed handler set | zero retry burn | no `NO_HANDLER` attempts |
| **S10** | 100 k-member batch | creation atomicity + completion detection | one `BatchCompleted`, exactly once |

### 20.3 Experiments to run *before* committing to the design

The brief asks what needs experimentation. These are the six that could invalidate a
decision, ordered by how expensive it would be to discover the answer late.

| # | Question | Method | Decision at risk | Kill criterion |
|---|---|---|---|---|
| **E1** | Does the table split actually deliver the write reduction? | Build both schemas; replay an identical 500 k-execution workload; compare `pg_stat_user_tables` (`n_tup_upd`, `n_dead_tup`), `pg_stat_wal` bytes, and end-to-end throughput. | **ADR-A** — the foundation | < 1.5× throughput improvement, or WAL bytes/execution not down ≥ 40% |
| **E2** | Is `DELETE … RETURNING` + `INSERT` genuinely cheaper than the current `SELECT FOR UPDATE SKIP LOCKED` + `UPDATE`, at 1/4/8/16 concurrent claimers? | `ClaimQueryLoadHarness`, extended; `EXPLAIN (ANALYZE, BUFFERS)` both forms — the new form measured **including** the dispatcher's batched payload read (§5.4), or the comparison is against a query that does not exist | **ADR-A**, §5.4 | new form is not ≥ 1.3× at 8 claimers |
| **E3** | Where does the `SKIP LOCKED` convoy actually start, and does sharding fix it? | Same harness at 1/2/4/8/16/32 claimers, with `SHARD_COUNT ∈ {1, 64}` | **ADR-F** | unsharded is already linear to 16 (then sharding is premature) |
| **E4** | Does token leasing hold the cap exactly, and what is the real under-delivery? | 8 nodes, limit at 50% of demand, 10-minute run; count deliveries per window against `max`; includes idle nodes exercising return-on-idle (§9.4) | **ADR-E** | any window exceeds `max`, or steady-state delivery < 90% of nominal |
| **E5** | What does group commit cost in duplicates, and gain in throughput? | S1 with `flush ∈ {1, 64, 256, 1024}` × `{1 ms, 5 ms, 20 ms}`; kill −9 at each setting and count duplicates | **ADR-C** (tuning of, not the decision) | duplicates scale worse than linearly with batch size |
| **E6** | Does the node-lease model detect and recover as fast as per-execution leases? | Chaos: kill −9, `SIGSTOP` (simulated GC pause), network partition to DB, at 80% load | **ADR-B** | recovery p99 > 20 s, or any lost execution |

**Phase 1 results (2026-08-21, BASELINE "E2/E3 — forma do claim e sharding"):**
E2 ran three rounds (round 3, with post-seed `ANALYZE`, JVM warmup discarded and
median-of-3 on verdict cells, is the round of record). The literal §5.4 form
(`shard = ANY(:owned)`, at the proposed 8-of-64-shards ownership) measured 1.01×
of the current engine before the methodology fixes and 1.44× after — above the
kill line but **~2× worse than the single-shard round-robin form in the same
cell**, with a plan that degenerates to full scan + external sort as the shard
fraction grows (0.47× unsharded; 25.5 ms/round vs 0.43 ms, EXPLAIN captured).
Retired by dominance and plan instability; §5.4 rewritten. The corrected form
**passes E2**: 2.21× (unsharded) and 2.91× (64 shards) of the current claim at 8
claimers, 487k rows/s at 16, payload read included. **E3 validated ADR-F**: the
unsharded form *degrades* from 8 to 16 claimers (261.7k → 161.7k rows/s,
medians) while the sharded one keeps scaling (345.1k → 487.3k) — the kill
criterion ("unsharded already linear to 16") was not met. Version note: the
`ANY` collapse is PG 16 planner behaviour (PG 17 preserves order for `= ANY`
scans); the single-shard decision does not depend on it, but re-checking the
`ANY` arm on PG 17 is an ADR-A line item. The WAL-per-row deltas from this
harness are checkpoint-noisy and are NOT evidence — WAL per execution remains
E1's job. E3's declared 32-claimer point was not run: the degradation the
criterion asks about already shows at 16, which decides it.

**E1 results (2026-08-22, BASELINE "E1 — replay fim a fim nos dois schemas"):**
`TableSplitExperimentHarness` replayed the 500 k lifecycle on both schemas,
three arms (current via the real store/claimer; split with per-execution
completion; split + §7.6 group commit), three rounds — rounds 1→3 were
measurement-methodology fixes (round 1's trivial payload was the adversarial
case for the WAL metric; round 2 exposed that idle pooled backends never
flush pg_stat pending counters, inflating the split's numbers; round 3, with
connections evicted before every snapshot, closes the books at exactly
1.000 counters/execution). Round of record (491 B payload): **throughput
passes** — end-to-end 1.79× (≥ 1.5×), drain 25.8× (5.3 k → 136.6 k exec/s);
**§3.3 confirmed exactly** — big-table tuple versions 4.00 → 2.00; **the WAL
kill criterion FIRES** — lifecycle WAL 3,829 → 2,437 B/exec = −36.4%, short
of the ≥ 40% bar (payload-dependent by construction: −31.6% at `'{}'`,
growing with inline payload, regime-inverting past ~2 KB via TOAST).
Attribution the ADR must carry: split WITHOUT group commit clears neither
bar (1.46× / −35.3%) — the throughput is ADR-C's, which Phase 3 also delivers
on the current schema; what only the split buys is ~−1 KB WAL/exec, exactly
two versions on the big table, and being the prerequisite of E3's validated
sharding. **Decision (2026-08-22): ADR-A stands with a revised gate** — the
−40% WAL bar was a point prediction of a payload-dependent quantity; the gate
becomes the measured reality: end-to-end throughput ≥ 1.5× (measured 1.79×),
exactly 2 big-table tuple versions (measured 2.00), WAL/execution down ≥ 30%
across the inline-payload range (measured −31.6% to −36.4%). ADR-A itself is
still born with Phase 5, carrying this result and its attribution — no
round 4 to rescue the original number.

**Two things that need measurement but do not gate the design:**

- The `/overview/stream` endpoint, which `DASHBOARD-STREAM-REVIEW.md` correctly notes
  nobody has measured. Measure before touching, as ADR-0046 already decided.
- The runner-count question from `CLAIM-GRANULARITY.md` — now largely moot, since
  sharding by execution id (ADR-F) solves the isolation problem without needing to
  know how many runners a real system declares.

---

## 21. Migration strategy

Eleven phases (0–10). The ordering rule: **each phase must be independently shippable, behind
a flag where possible, and each must be measurable against the previous one.** No
phase depends on a later phase to be correct.

### Phase 0 — Instrumentation and the honest baseline *(1 sprint)*

- **Objective.** Make the current system measurable at the granularity the redesign
  needs, so every later claim has a before-number.
- **Changes.** Add the §14.4 metric set to the *current* engine. Add
  `pg_stat_statements`, WAL-bytes-per-execution and tuple-version counters to the
  load harness. Create `mohs-benchmark` and move the harnesses into it. Run S1, S6,
  S8 against today's code.
- **Risk.** None. Nothing changes behaviour.
- **Validation.** A BASELINE entry containing commits/execution, tuple versions/
  execution and WAL bytes/execution for today's design. **These three numbers do not
  exist yet and every later phase is judged against them.**
- **Rollback.** n/a.
- **Result (2026-08-22, BASELINE "S6/S8 — chaos"):** Phase 0 closed. S6 passes
  (100% executed, duplicates exactly = in-flight at kill, recovery floored by the
  30 s lease TTL — the number E6 compares ADR-B against). S8: recovery in single-
  digit milliseconds, but two findings — a transient JDBC failure while loading
  the payload terminally FAILs the execution on attempt 1 with the retry budget
  untouched (`Engine.failUnreadablePayload` treats every read failure as
  terminal-by-nature; the §4.3/§6 transient-vs-permanent classification is the
  redesign's answer), and a pause equal to the lease TTL triggers a self-reap
  race (~500-600 in-contract duplicates; ADR-B's node lease removes it).

### Phase 1 — Experiments E1–E3 *(1–2 sprints, no production code)*

- **Objective.** Validate or kill ADR-A and ADR-F before building on them.
- **Changes.** Throwaway schemas and harnesses only.
- **Risk.** The design is wrong and we find out. That is the point, and it is why this
  phase is second, not eighth.
- **Validation.** Kill criteria in §20.3.
- **Rollback.** Abandon the affected ADR; the rest of the plan survives (ADR-C, D, E,
  G, J are independent of A).

### Phase 2 — Foundation: Flyway, `TIMESTAMPTZ`, module renames *(1 sprint)*

- **Objective.** Make schema change *possible* before making schema changes.
- **Changes.** Flyway with `mohs_schema_history`; migrate all four schemas to
  timestamptz semantics (removes the DST defect by construction); rename
  `mohs-core` → `mohs-api`, `mohs-jdbc` → `mohs-store-jdbc`; add the new ArchUnit
  rules; declare the dialect tiering (ADR-H) and move H2 to test-only.
- **Dependencies.** none.
- **Risk.** Renames touch every import. Mechanical, and the reactor catches it.
- **Validation.** Full suite green on all Tier-1/2 dialects; a migration applied to an
  existing database in a test.
- **Rollback.** Revert; no data change beyond column types.
- **Result (2026-08-22, ADRs 0048/0049/0050):** delivered, with one deliberate
  deviation. Library-owned Flyway with `mohs_schema_history` (idempotent V1
  adoption baseline per dialect, `baselineVersion=0`, opt-out
  `mohs.jdbc.migrate`); the DST defect killed **at the traversal, not the column
  type** — `LocalDateTime` via JDBC 4.2 everywhere (ADR-0049 records why the
  letter "migrate all four schemas to timestamptz" was impossible as written:
  MySQL's `TIMESTAMP` ends in 2038; the §7.2 tables are born TIMESTAMPTZ in
  Phase 5 instead), gap regression pinned by `JdbcTimestampsTest`; dialect
  tiering declared (ADR-0050, H2 = Tier 3 with a boot WARN); renames done
  (`mohs-core`→`mohs-api` keeping the frozen `io.mohs.core` public package,
  `mohs-jdbc`→`mohs-store-jdbc` with `io.mohs.jdbc`→`io.mohs.store.jdbc`); the
  two §18.2 ArchUnit rules that don't depend on the split added (engine free of
  JDBC; only the starter speaks autoconfigure).

### Phase 3 — Group commit + `markFired` removal *(1 sprint)* ⭐ ship first

- **Objective.** The largest gain available **without touching the schema**.
- **Changes.** `CompletionBatcher` (ADR-C); fold `fired_at` into the claim `UPDATE`;
  `unnest()`-based multi-row DML on Tier 1.
- **Dependencies.** Phase 0 (for the before-number).
- **Risk.** Low. Behind `flushOnEveryResult`, reversible at runtime.
- **Validation.** S1 throughput up ≥ 1.8×; commits/execution ≤ 1.5; E5 duplicate
  measurement recorded.
- **Rollback.** One property.
- **Why first:** it is the highest ratio of measured gain to structural risk in the
  entire plan, and it makes every later phase's numbers better.
- **Result (2026-08-22, ADR-0047; BASELINE "Phase 3 — group commit + fusões"):**
  shipped on the current schema — `fired_at` folded into the claim CAS, payload
  read batched per round (also killing the S8 finding's transient arm by
  construction), `CompletionBatcher` (256/5 ms, opt-out property), `completeAll`
  returning per-request verdicts, and bulk slot release (the first bench run
  showed the flusher serialized by one `decrementRunningExecutions` round trip
  per execution — §1.2's hidden counter, again). Gates: commits/execution
  3.9 → **0.04** (≤ 1.5 passes 30×); throughput 3.0-3.3k → median **~5.7k =
  1.7-1.9×** on a doubled-history bench (1.8× gate on the line; fold ~1.45×,
  group commit +1.05-1.36×); E5: zero duplicate exposure beyond in-flight
  (batcher queue is still RUNNING rows). The engine is no longer commit-bound —
  the new ceiling is the serial tick (§1.3), which is Phase 5/6's business.

### Phase 4 — Node lease + epoch fencing *(1–2 sprints)*

- **Objective.** Delete the 20 k writes/s.
- **Changes.** ADR-B: `mohs_node` gains `epoch`/`expires_at`/`shards`; reaper becomes
  node-driven; every ownership write gains the epoch guard; per-execution lease
  renewal deleted.
- **Dependencies.** Phase 2 (migrations).
- **Risk.** **Highest correctness risk in the plan** — it changes the failure-detection
  mechanism. Mitigation: E6 chaos suite is a *gate*, not a follow-up, and the old
  column is retained (unused) for one release so a rollback does not need a migration.
- **Validation.** E6 passes; `n_tup_upd` on the executions table drops ≥ 80%.
- **Rollback.** Re-enable per-execution renewal; column still present.
- **Result (2026-08-22, ADR-0051; BASELINE "Phase 4 — node lease + fence de
  posse"):** delivered. Renewal deleted; node lease (`epoch`/`expires_at`,
  15s TTL) in migration V2; reaper dead-node driven, heartbeat-first tick;
  ownership fenced by `(node_id, fired_at)` — the epoch guard deviation is
  recorded in the ADR (`fired_at` is already a per-incarnation token since
  ADR-0047; epoch stays on `mohs_nodes` until `mohs_lease` exists in
  Phase 5). Measured on the renewal-heavy slow-handler workload:
  `n_tup_upd` 6.67–6.97 → **2.00**/execution = **−70%** — the literal ≥80%
  gate assumed ~10 upd/exec before; the renewal component went to zero and
  the remaining 2.00 (claim + terminal CAS) is Phase 5's target, so the
  mechanism gate is met and the literal number is recorded as missed.
  E6 passes: S6 recovery 17.1s (< 20s; was ~31s), SUSPEND shows zero
  double-completions under real zombie resume (fence holds), S8 re-runs
  0 (self-reap dead — was 486–598). One pre-existing gap documented:
  a node frozen mid-claim leaves its batch locked-but-ENQUEUED until the
  session dies (DB-side `idle_in_transaction_session_timeout` is the
  mitigation).

### Phase 5 — The table split *(2–3 sprints)*

- **Objective.** ADR-A. The foundation.
- **Changes.** `mohs_ready`, `mohs_lease`, partitioned `mohs_execution`/`mohs_attempt`;
  new claim (§5.4); derived concurrency cap (ADR-D); `RETRY_SCHEDULED` → ready row
  with `visible_at`; the four ports (§18.3).
- **Dependencies.** Phases 2, 4 (the lease table is where the epoch already lives),
  E1/E2 green.
- **Risk.** High — it is most of the engine. Mitigation: dual-write/shadow-read for
  one release (new tables written and verified, old path still authoritative), then
  flip.
- **Validation.** S1 ≥ 12 k/s; tuple versions/execution = 2; S5 shows history size no
  longer affects claim latency.
- **Rollback.** Flip back to the old path (still present during the shadow release).

### Phase 6 — Sharding + adaptive poll + `NOTIFY` *(1 sprint)*

- **Objective.** ADR-F and ADR-G — the node-count and latency ceilings.
- **Dependencies.** Phase 5 (`shard` lives on `mohs_ready`), E3 green.
- **Risk.** Low. `SHARD_COUNT=1` is a no-op; `NOTIFY` is best-effort by construction.
- **Validation.** S2 ≥ 6× S1; idle query rate < 10/s at 10 nodes; p50 dispatch < 5 ms.
- **Rollback.** Two properties.

### Phase 7 — Rate-limit token leasing + circuit breaker *(1 sprint)*

- **Objective.** ADR-E and ADR-J.
- **Dependencies.** Phase 5 (claim no longer holds locks across the round).
- **Risk.** Medium — E4 must prove the cap is never exceeded.
- **Validation.** S7: zero over-delivery, unlimited jobs unaffected, aggregate rounds/s
  scaling past 8 nodes.
- **Rollback.** Revert to bucket-in-transaction (the row and CAS are unchanged).

### Phase 8 — Batch redesign + retention by partition + rollup *(1–2 sprints)*

- **Objective.** ADR-I; replace ADR-0032's delete batcher with partition drop; add the
  daily rollup.
- **Dependencies.** Phase 5 (partitions).
- **Risk.** Low technically; **breaking** for the public `Batch` API (§16.3).
- **Validation.** S10; the four `BATCH-ARCHITECTURE-REVIEW` defects are unreachable by
  construction (proven by tests that would have caught them).
- **Rollback.** Old batch path kept for one release.

### Phase 9 — API v1 freeze, dashboard rework, security *(2 sprints)*

- **Objective.** §16, §17. Ship the operator experience.
- **Dependencies.** everything.
- **Validation.** S5; a penetration checklist against §17; an operator can answer all
  four questions of §14.4 in under 30 seconds each.
- **Rollback.** n/a (additive).

### Phase 10 — Validation, documentation, GA *(1 sprint)*

Full S1–S10 run, BASELINE rewritten (not edited — a baseline changes only with a new
baseline), migration guide, tiering statement published, ADRs 0047+ committed.

### 21.1 Migration for existing users

Pre-GA, so `PENDENCIAS.md` item 10's "drop and recreate" is available and should be
used — **once**, at Phase 2, when Flyway lands. From then on, expand/contract for
every change. If a user must migrate live: the phases are individually deployable, and
Phase 5 offers a dual-write window, so an in-place migration is possible without
downtime — but it should be documented as a supported path only after S1–S10 pass on
a migrated database.

---

## 22. Things we should remove

Aggressively, as asked. Each with what replaces it.

| # | Remove | Why | Replaced by |
|---|---|---|---|
| 1 | **`lease_expires_at` per execution** | ~20 k writes/s for one bit of information about a *process* | node lease + epoch (ADR-B) |
| 2 | **`markFired` as a separate round trip** | one commit per execution for one timestamp | folded into the claim `UPDATE` |
| 3 | **`running_execution_count`** | hot row per capped job, inside the claim transaction; leaks; needs ADR-0025 to repair | derived count over `mohs_lease` (ADR-D) |
| 4 | **ADR-0025 (reaper releases the slot)** | exists only to repair #3 | deleting the lease *is* releasing the slot |
| 5 | **ADR-0026's constraint** (reaper fails terminally) | exists only because retry scheduling did not | re-queue is an insert |
| 6 | **`mohs_batches` table + counters** | four defects, two incurable | `correlation_id` + derived progress (ADR-I) |
| 7 | **`BatchCompletionCallbacks` in-memory map** | leaks (N−1)/N in a cluster; loses callbacks in a race | durable follow-up job + conflict-insert election |
| 8 | **`RETRY_SCHEDULED` as a claimable state** | forced a two-value `IN` into the claim index predicate; measured 3× throughput regression on MySQL (no plan regression on PostgreSQL — §4.3) | ready row with future `visible_at` |
| 9 | **`pollCancelRequests` every tick** | a query per node per 50 ms for an event that happens daily | `NOTIFY` + 5 s backstop |
| 10 | **H2 as a production backend** | its `FOR UPDATE SKIP LOCKED` double-locks ~33% of the time | test-only (ADR-H) |
| 11 | **The single serial `tick()`** | head-of-line blocking; needs a `leaseTtl/4` budget to avoid invalidating its own liveness | four loops, bounded queues |
| 12 | **ADR-0039's arithmetic claim clamp** | backpressure as a counter in one thread | a full bounded queue |
| 13 | **`claimRounds` / `batchSize` / `pollInterval` as three separate knobs** | three ways to say "how much work per unit time"; the BASELINE tuning table is evidence they are not independently meaningful | one `targetInFlight` + adaptive poll; the rest derived |
| 14 | **Delete-based retention with UUIDv7 frontier arithmetic and lock-escalation-sized batches** | intricate machinery for an O(1) operation | `DROP TABLE <partition>` |
| 15 | **Eight storage ports** | proliferation caused by the schema not matching the concepts | four (§18.3) |
| 16 | **`orphaned` + `paused` + `retired` as three booleans** | three columns encoding one lifecycle, with implicit precedence | one `status` enum with an explicit transition table |
| 17 | **Event dropping on executor saturation** | silently loses observations; was structurally unsafe for `BatchCompleted` | bounded queue + a *counter metric*; `BatchCompleted` no longer rides this path at all |
| 18 | **`GET /overview/stream` full-snapshot poll** | *not removed yet* — ADR-0046 decided correctly to measure first. Listed so it is not forgotten. | deltas from the completion batcher, **after** measurement |

---

## 23. Things we should NOT build

Attractive, and wrong for this system. Each with the reason it is tempting and the
reason it loses.

| Idea | Tempting because | Why not |
|---|---|---|
| **Microservices / a scheduler service** | "separation of concerns"; independent scaling | Destroys the transactional outbox — the product's single best property. Adds network partitions to a design that currently has none. Nodes already scale independently. |
| **Kafka / RabbitMQ as the queue** | "queues are for queueing" | Delayed delivery is not a broker primitive; we would rebuild the timer *and* run a broker. Breaks the outbox. Adds a dependency the host may not have. Our ceiling (~200 k/s) is far past any current requirement. |
| **Redis for coordination or rate limits** | fast counters, `INCR`, Lua | A second stateful dependency, a second consistency domain, and a second thing to operate — to replace one `UPDATE` per 5 s and an in-memory semaphore. |
| **Event sourcing the execution lifecycle** | perfect audit; natural fit for state machines | Multiplies write volume on the hottest path (the exact thing this redesign is removing) and forces a projection to answer "what is the state" — the most common query. `mohs_attempt` already *is* the append-only audit log for the part that matters. |
| **CQRS with a separate read store** | dashboard queries never touch the hot path | The table split already achieves the isolation, with no second store, no replication lag and no consistency lecture in the docs. Revisit only if S5 fails at 10⁹ rows. |
| **Distributed locks (ZooKeeper/etcd/Redlock)** | "we need coordination" | We do not. Every exclusion is a PK conflict or a guarded CAS on a database that is already the consistency authority. Redlock in particular is unsafe under the exact GC-pause scenario we must survive. |
| **Leader election for scheduling** | one materialiser, no duplicate work | CAS on the trigger already gives per-trigger exclusion with no leader to lose. Election is used for exactly one thing (retention, §15.4) because that is the only genuinely-must-run-once bulk task. |
| **Consensus (Raft) among nodes** | strong guarantees | The database is the consensus. Adding a second one means two truths that can disagree. |
| **Reactive / WebFlux / R2DBC** | fashion; "non-blocking is faster" | Cannot join the host's JDBC transaction, would infect the host's programming model, and virtual threads deliver the concurrency without any of it (§12.2). |
| **Job dependency DAGs** | users ask for it | That is a workflow engine (§5.9). Doing it badly is worse than not doing it. |
| **Spring-Batch-style chunking/checkpointing inside Mohs** | "we have batches" | Different problem domain. The answer is "run Spring Batch inside a Mohs job" (§10.3). |
| **A generic query API for executions** | flexible dashboards | Arbitrary predicates over a partitioned billion-row table produce arbitrary plans. Fixed, indexed filters only (§16.1). |
| **Capability tags / affinity / bin-packing placement** | "smart scheduling" | Requires a placement decision, which requires a decider, which is a bottleneck and a thing that can be wrong. Handler-aware claiming (§11.1) covers the real use case with one predicate. |
| **A distributed cache in front of the DB** | fewer queries | The queries are already cheap by construction; the cache would add a consistency domain to save work we removed. |
| **Configurable everything** | flexibility | Every knob is a support conversation and a combinatorial test case. The BASELINE tuning table shows `poll`/`batch`/`rounds` are not independently meaningful — collapse them (§22 #13). New knobs must justify themselves with a *measured* trade-off, which in this document only `flushOnEveryResult` does. |
| **Exactly-once execution** | users want it | Impossible for arbitrary side effects. Promising it is the most dangerous thing a scheduler can do. Ship `(executionId, attemptNumber)` + the outbox and explain effectively-once honestly (§6.7). |

---

## 24. Final recommendation

### 1. What should Mohs become?

**The durable job scheduler you can join to your own transaction** — a library, not a
platform. Its differentiator is not throughput (though it should be the fastest in the
JVM ecosystem at this shape); it is that `schedule()` inside `@Transactional` commits
or rolls back with your business write, with no broker, no outbox table you maintain,
and no second system to operate. Temporal cannot do this because it is a service.
Quartz, JobRunr and db-scheduler can approach it but do not build their contract
around it. **Mohs should make that property the headline and let the architecture
defend it** — which is precisely why §14.1 rejects extraction into a service and
§12.2 rejects R2DBC.

### 2. Core architectural principles

1. **One table, one write profile.** The four-table split is the foundation.
2. **Nothing hot is a counter.** Derive it, or lease it.
3. **Coordination is amortised or absent.** Group commits; leased budgets; no
   distributed locks.
4. **Liveness is per process, not per work item.**
5. **Every exclusion is a CAS or a primary key** — never a lock held across logic.
6. **Backpressure is structural** (a full bounded queue), never arithmetic.
7. **State machines are explicit; transitions are named, guarded operations.**
8. **The database is the only shared dependency, and it is the host's.**
9. **Every guarantee is stated with its bound**; anything unbounded is a bug.
10. **Measure, then decide.** BASELINE outranks intuition — including this document's.

### 3. Technologies to use

Java 25 (virtual threads, records, sealed types, pattern matching, `ScopedValue`);
Spring Boot 4 (auto-configuration and JDBC only — the framework stays at the edges);
plain JDBC with explicit SQL; PostgreSQL as Tier 1; Flyway; Micrometer +
OpenTelemetry; React + TypeScript for the dashboard; JMH + Testcontainers for the
benchmark module.

### 4. Technologies to avoid

Any message broker; any second datastore (Redis, etcd, ZooKeeper); R2DBC/WebFlux; any
ORM on the hot path; any distributed-lock library; `--enable-preview` in shipped
artifacts; GraalVM native image as a *requirement* (fine as a host's choice).

### 5. Biggest architectural risks

1. **Phase 5 is most of the engine.** Mitigated by shadow-read/dual-write and by
   E1/E2 gating it.
2. **ADR-B changes failure detection** — the mechanism most likely to be subtly wrong.
   E6 chaos testing is a gate, not a follow-up.
3. **The four-table split moves complexity from one row to four tables.** If the
   invariant "ready XOR lease" is ever violated by a code path that forgets the
   transaction, work is lost or duplicated. Mitigation: it is expressible as a single
   SQL assertion, and it should run as a continuous invariant check in the chaos
   suite.
4. **Tiering the dialects is a public commitment** that will disappoint someone. Make
   it explicitly and early rather than by degradation.
5. **Scope.** This plan is ~12 sprints. The highest-value 40% is Phases 3 and 4, which
   need no schema split. **If only one thing ships, ship Phase 3.**

### 6. Biggest performance risks

1. **WAL fsync is the floor.** Group commit amortises it; nothing removes it. If E1
   shows WAL bytes/execution does not drop, the 12 k/s target is not reachable and
   should be restated.
2. **`mohs_ready`/`mohs_lease` bloat** if autovacuum cannot keep up with insert/delete
   churn. The `fillfactor` and per-table autovacuum settings are the mitigation and
   must be verified under S3 (burst), not just steady state.
3. **Partition maintenance** — a missing future partition is an outage. Creation must
   be automatic, run ahead by N periods, and alarm loudly.
4. **The derived concurrency count** is one extra query per round; if `mohs_lease`
   ever grows beyond cache residency (it should not — it is bounded by in-flight
   work), it becomes a hot-path scan.
5. **`NOTIFY` storms** at high enqueue rates. Mitigation: notify per shard with
   coalescing, and it is best-effort, so dropping notifications under load is correct
   behaviour, not degradation.

### 7. What to redesign first

**Phase 3 (group commit + `markFired`)** — highest measured gain, no schema change,
runtime-reversible. Then **Phase 4 (node lease)** — largest single write reduction in
the system. Both are independent of the table split, so they deliver value even if E1
kills ADR-A.

### 8. What must be validated experimentally

E1–E6 (§20.3), in that order, with E1 and E2 gating Phase 5 and E6 gating Phase 4.
The three numbers that do not exist today and must exist before anything else —
**commits per execution, tuple versions per execution, WAL bytes per execution** —
are Phase 0's only deliverable.

### 9. The architecture at 10× (≈ 40 k exec/s, ~8 nodes)

Unchanged in shape. Four tables, four loops, sharded ready set, group commit, token
leases. Operationally: hourly instead of weekly partitions; `mohs_ready` still fits in
cache; the engine pool stays at ~32 connections per node. **The design already targets
this point** — that is what the 12 k/s single-node and 6× scaling targets add up to.

### 10. The architecture at 100× (≈ 400 k exec/s, ~64 nodes)

This is past the declared wall (§8.5) and the shape *does* change:

- **The single PostgreSQL primary becomes the bottleneck** at WAL fsync, somewhere
  around 200 k exec/s. Below that, add nodes; above it, adding nodes makes things
  worse.
- **The answer is horizontal database partitioning**, and Mohs is unusually well
  placed for it: it is embedded, so each host instance already points at its own
  `DataSource`. A 100× deployment is *N independent Mohs clusters, one per database
  shard*, with the host routing by tenant or by job family. Shard count 64 already
  exists in the design and maps naturally onto physical shards.
- **What we would need to add:** a cross-shard read API for the dashboard (scatter-
  gather over N clusters) and cross-shard rate limits (per-shard budget = `max/N`,
  accepting Temporal-style approximation *at that scale only*, where the exactness
  matters less than the ceiling).
- **What we would still refuse:** a coordination layer between shards. At 400 k
  exec/s the correct architecture is N independent systems that never talk to each
  other, not one system with a consensus protocol. If a workload genuinely needs
  global ordering or cross-shard transactions at that rate, it needs a log, and that
  is Kafka's product, not ours.

---

### Closing

The current Mohs is not badly built — it is *well* built against an assumption that
has run out of room: that one row can be a queue entry, a lease, and a history record
at once. Every hot row, every counter, every extra commit and every one of the four
documented batch defects traces back to that single assumption. Removing it makes the
system **smaller**: fewer ADRs, fewer ports, fewer states, fewer knobs, fewer failure
modes, and roughly three times the throughput.

That is the test this plan should be judged by. If the redesign ends up with more
components than it started with, it failed.
