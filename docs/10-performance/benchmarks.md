# Benchmarks and harnesses

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository (`mohs-benchmark`)

Everything measurable lives in `mohs-benchmark`, a module that is **never published**. There is no
JMH in the project.

## Two kinds of harness

| Kind | Form | Runs where |
| --- | --- | --- |
| **Scenarios** | JUnit classes named `*Scenario` | In-JVM, against a real PostgreSQL container via Testcontainers |
| **Scripts** | PowerShell (`pwsh`) | Against the demo application as a real process, or several |

Scenarios are **not** picked up by Surefire's default pattern, so a normal `./mvnw test` does not run
them. Invoke them by name.

## Scenarios

All run against `ScenarioCluster` — **N engines in one JVM** against the real PostgreSQL container.
Each `Engine` has its own `nodeId`, epoch, lease and tick loop, so from the database's point of view
they are N nodes and the **database arbitrates** claim, ownership and sharding. That is the fidelity
correctness scenarios need, and the one the shell scripts do not give for free: here the test
observes what each handler saw, **in memory**, rather than inferring it from logs.

The wiring deliberately mirrors `MohsAutoConfiguration` — group commit on at the same 256/5 ms, the
event executor at the same ceiling of 16, the same shutdown order. That is not fussiness: *a verdict
about lost work drawn from a wiring nobody runs in production is not admissible as release
evidence.*

### The nine scenarios

| Scenario | Question it answers |
| --- | --- |
| `NodeChurnScenario` | An everyday deploy: one node leaves via `stop(grace)`, a new one joins mid-drain. Asserts no loss, **bounded** redelivery (only what was in flight), and that the departed node's shards are claimed again — otherwise the queue would stall with ready work in front of it |
| `ColdStartScenario` | N nodes come up at once and find a full queue. It isolates a specific hypothesis: a node born *inside* the window between a peer reading `mohs_nodes` and running its reaper has already claimed work while being absent from the snapshot — and absent means dead by definition |
| `RollingUpdateScenario` | A deploy adds a job, so part of the cluster lacks the handler. Two separate cases, because they are two questions with different answers: (a) does work survive when the rollout finishes within the retry budget — the ship/no-ship question; and (b) what does a permanently blind node cost, asserted at the value it **has** today so it shouts when it gets worse |
| `ShutdownLatencyScenario` | What a SIGTERM costs with the node **full** — `dispatch-concurrency` executions in flight, slow handler. The declared ceiling is the drain contract: the floor is one handler's duration and the ceiling is `grace`; what it protects is the middle ground |
| `ConcurrentMigrationScenario` | N replicas call `migrate()` within the same window of microseconds (a `CountDownLatch`, not a sleep). Asserts exactly one applies each version and none fails to boot |
| `RecurringTriggerScenario` | Three nodes, one recurring job. Asserts no *duplicate occurrence* (two executions for one occurrence, which no idempotent consumer detects because their ids differ), measures **punctuality** (`started_at − scheduled_at` over real firings, not a synthetic enqueue), and asserts the burst on resume |
| `BatchCompletionScenario` | Two nodes completing the second-to-last and last members simultaneously must produce **one** `BatchCompleted`. Half the members fail on purpose, with `retries(1)` — which is what makes the assertion **falsifiable**: each failing member is invoked twice and must count once, separating "counted the terminal failure" from "counted every attempt" |
| `RateLimitCeilingScenario` | Does the cluster-wide ceiling hold under concurrency, **and** does an unlimited job pay for its limited neighbour? (A round that fails the CAS is undone entirely, and may contain unlimited executions.) The criterion is the token bucket's envelope, `t_k >= (k − max) × window/max`, not a sliding window — demanding the latter would demand a mechanism that was deliberately not chosen |
| `OverviewLatencyScenario` | The cost of `GET /overview` once the database is no longer small. It exposes that the endpoint's two reads have **opposite** cost profiles: the throughput count should cost the window, the backlog count costs the whole queue |

### Running one

```bash
./mvnw -pl mohs-benchmark test -Dtest=NodeChurnScenario
```

Requires Docker (Testcontainers starts PostgreSQL).

### Two declared divergences from production

Stated in `ScenarioCluster`'s Javadoc, and worth knowing before trusting a verdict:

1. **Process death is not expressible.** A node here dies through `stop()`, not with the carrier
   ripped out from under it. `kill -9` and freezes stay with `chaos-recovery.ps1`, which exists for
   exactly this reason.
2. **No connection pool.** `PostgresTestSupport` hands out a `PGSimpleDataSource`, so every statement
   pays TCP plus authentication. Every latency figure from a scenario is pessimistic by that amount.

## Scripts

Run with `pwsh` (PowerShell 7+), not Windows PowerShell 5.1.

### `chaos-recovery.ps1`

Starts and kills or freezes the demo application itself.

```powershell
pwsh mohs-benchmark/scripts/chaos-recovery.ps1 -Scenario S6
pwsh mohs-benchmark/scripts/chaos-recovery.ps1 -Scenario S8 -PauseSeconds 30
pwsh mohs-benchmark/scripts/chaos-recovery.ps1 -Scenario SUSPEND -SuspendSeconds 20
```

| Scenario | Injection | Pass criterion |
| --- | --- | --- |
| `S6` | `kill -9` mid-drain | 100% of the seed reaches a terminal state; re-executions **only** for what was `RUNNING` at the kill |
| `S8` | `docker pause` the database mid-drain | No loss, no exception storm, drain resumes, **no self-reap** |
| `SUSPEND` | Freeze node 1 for longer than `node-lease-ttl`; node 2's reaper reclaims; node 1 resumes as a zombie | Seed fully terminal; reclaim actually happened; **zero** executions with more than one `SUCCEEDED` attempt |

Measurement is by `mohs_attempt` timestamps, with a unique seed prefix per round.

**A known mode of `SUSPEND`, documented rather than hidden**: if the freeze catches the node
*mid-claim-transaction*, the batch's rows stay locked-but-uncommitted — invisible to the reaper (not
`RUNNING`) and skipped by other claims (`SKIP LOCKED`) until the frozen session ends. A run reporting
"reclaimed: 0" with a full drain right after resume is *that* mode, not a fence failure. It is
pre-existing, and the database-side mitigation is `idle_in_transaction_session_timeout`.

### `cluster-scale.ps1`

Starts and stops N node-processes on ports 8080+ by itself.

```powershell
pwsh mohs-benchmark/scripts/cluster-scale.ps1 -Mode Idle    -Nodes 4
pwsh mohs-benchmark/scripts/cluster-scale.ps1 -Mode Latency -Nodes 4 -Reset
foreach ($n in 1,2,4,4,2,1) {
  pwsh mohs-benchmark/scripts/cluster-scale.ps1 -Mode Drain -Nodes $n -Rounds 4 -Reset
}
```

| Mode | Measures |
| --- | --- |
| `Idle` | Pauses every definition (a genuinely idle cluster), waits for the backoff to settle at its ceiling, then reads `pg_stat_statements` for queries/s with a top-by-calls attribution |
| `Drain` | Seeds `SeedSize` ready executions with uniform shard distribution and measures throughput from the `mohs_attempt` window |
| `Latency` | Enqueues **one** execution at a time through node 0's REST API, spaced enough for the backoff to return to its ceiling, and attributes `scheduled_at → started_at` per dispatching node — which is how the local hand-off versus shard-owner poll split was measured |

Two methodological rules the script encodes, and both matter:

- **`-Reset`** truncates queue and history between cells. Without it the last cell measures a larger
  base than the first.
- **The palindromic order (`1,2,4,4,2,1`)** neutralises session drift. The honest comparison is
  1/2/4 in the *same* session with the *same* per-node configuration: what scales is the cluster, not
  the sizing of each node.

The first round of each cell is warm-up and is discarded.

### `write-amplification.ps1`

Measures commits, tuple versions and WAL bytes per execution, with the demo application running.

```powershell
pwsh mohs-benchmark/scripts/write-amplification.ps1
pwsh mohs-benchmark/scripts/write-amplification.ps1 -JobKey slow-job   # renewal-heavy workload
```

## Prerequisites

| Requirement | Note |
| --- | --- |
| Docker | Testcontainers starts PostgreSQL, MySQL and SQL Server |
| `pwsh` | PowerShell 7+; the scripts use syntax 5.1 does not have |
| A built demo | `./mvnw -pl mohs-demo -am install`, then `dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=target/cp.txt` |
| Free ports | 8080 and upwards, one per node |
| A `postgres` container | The scripts expect a container of that name |

## Harnesses that no longer exist

The single-table era's harnesses (`ClaimQueryLoadHarness`, `ClaimQueryExplainHarness`,
`ClaimIndexTuning`, `OverviewQueryExplain`, `FindPageQueryExplain`, `Liveness`,
`RateLimitContention`, `InVsJoin`, `BatchCounter*`, `TableSplitExperiment`) **went with the tables
they measured**. Their results are recorded in the project's performance history and the code lives
in git history.

New harnesses for the current model are written when a recorded trigger fires — which is the
project's stated policy: a harness exists to answer a question somebody is actually asking.

## Writing a new scenario

| Rule | Reason |
| --- | --- |
| Extend `ScenarioCluster` | It is what makes N in-JVM engines behave as N nodes to the database |
| Mirror `MohsAutoConfiguration`'s wiring | A verdict from a wiring nobody runs is not admissible |
| Name it `*Scenario` | So Surefire's default pattern does not pick it up |
| State the pass criterion in the class Javadoc | Every existing one does |
| Make the assertion **falsifiable** | `BatchCompletionScenario`'s `retries(1)` is the model: with zero budget, both correct and incorrect behaviours produce the same number and the assertion proves nothing |
| Declare the limitations | Every existing scenario has a "declared limitation" paragraph |
| Use latches, never sleeps, for synchronisation | A sleep is admissible only as a deliberate workload delay, and must say so |
