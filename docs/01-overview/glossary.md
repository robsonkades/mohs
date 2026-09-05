# Glossary

Status: Active · Last Reviewed: 2026-08-29 · Source of Truth: Repository

The vocabulary is deliberate and consistent across code, SQL and the REST wire format. Where two
words look interchangeable in English, they mean different things here.

## Core vocabulary

| Term | Meaning | Type |
| --- | --- | --- |
| **Job** | A definition: a handler, a schedule, and policies. Defined once, invoked N ways. | `JobDefinition` |
| **JobKey** | A job's stable identity and persistence key. Never changes; renaming the display label does not change it. | `JobKey` |
| **JobRef&lt;T&gt;** | A typed reference binding a `JobKey` to its payload class at compile time. | `JobRef` |
| **Handler** | The method that processes one execution. Resolved from `@MohsJob` at boot. | `JobHandler` |
| **Schedule** | *When* a job fires — and nothing else. Policies live on the definition. | sealed `Schedule` |
| **Trigger** | The armed state of a recurring schedule: the `next_fire_at` column. "Firing a trigger" means advancing it and materialising occurrences. | `mohs_job_definitions.next_fire_at` |
| **Occurrence** | One firing produced by a trigger. Carries `actor = "scheduler"`. | `Execution` row |
| **Execution** | One invocation of a job, from enqueue to a terminal state. Survives retries — its id never changes. | `Execution`, `ExecutionId` |
| **Attempt** | One try of an execution. 1-based; a retry increments the attempt number. | `Attempt` |
| **Payload** | The data handed to the handler. Serialised as JSON with its concrete class name. | `mohs_execution.payload` / `payload_type` |
| **Actor** | Who caused the invocation. Non-negotiable on every execution. `"scheduler"` is reserved for the engine. | `Execution.actor` |

## Queue and ownership

| Term | Meaning |
| --- | --- |
| **Queue** (`mohs_ready`) | The backlog. One row per execution that is due or waiting to become due. Its size is the backlog, never the history. |
| **Ready entry** | A queue row: id, job key, shard, priority, the attempt it *will become*, and `visible_at`. |
| **Visibility rule** | The queue's single admission predicate: `visible_at <= now`. Retry, delay, requeue and immediate enqueue are the same operation with a different `visible_at`. |
| **Claim** | The transaction that removes a ready entry and inserts an ownership row. Atomic by construction: there is no instant at which an execution is neither queued nor owned. |
| **Lease** (`mohs_lease`) | Ownership. Exactly the work executing across the cluster. Deleting the lease *is* releasing the slot. |
| **Fence / fencing token** | The triple `(node_id, epoch, attempt)` carried by every write over owned work. A revived zombie carries an old epoch, or an old attempt on the same node, and loses every write. |
| **Epoch** | A node's incarnation counter. Starts at 1 and rises only when the node itself observes that its lease had expired. |
| **Shard** | `FNV-1a(execution_id) mod 64`. A pure function of the id — never a transported value. |
| **Shard assignment** | Derived, not negotiated: each node sorts the live node ids, finds its own index `i` of `n`, and owns `{ s : s mod n == i }`. |
| **Admission** | The per-job guards applied around a claim round: execution window, concurrency cap, rate limit. |
| **Inadmissible list** | The set of job keys excluded from *this* round, computed in memory before the round and pushed into the claim SQL as a `NOT IN` filter. |
| **Requeue** | Returning a claimed execution to the queue with the *same* attempt number, without consuming retry budget. |

## Liveness and recovery

| Term | Meaning |
| --- | --- |
| **Node** | One JVM running the engine. Its id is a UUIDv7 generated per `Engine` instance. |
| **Heartbeat** | One `mohs_nodes` upsert per tick, carrying the node's state, epoch and lease expiry. |
| **Node lease** | The promise `expires_at = now + node-lease-ttl` written by each heartbeat. The liveness authority. |
| **Reaper** | The tick step that finds leases owned by nodes absent from the live set and resolves them through the retry budget. |
| **Orphaned lease** | A lease whose owner is not alive. |
| **Stray lease** | A lease held by *this* node with no in-flight incarnation — work lost between claim and dispatch. Recovered by `reconcileOwnStrayLeases`. |
| **Zombie** | A handler still running after its node released or lost ownership. Its result is discarded by the fence. |
| **Watchdog bound** | An optional runtime ceiling (`mohs.engine.watchdog-timeout`). On breach the node releases ownership; the local handler keeps running as a zombie. |
| **Drain** | Stop accepting new claims and wait for in-flight work. A drain is not a cancel. |
| **Escalation** | What happens when the drain grace expires: flag plus interrupt on everything still in flight. |

## Scheduling vocabulary

| Term | Meaning |
| --- | --- |
| **Fixed rate** | `IntervalSpec(afterFinish=false)`. The next firing is anchored to the *scheduled* time, so the series does not drift. |
| **Fixed delay** | `IntervalSpec(afterFinish=true)`. The next firing is anchored to the *end* of the previous execution; the trigger is disarmed (`NULL`) while one is in flight and rearmed inside the completion transaction. |
| **Misfire** | A firing older than `misfire-threshold` that was never materialised. A firing *within* the threshold is merely late and fires under any policy. |
| **Misfire policy** | `IGNORE` (skip), `FIRE_NOW` (one compensating firing), `FIRE_ALL_MISSED` (replay each, capped at 1,440 per cycle). |
| **Execution window** | A named set of exclusion predicates. A job whose scheduled time falls inside any exclusion does not fire. Code-only; there is no property form. |
| **Rate limit** | A named, cluster-wide token bucket bounding a job's firing rate. Capacity `max`, one token every `window/max`. |

## Runtime resources

| Term | Meaning |
| --- | --- |
| **Runner** | A named, node-local execution capability. A *specification*, never an `Executor` — Mohs creates and owns the threads. |
| **IO runner** | Virtual thread per task, with a real ceiling enforced by a semaphore. Above the ceiling the executor **rejects**. |
| **CPU runner** | A bounded platform-thread pool with an explicit queue capacity and an abort policy. |
| **Dispatch concurrency** | The node's ceiling on in-flight executions. It also bounds each claim. |
| **Group commit** | Batched completion: results accumulate for 256 items or 5 ms and commit in one transaction. |

## Read-model vocabulary

| Term | Meaning |
| --- | --- |
| **Advisory state** | The `state` column in `mohs_execution`. `PENDING` until terminal. While work is in flight the *truth* is the queue and the lease, not this column. |
| **Derived state** | What the API returns: lease present means `RUNNING`; a queue entry with `attempt > 1` still invisible means `RETRY_WAITING`; any other queue entry means `ENQUEUED`; a terminal column means itself. |
| **Snapshot** | A public read model (`JobSnapshot`, `NodeSnapshot`, `RunnerSnapshot`, `RateLimitSnapshot`, `BatchSnapshot`, `OverviewSnapshot`). |
| **Throughput reading** | A count of terminal attempts plus the window it was counted over, so a consumer can derive a rate. |
| **Receipt** | What a schedule returns: an `Enqueued` event carrying a durable `ExecutionId`. Never a `Future` of the result. |

## Acronyms and external references

| Term | Meaning |
| --- | --- |
| **CAS** | Compare-and-set. Here always a guarded SQL `UPDATE … WHERE <observed value>`. |
| **RFC 7807** | The Problem Details format used for every REST error (`application/problem+json`). |
| **SSE** | Server-Sent Events, used by `GET /overview/stream`. |
| **JSpecify** | The nullness annotation set; every production package is `@NullMarked`. |
| **UUIDv7** | Time-ordered UUID, used for every generated primary key. |
| **FNV-1a** | The hash used for sharding. Chosen over `String.hashCode()` because the value must be re-derivable by any JVM, forever. |
