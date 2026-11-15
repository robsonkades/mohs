# Indexes

Status: Active · Last reviewed: 2026-09-10 · Source of truth: canonical JDBC installers.

The [data model](data-model.md) describes the tables behind these indexes. Use the [benchmark harnesses](../10-performance/benchmarks.md) and actual query plans to measure the current candidate pages on the target database.

| Index or key | Columns / scope | Main operation |
|---|---|---|
| `mohs_work` PK | `execution_id` | Fenced work transition, cancellation and history join |
| `idx_mohs_work_claim` | READY: `(shard, priority, visible_at, execution_id)` | Ordered acquisition inside one shard and candidate-job page |
| `idx_mohs_work_candidates` | READY: `(shard, job_key, visible_at)` | Stable keyset page of visible job identities |
| `idx_mohs_work_node` | LEASED: `(node_id, claimed_at, execution_id)` | Ownership lookup and recovery |
| `idx_mohs_work_job` | LEASED: `job_key` | Authoritative occupancy under the job lock |
| `mohs_execution` PK | `execution_id` | Snapshot/result lookup, latest-attempt update, global cursor |
| `idx_mohs_execution_job` | `(job_key, execution_id DESC)` | Per-job execution pages |
| `idx_mohs_execution_batch` | `batch_id` | Batch members and retention |
| `idx_mohs_execution_outcome` | `(finished_at, state)` | Terminal summaries and history cutoff |
| `mohs_idempotency` PK | `(job_key, idempotency_key)` | Acceptance arbitration and receipt lookup |
| Receipt expiry index | `expires_at` | Expired-receipt cleanup, independent of history |
| `mohs_job_definitions` PK | `job_key` | Definition locks, typed acceptance and bulk policy reads |
| `idx_mohs_job_executable` | `(executable_key, retired, job_key)` | Active dependent recurrences |
| `idx_mohs_job_next_fire` | `next_fire_at` | Due triggers and nearest future active trigger |
| `mohs_catalog` PK | `singleton` | Catalog publication/session-entry mutex |
| `mohs_nodes` PK | `node_id` | Session registration, renewal and revocation |
| `mohs_batches` PK | `id` | Atomic counters and batch lookup |
| `mohs_rate_limits` PK | `name` | Bucket read/CAS and configuration |

PostgreSQL and SQL Server use filtered work indexes for READY/LEASED. MySQL and H2 prefix the corresponding indexes with `work_state`, since these installers do not use filtered indexes. PostgreSQL filters non-null batch membership; PostgreSQL and SQL Server filter non-null terminal finish time in the outcome index.

The receipt expiry index is `idx_mohs_idempotency_expires` on PostgreSQL, MySQL and H2. SQL Server uses a clustered expiry index, currently named `ix_mohs_idempotency_created`, and a nonclustered receipt primary key. Its definition indexes expiry despite that historical name.

## Reading the access paths

The claim uses equality on state/shard, candidate-job membership, visibility and priority order. That does not force every optimizer to choose the ordering index: filtering a small job subset or a small table can favor another access path. Row locks still require table access even when metadata is covered. Payload and error text are absent from work indexes.

Candidate paging orders job keys and advances its cursor even when the page is excluded by windows. It limits returned identities to 128 per probe. It does **not** guarantee that only 128 index entries are examined: duplicate job keys, visibility filtering, statistics and database DISTINCT strategies affect scan work. Statements have a three-second deadline. Validate deep visible/future backlogs with actual plans before claiming a capacity bound.

A single job's positive concurrency cap is serialized by its definition row. `idx_mohs_work_job` serves the occupancy read, but an index cannot remove that deliberate serialization. Multiple jobs sharing a rate limit can also contend on one bucket CAS. These are distinct from work-row lock contention.

## Evidence and changes

The frozen comparison collected PostgreSQL actual plans with buffers/WAL, MySQL SELECT EXPLAIN ANALYZE, and SQL Server actual plans with IO messages. Its plan population had 256 active rows and preceded churn. Scan/sort choices in those files are observations, not proof that an index fails at larger cardinalities.

The candidate index was added with the coordinator paging change after the frozen comparison. It adds write and storage cost and needs measurement against the former read pattern on equivalent workloads. Do not transfer older results about queue-only indexes to this index without a new plan and churn run.

There are no SQL expressions on work identifiers or timestamps to repair incompatible persisted types. Parameters are normalized at the Java/JDBC boundary. SQL Server plans may still contain parameter-width, collation and timestamp-precision normalization; distinguish those from conversion of indexed table columns.
