# Schema reference

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository (`mohs-store-jdbc/src/main/resources/schema-*.sql`)

Selected DDL for PostgreSQL; use the linked installers for execution, not this excerpt. Type differences per dialect are listed at the end; the
authoritative files are `schema-postgresql.sql`, `schema-h2.sql`, `schema-mysql.sql` and
`schema-sqlserver.sql`. PostgreSQL has a structural test comparing installer and delta chain;
all production dialects have store round-trip tests.

- [PostgreSQL installer](../../mohs-store-jdbc/src/main/resources/schema-postgresql.sql)
- [MySQL installer](../../mohs-store-jdbc/src/main/resources/schema-mysql.sql)
- [SQL Server installer](../../mohs-store-jdbc/src/main/resources/schema-sqlserver.sql)
- [H2 installer](../../mohs-store-jdbc/src/main/resources/schema-h2.sql)

## Control plane

```sql
CREATE TABLE IF NOT EXISTS mohs_job_definitions (
    id                          VARCHAR(255) PRIMARY KEY,
    job_key                     VARCHAR(255) NOT NULL UNIQUE,
    name                        VARCHAR(255),
    handler_type                VARCHAR(500) NOT NULL,
    schedule_type               VARCHAR(20)  NOT NULL,      -- CRON | INTERVAL | ON_DEMAND
    cron_expression             VARCHAR(255),
    cron_zone                   VARCHAR(100),
    interval_duration           VARCHAR(50),                -- ISO-8601
    interval_after_finish       BOOLEAN,
    runner                      VARCHAR(255),
    window_name                 VARCHAR(255),
    rate_limit                  VARCHAR(255),
    misfire                     VARCHAR(20)  NOT NULL,
    start_paused                BOOLEAN      NOT NULL DEFAULT FALSE,   -- definitional
    allow_concurrent_executions BOOLEAN      NOT NULL DEFAULT TRUE,
    max_concurrent_executions   INT          NOT NULL DEFAULT 0,
    retries                     INT          NOT NULL DEFAULT 0,
    timeout                     VARCHAR(50),                -- ISO-8601
    retry_policy                VARCHAR(255),               -- named RetryPolicy bean
    source                      VARCHAR(20)  NOT NULL,      -- ANNOTATION | PROGRAMMATIC
    orphaned                    BOOLEAN      NOT NULL DEFAULT FALSE,   -- operational
    paused                      BOOLEAN      NOT NULL DEFAULT FALSE,   -- operational
    retired                     BOOLEAN      NOT NULL DEFAULT FALSE,   -- operational
    next_fire_at                TIMESTAMP,                  -- the trigger's state; NULL = disarmed
    created_at                  TIMESTAMP    NOT NULL,
    updated_at                  TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS mohs_batches (
    id         VARCHAR(255) PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,      -- the caller's label; durable data
    total      INT NOT NULL DEFAULT 0,
    succeeded  INT NOT NULL DEFAULT 0,
    failed     INT NOT NULL DEFAULT 0,     -- pending is DERIVED, never stored
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_duration VARCHAR(50) NOT NULL,  -- ISO-8601
    tokens          INT NOT NULL,          -- balance as of the last charge
    refilled_at     TIMESTAMP NOT NULL     -- refill applied in memory at read time
);

CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at TIMESTAMP   NOT NULL,
    epoch             BIGINT      NOT NULL DEFAULT 0,   -- the node's incarnation
    expires_at        TIMESTAMP                          -- the node LEASE; NULL = pre-lease jar
);
```

## Hot path — the queue

```sql
CREATE TABLE IF NOT EXISTS mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0,   -- FNV-1a(execution_id) mod 64
    priority     INT          NOT NULL DEFAULT 20,  -- lower claims first
    attempt      INT          NOT NULL,             -- the attempt this entry BECOMES
    visible_at   TIMESTAMPTZ  NOT NULL              -- the single admission rule
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);

-- The column order IS the claim's ORDER BY.
CREATE INDEX IF NOT EXISTS idx_mohs_ready_claim ON mohs_ready (shard, priority, visible_at);
```

## Hot path — ownership

```sql
CREATE TABLE IF NOT EXISTS mohs_lease (
    execution_id     VARCHAR(255) PRIMARY KEY,
    job_key          VARCHAR(255) NOT NULL,
    node_id          VARCHAR(255) NOT NULL,          -- fencing token, part 1
    epoch            BIGINT       NOT NULL,          -- fencing token, part 2
    attempt_number   INT          NOT NULL,
    priority         INT          NOT NULL DEFAULT 20,  -- lets the reaper rebuild a requeue entry
    claimed_at       TIMESTAMPTZ  NOT NULL,
    cancel_requested BOOLEAN      NOT NULL DEFAULT FALSE
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);

CREATE INDEX IF NOT EXISTS idx_mohs_lease_node ON mohs_lease (node_id, epoch);  -- the reaper
CREATE INDEX IF NOT EXISTS idx_mohs_lease_job  ON mohs_lease (job_key);         -- the derived cap
```

## History

```sql
CREATE TABLE IF NOT EXISTS mohs_execution (
    execution_id    VARCHAR(255) NOT NULL,
    job_key         VARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL,      -- ADVISORY: 'PENDING' until terminal
    scheduled_at    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,      -- when the ROW was born
    finished_at     TIMESTAMPTZ,
    actor           VARCHAR(255) NOT NULL,
    correlation_id  VARCHAR(255),               -- the batchId
    idempotency_key VARCHAR(255),
    payload         TEXT         NOT NULL,
    payload_type    VARCHAR(500) NOT NULL,
    PRIMARY KEY (execution_id)
);

CREATE INDEX IF NOT EXISTS idx_mohs_execution_job ON mohs_execution (job_key, execution_id DESC);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_corr ON mohs_execution (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS mohs_attempt (
    execution_id VARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      VARCHAR(255) NOT NULL,     -- forensics: WHO executed this attempt
    started_at   TIMESTAMPTZ  NOT NULL,
    finished_at  TIMESTAMPTZ  NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,     -- SUCCEEDED | FAILED | CANCELLED
    error_type   VARCHAR(500),              -- the exception CLASS
    error        TEXT,                      -- the MESSAGE only
    PRIMARY KEY (execution_id, number)
);

CREATE INDEX IF NOT EXISTS idx_mohs_attempt_throughput ON mohs_attempt (finished_at, outcome);
```

All four dialects use `mohs_execution(execution_id)` and
`mohs_attempt(execution_id, number)` as primary keys. The throughput index supports
windowed attempt counts; the job and correlation indexes support execution listings and batches.

## Idempotency

```sql
CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)   -- the PK conflict IS the dedup check
);

CREATE INDEX IF NOT EXISTS idx_mohs_idempotency_created ON mohs_idempotency (created_at);
```

SQL Server instead:

```sql
CREATE TABLE mohs_idempotency (
    job_key         NVARCHAR(255) NOT NULL,
    idempotency_key NVARCHAR(255) NOT NULL,
    execution_id    NVARCHAR(255) NOT NULL,
    created_at      DATETIME2     NOT NULL,
    CONSTRAINT pk_mohs_idempotency PRIMARY KEY NONCLUSTERED (job_key, idempotency_key)
);
CREATE CLUSTERED INDEX ix_mohs_idempotency_created ON mohs_idempotency (created_at);
```

`2 × (255 × 2) = 1020` bytes exceeds the 900-byte ceiling for a clustered key. Measured: 225+225
characters inserts, 256+255 fails with `Msg 1946` at enqueue time.

## No history table

There is none. `mohs_schema_history` went away with the migration engine, and nothing replaced it:
**nothing records which schema versions a database has already seen.** Tracking that is the
operator's, and the options are in
[installing and upgrading the schema](migrations.md#which-versions-have-you-applied).

## Dialect type map

| Concept | PostgreSQL | H2 | MySQL | SQL Server |
| --- | --- | --- | --- | --- |
| Short text | `VARCHAR(n)` | `VARCHAR(n)` | `VARCHAR(n)` | `NVARCHAR(n)` |
| Long text | `TEXT` | `TEXT` | `MEDIUMTEXT` | `NVARCHAR(MAX)` |
| Boolean | `BOOLEAN` | `BOOLEAN` | `BOOLEAN` | `BIT` |
| Control-plane time | `TIMESTAMP` | `TIMESTAMP` | `DATETIME(6)` | `DATETIME2` |
| Split-table time | `TIMESTAMPTZ` | `TIMESTAMP` | `DATETIME(6)` | `DATETIME2` |
| 64-bit integer | `BIGINT` | `BIGINT` | `BIGINT` | `BIGINT` |
| Small integer | `SMALLINT` | `SMALLINT` | `SMALLINT` | `SMALLINT` |

## Constraints inventory

| Kind | Present |
| --- | --- |
| Primary keys | On all nine tables |
| Unique | `mohs_job_definitions.job_key`; `mohs_idempotency`'s PK |
| **Foreign keys** | **None on the split tables.** The legacy single-table schema had them; they went with `V4` |
| Check constraints | **None.** All value invariants are enforced in Java compact constructors |
| Not null | As shown |
| Defaults | `priority = 20`, `shard = 0`, `epoch = 0`, and the boolean flags |

The absence of foreign keys is a deliberate consequence of the write-profile split: an FK from
`mohs_ready` to `mohs_execution` would add an index maintenance cost and a lock dependency to the
hottest insert path, and the relationship is already guaranteed by the enqueue unit's atomicity.
