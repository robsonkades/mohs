-- Phase 5 do redesign (ADR-A) — ver o V3 do Postgres para o racional das
-- quatro tabelas. SQL Server é Tier 2 (ADR-0050): equivalente funcional,
-- sem partições nesta fase (retenção continua no mecanismo ADR-0032;
-- particionamento nativo chega com trigger medido). Guardas T-SQL de
-- statement único (mesma forma da V1/V2); índices INLINE no CREATE —
-- tabela nova ou nada, sem estado intermediário a guardar.

IF OBJECT_ID('mohs_ready') IS NULL
CREATE TABLE mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0,
    priority     INT          NOT NULL DEFAULT 20,
    attempt      INT          NOT NULL,
    visible_at   DATETIME2    NOT NULL,
    INDEX idx_mohs_ready_claim NONCLUSTERED (shard, priority, visible_at)
);

IF OBJECT_ID('mohs_lease') IS NULL
CREATE TABLE mohs_lease (
    execution_id     VARCHAR(255) PRIMARY KEY,
    job_key          VARCHAR(255) NOT NULL,
    node_id          VARCHAR(255) NOT NULL,
    epoch            BIGINT       NOT NULL,
    attempt_number   INT          NOT NULL,
    claimed_at       DATETIME2    NOT NULL,
    cancel_requested BIT          NOT NULL DEFAULT 0,
    INDEX idx_mohs_lease_node NONCLUSTERED (node_id, epoch),
    INDEX idx_mohs_lease_job NONCLUSTERED (job_key)
);

IF OBJECT_ID('mohs_execution') IS NULL
CREATE TABLE mohs_execution (
    execution_id    VARCHAR(255) PRIMARY KEY,
    job_key         VARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL,
    scheduled_at    DATETIME2    NOT NULL,
    created_at      DATETIME2    NOT NULL,
    finished_at     DATETIME2,
    actor           VARCHAR(255) NOT NULL,
    correlation_id  VARCHAR(255),
    idempotency_key VARCHAR(255),
    payload         VARCHAR(MAX) NOT NULL,
    payload_type    VARCHAR(500) NOT NULL,
    INDEX idx_mohs_execution_created NONCLUSTERED (created_at),
    INDEX idx_mohs_execution_job NONCLUSTERED (job_key, created_at DESC),
    INDEX idx_mohs_execution_corr NONCLUSTERED (correlation_id)
);

IF OBJECT_ID('mohs_attempt') IS NULL
CREATE TABLE mohs_attempt (
    execution_id VARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      VARCHAR(255) NOT NULL,
    started_at   DATETIME2    NOT NULL,
    finished_at  DATETIME2    NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,
    error_type   VARCHAR(500),
    error        VARCHAR(MAX),
    PRIMARY KEY (execution_id, number),
    INDEX idx_mohs_attempt_throughput NONCLUSTERED (finished_at, outcome)
);

IF OBJECT_ID('mohs_idempotency') IS NULL
CREATE TABLE mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      DATETIME2    NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);
