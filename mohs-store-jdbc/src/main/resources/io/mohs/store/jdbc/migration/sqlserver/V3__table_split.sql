-- Phase 5 do redesign — ver o V3 do Postgres para o racional das
-- quatro tabelas. SQL Server é Tier 2: equivalente funcional,
-- sem partições nesta fase (retenção continua no mecanismo de sempre;
-- particionamento nativo chega com trigger medido). Guardas T-SQL de
-- statement único (mesma forma da V1/V2); índices INLINE no CREATE —
-- tabela nova ou nada, sem estado intermediário a guardar.

IF OBJECT_ID('mohs_ready', 'U') IS NULL
CREATE TABLE mohs_ready (
    execution_id NVARCHAR(255) PRIMARY KEY,
    job_key      NVARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0,
    priority     INT          NOT NULL DEFAULT 20,
    attempt      INT          NOT NULL,
    visible_at   DATETIME2    NOT NULL,
    INDEX idx_mohs_ready_claim NONCLUSTERED (shard, priority, visible_at)
);

IF OBJECT_ID('mohs_lease', 'U') IS NULL
CREATE TABLE mohs_lease (
    execution_id     NVARCHAR(255) PRIMARY KEY,
    job_key          NVARCHAR(255) NOT NULL,
    node_id          NVARCHAR(255) NOT NULL,
    epoch            BIGINT       NOT NULL,
    attempt_number   INT          NOT NULL,
    priority         INT          NOT NULL DEFAULT 20, -- viaja fila->posse: o requeue do reaper reconstroi a entrada sem ler historia (S5.3)
    claimed_at       DATETIME2    NOT NULL,
    cancel_requested BIT          NOT NULL DEFAULT 0,
    INDEX idx_mohs_lease_node NONCLUSTERED (node_id, epoch),
    INDEX idx_mohs_lease_job NONCLUSTERED (job_key)
);

IF OBJECT_ID('mohs_execution', 'U') IS NULL
CREATE TABLE mohs_execution (
    execution_id    NVARCHAR(255) PRIMARY KEY,
    job_key         NVARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           NVARCHAR(20)  NOT NULL,
    scheduled_at    DATETIME2    NOT NULL,
    created_at      DATETIME2    NOT NULL,
    finished_at     DATETIME2,
    actor           NVARCHAR(255) NOT NULL,
    correlation_id  NVARCHAR(255),
    idempotency_key NVARCHAR(255),
    payload         NVARCHAR(MAX) NOT NULL,
    payload_type    NVARCHAR(500) NOT NULL,
    INDEX idx_mohs_execution_created NONCLUSTERED (created_at),
    INDEX idx_mohs_execution_job NONCLUSTERED (job_key, execution_id DESC), -- ORDER BY/cursor do findPage — ver o V3 do Postgres
    INDEX idx_mohs_execution_corr NONCLUSTERED (correlation_id)
);

IF OBJECT_ID('mohs_attempt', 'U') IS NULL
CREATE TABLE mohs_attempt (
    execution_id NVARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      NVARCHAR(255) NOT NULL,
    started_at   DATETIME2    NOT NULL,
    finished_at  DATETIME2    NOT NULL,
    outcome      NVARCHAR(20)  NOT NULL,
    error_type   NVARCHAR(500),
    error        NVARCHAR(MAX),
    PRIMARY KEY (execution_id, number),
    INDEX idx_mohs_attempt_throughput NONCLUSTERED (finished_at, outcome)
);

IF OBJECT_ID('mohs_idempotency', 'U') IS NULL
CREATE TABLE mohs_idempotency (
    job_key         NVARCHAR(255) NOT NULL,
    idempotency_key NVARCHAR(255) NOT NULL,
    execution_id    NVARCHAR(255) NOT NULL,
    created_at      DATETIME2    NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);
