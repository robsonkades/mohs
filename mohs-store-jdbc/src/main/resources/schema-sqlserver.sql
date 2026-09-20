-- Mohs: instalador canônico da primeira versão, sqlserver.
-- Instantes UTC com precisão de microssegundos; durações BIGINT em microssegundos.
-- Chaves técnicas têm comparação exata; job_key é a identidade de uma definição.
-- Recibos de idempotência expiram pelo expires_at persistido, independentemente da limpeza.
-- Consulte docs/06-data/type-contract.md e docs/06-data/data-model.md.

-- Mutex durável do catálogo, necessário também quando ainda não há workers nem jobs.
IF OBJECT_ID('mohs_catalog', 'U') IS NULL
CREATE TABLE mohs_catalog (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    fingerprint NVARCHAR(64) COLLATE Latin1_General_100_BIN2
);
INSERT INTO mohs_catalog (singleton, fingerprint)
SELECT 1, NULL WHERE NOT EXISTS (SELECT 1 FROM mohs_catalog WHERE singleton = 1);
-- Pré-requisito: READ_COMMITTED_SNAPSHOT ON. A validação de startup verifica essa opção.
-- O instalador não altera a configuração global do banco hospedeiro.

IF OBJECT_ID('mohs_job_definitions', 'U') IS NULL
CREATE TABLE mohs_job_definitions (
    job_key         NVARCHAR(255) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    name            NVARCHAR(255),

    schedule_type   NVARCHAR(20)  NOT NULL,
    schedule_revision BIGINT NOT NULL DEFAULT 1,
    cron_expression NVARCHAR(255),
    cron_zone       NVARCHAR(100),
    interval_micros      BIGINT,
    interval_after_finish  BIT,
    runner          NVARCHAR(255) COLLATE Latin1_General_100_BIN2,
    window_name     NVARCHAR(255) COLLATE Latin1_General_100_BIN2,
    rate_limit      NVARCHAR(255) COLLATE Latin1_General_100_BIN2,
    misfire         NVARCHAR(20)  NOT NULL,
    start_paused    BIT           NOT NULL DEFAULT 0,

    max_concurrent_executions INT NOT NULL DEFAULT 0 CHECK (max_concurrent_executions >= 0),
    retries         INT           NOT NULL DEFAULT 0,
    timeout_micros         BIGINT,
    retry_policy    NVARCHAR(255) COLLATE Latin1_General_100_BIN2,
    source          NVARCHAR(20)  NOT NULL,

    paused          BIT           NOT NULL DEFAULT 0,
    retired         BIT           NOT NULL DEFAULT 0,
    next_fire_at    DATETIME2(6),
    created_at      DATETIME2(6)     NOT NULL,
    updated_at      DATETIME2(6)     NOT NULL,
    executable_key         NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
    recurring_payload      NVARCHAR(MAX),
    input_contract NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
    CONSTRAINT ck_mohs_job_input CHECK (
        (job_key = executable_key
            AND recurring_payload IS NULL)
        OR (job_key <> executable_key
            AND recurring_payload IS NOT NULL)),
    CONSTRAINT fk_mohs_job_executable FOREIGN KEY (executable_key) REFERENCES mohs_job_definitions(job_key),
    INDEX idx_mohs_job_executable NONCLUSTERED (executable_key, retired, job_key),
    INDEX idx_mohs_job_next_fire NONCLUSTERED (next_fire_at)
);

IF OBJECT_ID('mohs_batches', 'U') IS NULL
CREATE TABLE mohs_batches (
    id         NVARCHAR(36) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    name       NVARCHAR(255) NOT NULL,
    total      BIGINT NOT NULL DEFAULT 0,
    succeeded  BIGINT NOT NULL DEFAULT 0,
    failed     BIGINT NOT NULL DEFAULT 0,
    created_at DATETIME2(6) NOT NULL,
    CONSTRAINT ck_mohs_batch_counts CHECK (total >= 0 AND succeeded >= 0 AND failed >= 0
        AND succeeded <= total AND failed <= total - succeeded)
);

IF OBJECT_ID('mohs_rate_limits', 'U') IS NULL
CREATE TABLE mohs_rate_limits (
    name            NVARCHAR(255) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    max_count       INT NOT NULL,
    window_micros BIGINT NOT NULL,

    tokens          INT NOT NULL,
    refill_remainder BIGINT NOT NULL DEFAULT 0,
    refilled_at     DATETIME2(6) NOT NULL
);

IF OBJECT_ID('mohs_nodes', 'U') IS NULL
CREATE TABLE mohs_nodes (
    node_id           NVARCHAR(36) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    state             NVARCHAR(20) NOT NULL,
    last_heartbeat_at DATETIME2(6)    NOT NULL,
    revoked           BIT NOT NULL DEFAULT 0,
    expires_at        DATETIME2(6) NOT NULL
);

IF OBJECT_ID('mohs_work', 'U') IS NULL
CREATE TABLE mohs_work (
    execution_id NVARCHAR(36) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    job_key NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
    shard SMALLINT NOT NULL DEFAULT 0,
    priority INT NOT NULL DEFAULT 20,
    attempt INT NOT NULL,
    work_state NVARCHAR(6) NOT NULL,
    visible_at DATETIME2(6),
    node_id NVARCHAR(36) COLLATE Latin1_General_100_BIN2,
    claim_token NVARCHAR(36) COLLATE Latin1_General_100_BIN2,
    claimed_at DATETIME2(6),
    cancel_requested BIT NOT NULL DEFAULT 0,
    CONSTRAINT ck_mohs_work_state CHECK (
        (work_state = 'READY' AND visible_at IS NOT NULL AND node_id IS NULL
            AND claim_token IS NULL AND claimed_at IS NULL AND cancel_requested = 0)
        OR (work_state = 'LEASED' AND visible_at IS NULL AND node_id IS NOT NULL
            AND claim_token IS NOT NULL AND claimed_at IS NOT NULL)),
    INDEX idx_mohs_work_claim NONCLUSTERED (shard, priority, visible_at, execution_id) WHERE work_state = 'READY',
    INDEX idx_mohs_work_node NONCLUSTERED (node_id, claimed_at, execution_id) WHERE work_state = 'LEASED',
    INDEX idx_mohs_work_candidates NONCLUSTERED (shard, job_key, visible_at) WHERE work_state = 'READY',
    INDEX idx_mohs_work_job NONCLUSTERED (job_key) WHERE work_state = 'LEASED'
);

IF OBJECT_ID('mohs_execution', 'U') IS NULL
CREATE TABLE mohs_execution (
    execution_id    NVARCHAR(36) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    job_key         NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           NVARCHAR(20)  NOT NULL,
    scheduled_at    DATETIME2(6)    NOT NULL,
    created_at      DATETIME2(6)    NOT NULL,
    finished_at     DATETIME2(6),
    actor           NVARCHAR(255) NOT NULL,
    batch_id  NVARCHAR(36) COLLATE Latin1_General_100_BIN2,
    origin NVARCHAR(16) NOT NULL DEFAULT 'MANUAL',
    schedule_revision BIGINT,
    CONSTRAINT ck_mohs_execution_origin CHECK ((origin = 'MANUAL' AND schedule_revision IS NULL)
        OR (origin = 'SCHEDULED' AND schedule_revision IS NOT NULL AND schedule_revision > 0)),
    idempotency_key NVARCHAR(255) COLLATE Latin1_General_100_BIN2,
    payload         NVARCHAR(MAX) NOT NULL,
    input_contract NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
    last_claim_token NVARCHAR(36) COLLATE Latin1_General_100_BIN2,
    last_attempt_number INT NOT NULL DEFAULT 0,
    last_attempt_node_id NVARCHAR(36) COLLATE Latin1_General_100_BIN2,
    last_attempt_started_at DATETIME2(6),
    last_attempt_finished_at DATETIME2(6),
    last_attempt_outcome NVARCHAR(20),
    last_attempt_error_type NVARCHAR(500),
    last_attempt_error NVARCHAR(MAX),
    CONSTRAINT ck_mohs_execution_attempt CHECK ((last_attempt_number = 0 AND last_attempt_node_id IS NULL
        AND last_attempt_started_at IS NULL AND last_attempt_finished_at IS NULL
        AND last_attempt_outcome IS NULL AND last_attempt_error_type IS NULL AND last_attempt_error IS NULL)
        OR (last_attempt_number > 0 AND last_attempt_node_id IS NOT NULL
        AND last_attempt_started_at IS NOT NULL AND last_attempt_finished_at IS NOT NULL
        AND last_attempt_outcome IS NOT NULL AND last_attempt_outcome IN ('SUCCEEDED', 'FAILED', 'CANCELLED'))),
    INDEX idx_mohs_execution_job NONCLUSTERED (job_key, execution_id DESC),
    INDEX idx_mohs_execution_outcome NONCLUSTERED (finished_at, state) WHERE finished_at IS NOT NULL,
    INDEX idx_mohs_execution_batch NONCLUSTERED (batch_id)
);

IF OBJECT_ID('mohs_idempotency', 'U') IS NULL
CREATE TABLE mohs_idempotency (
    job_key         NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
    idempotency_key NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
    execution_id    NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL,
    created_at      DATETIME2(6)    NOT NULL,
    expires_at DATETIME2(6),
    scheduled_at    DATETIME2(6) NOT NULL,
    actor           NVARCHAR(255) NOT NULL,

    CONSTRAINT pk_mohs_idempotency PRIMARY KEY NONCLUSTERED (job_key, idempotency_key)
);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_mohs_idempotency_created' AND object_id = OBJECT_ID('mohs_idempotency', 'U'))
    CREATE CLUSTERED INDEX ix_mohs_idempotency_created ON mohs_idempotency (expires_at);
