-- Mohs: instalador canônico da primeira versão, h2.
-- Instantes UTC com precisão de microssegundos; durações BIGINT em microssegundos.
-- Chaves técnicas têm comparação exata; job_key é a identidade de uma definição.
-- Recibos de idempotência expiram pelo expires_at persistido, independentemente da limpeza.
-- Consulte docs/06-data/type-contract.md e docs/06-data/data-model.md.

-- Mutex durável do catálogo, necessário também quando ainda não há workers nem jobs.
CREATE TABLE IF NOT EXISTS mohs_catalog (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    fingerprint VARCHAR(64)
);
INSERT INTO mohs_catalog (singleton, fingerprint)
SELECT 1, NULL WHERE NOT EXISTS (SELECT 1 FROM mohs_catalog WHERE singleton = 1);
-- mohs_clock_now usa o relógio atual do processo H2, sem fixá-lo no início da transação.

CREATE ALIAS IF NOT EXISTS mohs_clock_now FOR "java.time.OffsetDateTime.now()";

CREATE TABLE IF NOT EXISTS mohs_job_definitions (
    job_key         VARCHAR(255) PRIMARY KEY,
    name            VARCHAR(255),

    schedule_type   VARCHAR(20)  NOT NULL,
    schedule_revision BIGINT NOT NULL DEFAULT 1,
    cron_expression VARCHAR(255),
    cron_zone       VARCHAR(100),
    interval_micros      BIGINT,
    interval_after_finish  BOOLEAN,
    runner          VARCHAR(255),
    window_name     VARCHAR(255),
    rate_limit      VARCHAR(255),
    misfire         VARCHAR(20)  NOT NULL,
    start_paused    BOOLEAN      NOT NULL DEFAULT FALSE,

    max_concurrent_executions INT NOT NULL DEFAULT 0 CHECK (max_concurrent_executions >= 0),
    retries         INT          NOT NULL DEFAULT 0,
    timeout_micros         BIGINT,
    retry_policy    VARCHAR(255),
    source          VARCHAR(20)  NOT NULL,

    paused          BOOLEAN      NOT NULL DEFAULT FALSE,
    retired         BOOLEAN      NOT NULL DEFAULT FALSE,
    next_fire_at    TIMESTAMP(6),
    created_at      TIMESTAMP(6)    NOT NULL,
    updated_at      TIMESTAMP(6)    NOT NULL,
    executable_key         VARCHAR(255) NOT NULL,
    recurring_payload      TEXT,
    input_contract VARCHAR(255) NOT NULL,
    CONSTRAINT ck_mohs_job_input CHECK (
        (job_key = executable_key
            AND recurring_payload IS NULL)
        OR (job_key <> executable_key
            AND recurring_payload IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_mohs_job_next_fire ON mohs_job_definitions (next_fire_at);

CREATE TABLE IF NOT EXISTS mohs_batches (
    id         VARCHAR(36) PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    total      BIGINT NOT NULL DEFAULT 0,
    succeeded  BIGINT NOT NULL DEFAULT 0,
    failed     BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT ck_mohs_batch_counts CHECK (total >= 0 AND succeeded >= 0 AND failed >= 0
        AND succeeded <= total AND failed <= total - succeeded)
);

CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_micros BIGINT NOT NULL,

    tokens          INT NOT NULL,
    refill_remainder BIGINT NOT NULL DEFAULT 0,
    refilled_at     TIMESTAMP(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(36) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at TIMESTAMP(6)   NOT NULL,
    revoked           BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at        TIMESTAMP(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS mohs_work (
    execution_id VARCHAR(36) PRIMARY KEY,
    job_key VARCHAR(255) NOT NULL,
    shard SMALLINT NOT NULL DEFAULT 0,
    priority INT NOT NULL DEFAULT 20,
    attempt INT NOT NULL,
    work_state VARCHAR(6) NOT NULL,
    visible_at TIMESTAMP(6),
    node_id VARCHAR(36),
    claim_token VARCHAR(36),
    claimed_at TIMESTAMP(6),
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_mohs_work_state CHECK (
        (work_state = 'READY' AND visible_at IS NOT NULL AND node_id IS NULL
            AND claim_token IS NULL AND claimed_at IS NULL AND cancel_requested = FALSE)
        OR (work_state = 'LEASED' AND visible_at IS NULL AND node_id IS NOT NULL
            AND claim_token IS NOT NULL AND claimed_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_mohs_work_claim ON mohs_work (work_state, shard, priority, visible_at, execution_id);
CREATE INDEX IF NOT EXISTS idx_mohs_work_node ON mohs_work (work_state, node_id, claimed_at, execution_id);
CREATE INDEX IF NOT EXISTS idx_mohs_work_job ON mohs_work (work_state, job_key);

CREATE TABLE IF NOT EXISTS mohs_execution (
    execution_id    VARCHAR(36) PRIMARY KEY,
    job_key         VARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL,
    scheduled_at    TIMESTAMP(6)    NOT NULL,
    created_at      TIMESTAMP(6)    NOT NULL,
    finished_at     TIMESTAMP(6),
    actor           VARCHAR(255) NOT NULL,
    batch_id  VARCHAR(36),
    origin VARCHAR(16) NOT NULL DEFAULT 'MANUAL',
    schedule_revision BIGINT,
    CONSTRAINT ck_mohs_execution_origin CHECK ((origin = 'MANUAL' AND schedule_revision IS NULL)
        OR (origin = 'SCHEDULED' AND schedule_revision IS NOT NULL AND schedule_revision > 0)),
    idempotency_key VARCHAR(255),
    payload         TEXT         NOT NULL,
    input_contract VARCHAR(255) NOT NULL,
    last_claim_token VARCHAR(36),
    last_attempt_number INT NOT NULL DEFAULT 0,
    last_attempt_node_id VARCHAR(36),
    last_attempt_started_at TIMESTAMP(6),
    last_attempt_finished_at TIMESTAMP(6),
    last_attempt_outcome VARCHAR(20),
    last_attempt_error_type VARCHAR(500),
    last_attempt_error TEXT,

    CONSTRAINT ck_mohs_execution_attempt CHECK ((last_attempt_number = 0 AND last_attempt_node_id IS NULL
        AND last_attempt_started_at IS NULL AND last_attempt_finished_at IS NULL
        AND last_attempt_outcome IS NULL AND last_attempt_error_type IS NULL AND last_attempt_error IS NULL)
        OR (last_attempt_number > 0 AND last_attempt_node_id IS NOT NULL
        AND last_attempt_started_at IS NOT NULL AND last_attempt_finished_at IS NOT NULL
        AND CASE last_attempt_outcome WHEN 'SUCCEEDED' THEN 1 WHEN 'FAILED' THEN 1 WHEN 'CANCELLED' THEN 1 ELSE 0 END = 1))
);

CREATE INDEX IF NOT EXISTS idx_mohs_execution_job     ON mohs_execution (job_key, execution_id DESC);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_batch    ON mohs_execution (batch_id);

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(36) NOT NULL,
    created_at      TIMESTAMP(6)    NOT NULL,
    expires_at TIMESTAMP(6),
    scheduled_at    TIMESTAMP(6) NOT NULL,
    actor           VARCHAR(255) NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_mohs_idempotency_expires ON mohs_idempotency (expires_at);

CREATE INDEX IF NOT EXISTS idx_mohs_job_executable ON mohs_job_definitions (executable_key, retired, job_key);
ALTER TABLE mohs_job_definitions ADD CONSTRAINT IF NOT EXISTS fk_mohs_job_executable
    FOREIGN KEY (executable_key) REFERENCES mohs_job_definitions(job_key);

CREATE INDEX IF NOT EXISTS idx_mohs_execution_outcome ON mohs_execution (finished_at, state);

CREATE INDEX IF NOT EXISTS idx_mohs_work_candidates ON mohs_work (work_state, shard, job_key, visible_at);
