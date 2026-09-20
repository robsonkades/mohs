-- Mohs: instalador canônico da primeira versão, mysql.
-- Instantes UTC com precisão de microssegundos; durações BIGINT em microssegundos.
-- Chaves técnicas têm comparação exata; job_key é a identidade de uma definição.
-- Recibos de idempotência expiram pelo expires_at persistido, independentemente da limpeza.
-- Consulte docs/06-data/type-contract.md e docs/06-data/data-model.md.

-- Mutex durável do catálogo, necessário também quando ainda não há workers nem jobs.
CREATE TABLE IF NOT EXISTS mohs_catalog (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    fingerprint VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin
) DEFAULT CHARACTER SET utf8mb4;
INSERT INTO mohs_catalog (singleton, fingerprint)
SELECT 1, NULL WHERE NOT EXISTS (SELECT 1 FROM mohs_catalog WHERE singleton = 1);

CREATE TABLE IF NOT EXISTS mohs_job_definitions (
    job_key         VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    name            VARCHAR(255),

    schedule_type   VARCHAR(20)  NOT NULL,
    schedule_revision BIGINT NOT NULL DEFAULT 1,
    cron_expression VARCHAR(255),
    cron_zone       VARCHAR(100),
    interval_micros      BIGINT,
    interval_after_finish  BOOLEAN,
    runner          VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin,
    window_name     VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin,
    rate_limit      VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin,
    misfire         VARCHAR(20)  NOT NULL,
    start_paused    BOOLEAN      NOT NULL DEFAULT FALSE,

    max_concurrent_executions INT NOT NULL DEFAULT 0 CHECK (max_concurrent_executions >= 0),
        retries         INT          NOT NULL DEFAULT 0,
    timeout_micros         BIGINT,
    retry_policy    VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin,
    source          VARCHAR(20)  NOT NULL,

    paused          BOOLEAN      NOT NULL DEFAULT FALSE,
    retired         BOOLEAN      NOT NULL DEFAULT FALSE,
    next_fire_at    DATETIME(6),
    created_at      DATETIME(6)  NOT NULL,
    updated_at      DATETIME(6)  NOT NULL,
    executable_key         VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    recurring_payload      MEDIUMTEXT,
    input_contract VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    CONSTRAINT ck_mohs_job_input CHECK (
        (job_key = executable_key
            AND recurring_payload IS NULL)
        OR (job_key <> executable_key
            AND recurring_payload IS NOT NULL)),
    CONSTRAINT fk_mohs_job_executable FOREIGN KEY (executable_key) REFERENCES mohs_job_definitions(job_key),
    INDEX idx_mohs_job_executable (executable_key, retired, job_key),
    INDEX idx_mohs_job_next_fire (next_fire_at)
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_batches (
    id         VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    total      BIGINT NOT NULL DEFAULT 0,
    succeeded  BIGINT NOT NULL DEFAULT 0,
    failed     BIGINT NOT NULL DEFAULT 0,
    created_at DATETIME(6) NOT NULL,
    CONSTRAINT ck_mohs_batch_counts CHECK (total >= 0 AND succeeded >= 0 AND failed >= 0
        AND succeeded <= total AND failed <= total - succeeded)
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    max_count       INT NOT NULL,
    window_micros BIGINT NOT NULL,

    tokens          INT NOT NULL,
    refill_remainder BIGINT NOT NULL DEFAULT 0,
    refilled_at     DATETIME(6) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at DATETIME(6) NOT NULL,
    revoked           BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at        DATETIME(6) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_work (
    execution_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    job_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    shard SMALLINT NOT NULL DEFAULT 0,
    priority INT NOT NULL DEFAULT 20,
    attempt INT NOT NULL,
    work_state VARCHAR(6) NOT NULL,
    visible_at DATETIME(6),
    node_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    claim_token VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    claimed_at DATETIME(6),
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_mohs_work_state CHECK (
        (work_state = 'READY' AND visible_at IS NOT NULL AND node_id IS NULL
            AND claim_token IS NULL AND claimed_at IS NULL AND cancel_requested = FALSE)
        OR (work_state = 'LEASED' AND visible_at IS NULL AND node_id IS NOT NULL
            AND claim_token IS NOT NULL AND claimed_at IS NOT NULL)),
    INDEX idx_mohs_work_claim (work_state, shard, priority, visible_at, execution_id),
    INDEX idx_mohs_work_node (work_state, node_id, claimed_at, execution_id),
    INDEX idx_mohs_work_candidates (work_state, shard, job_key, visible_at),
    INDEX idx_mohs_work_job (work_state, job_key)
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_execution (
    execution_id    VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    job_key         VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL,
    scheduled_at    DATETIME(6)  NOT NULL,
    created_at      DATETIME(6)  NOT NULL,
    finished_at     DATETIME(6),
    actor           VARCHAR(255) NOT NULL,
    batch_id  VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    origin VARCHAR(16) NOT NULL DEFAULT 'MANUAL',
    schedule_revision BIGINT,
    CONSTRAINT ck_mohs_execution_origin CHECK ((origin = 'MANUAL' AND schedule_revision IS NULL)
        OR (origin = 'SCHEDULED' AND schedule_revision IS NOT NULL AND schedule_revision > 0)),
    idempotency_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin,
    payload         MEDIUMTEXT   NOT NULL,
    input_contract VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    last_claim_token VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    last_attempt_number INT NOT NULL DEFAULT 0,
    last_attempt_node_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    last_attempt_started_at DATETIME(6),
    last_attempt_finished_at DATETIME(6),
    last_attempt_outcome VARCHAR(20),
    last_attempt_error_type VARCHAR(500),
    last_attempt_error MEDIUMTEXT,
    CONSTRAINT ck_mohs_execution_attempt CHECK ((last_attempt_number = 0 AND last_attempt_node_id IS NULL
        AND last_attempt_started_at IS NULL AND last_attempt_finished_at IS NULL
        AND last_attempt_outcome IS NULL AND last_attempt_error_type IS NULL AND last_attempt_error IS NULL)
        OR (last_attempt_number > 0 AND last_attempt_node_id IS NOT NULL
        AND last_attempt_started_at IS NOT NULL AND last_attempt_finished_at IS NOT NULL
        AND last_attempt_outcome IS NOT NULL AND last_attempt_outcome IN ('SUCCEEDED', 'FAILED', 'CANCELLED'))),
    INDEX idx_mohs_execution_job (job_key, execution_id DESC),
    INDEX idx_mohs_execution_outcome (finished_at, state),
    INDEX idx_mohs_execution_batch (batch_id)
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    idempotency_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    execution_id    VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at      DATETIME(6)  NOT NULL,
    expires_at DATETIME(6),
    scheduled_at    DATETIME(6) NOT NULL,
    actor           VARCHAR(255) NOT NULL,
    PRIMARY KEY (job_key, idempotency_key),
    INDEX idx_mohs_idempotency_expires (expires_at)
) DEFAULT CHARACTER SET utf8mb4;
