-- Phase 5 do redesign (ADR-A) — ver o V3 do Postgres para o racional das
-- quatro tabelas. MySQL é Tier 2 (ADR-0050): equivalente funcional, sem
-- partições nesta fase (retenção continua no mecanismo ADR-0032; partição
-- por RANGE chega com trigger medido). Tabelas NOVAS levam os índices
-- INLINE no CREATE TABLE IF NOT EXISTS — dispensa as guardas
-- information_schema/PREPARE da V1/V2, que só existem porque lá as
-- tabelas podiam pré-existir sem os índices.

CREATE TABLE IF NOT EXISTS mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0,
    priority     INT          NOT NULL DEFAULT 20,
    attempt      INT          NOT NULL,
    visible_at   DATETIME(6)  NOT NULL,
    INDEX idx_mohs_ready_claim (shard, priority, visible_at)
);

CREATE TABLE IF NOT EXISTS mohs_lease (
    execution_id     VARCHAR(255) PRIMARY KEY,
    job_key          VARCHAR(255) NOT NULL,
    node_id          VARCHAR(255) NOT NULL,
    epoch            BIGINT       NOT NULL,
    attempt_number   INT          NOT NULL,
    priority         INT          NOT NULL DEFAULT 20, -- viaja fila->posse: o requeue do reaper reconstroi a entrada sem ler historia (S5.3)
    claimed_at       DATETIME(6)  NOT NULL,
    cancel_requested BOOLEAN      NOT NULL DEFAULT FALSE,
    INDEX idx_mohs_lease_node (node_id, epoch),
    INDEX idx_mohs_lease_job (job_key)
);

CREATE TABLE IF NOT EXISTS mohs_execution (
    execution_id    VARCHAR(255) PRIMARY KEY,
    job_key         VARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL,
    scheduled_at    DATETIME(6)  NOT NULL,
    created_at      DATETIME(6)  NOT NULL,
    finished_at     DATETIME(6),
    actor           VARCHAR(255) NOT NULL,
    correlation_id  VARCHAR(255),
    idempotency_key VARCHAR(255),
    payload         MEDIUMTEXT   NOT NULL, -- TEXT trava em 64 KB; payload chega a 256 KB (§12.6)
    payload_type    VARCHAR(500) NOT NULL,
    INDEX idx_mohs_execution_created (created_at),
    INDEX idx_mohs_execution_job (job_key, execution_id DESC), -- ORDER BY/cursor do findPage — ver o V3 do Postgres
    INDEX idx_mohs_execution_corr (correlation_id)
);

CREATE TABLE IF NOT EXISTS mohs_attempt (
    execution_id VARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      VARCHAR(255) NOT NULL,
    started_at   DATETIME(6)  NOT NULL,
    finished_at  DATETIME(6)  NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,
    error_type   VARCHAR(500),
    error        MEDIUMTEXT, -- mesmo racional do payload: stack trace não pode truncar em 64 KB
    PRIMARY KEY (execution_id, number),
    INDEX idx_mohs_attempt_throughput (finished_at, outcome)
);

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      DATETIME(6)  NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);
