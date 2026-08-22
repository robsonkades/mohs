-- Phase 5 do redesign (ADR-A) — ver o V3 do Postgres para o racional das
-- quatro tabelas. H2 é Tier 3 (ADR-0050, dev/teste): equivalente
-- FUNCIONAL, sem partições (retenção continua no mecanismo ADR-0032),
-- sem storage options, sem índice parcial/INCLUDE. A história ganha
-- índice em created_at no lugar da PK composta particionada do PG —
-- mesmo predicado de poda, outro suporte físico.

CREATE TABLE IF NOT EXISTS mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0,
    priority     INT          NOT NULL DEFAULT 20,
    attempt      INT          NOT NULL,
    visible_at   TIMESTAMP    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mohs_ready_claim ON mohs_ready (shard, priority, visible_at);

CREATE TABLE IF NOT EXISTS mohs_lease (
    execution_id     VARCHAR(255) PRIMARY KEY,
    job_key          VARCHAR(255) NOT NULL,
    node_id          VARCHAR(255) NOT NULL,
    epoch            BIGINT       NOT NULL,
    attempt_number   INT          NOT NULL,
    priority         INT          NOT NULL DEFAULT 20, -- viaja fila->posse: o requeue do reaper reconstroi a entrada sem ler historia (S5.3)
    claimed_at       TIMESTAMP    NOT NULL,
    cancel_requested BOOLEAN      NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_mohs_lease_node ON mohs_lease (node_id, epoch);
CREATE INDEX IF NOT EXISTS idx_mohs_lease_job  ON mohs_lease (job_key);

CREATE TABLE IF NOT EXISTS mohs_execution (
    execution_id    VARCHAR(255) PRIMARY KEY,
    job_key         VARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL,
    scheduled_at    TIMESTAMP    NOT NULL,
    created_at      TIMESTAMP    NOT NULL,
    finished_at     TIMESTAMP,
    actor           VARCHAR(255) NOT NULL,
    correlation_id  VARCHAR(255),
    idempotency_key VARCHAR(255),
    payload         TEXT         NOT NULL,
    payload_type    VARCHAR(500) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_created ON mohs_execution (created_at);
-- (job_key, execution_id): serve o ORDER BY/cursor do findPage — ver o V3 do Postgres
CREATE INDEX IF NOT EXISTS idx_mohs_execution_job     ON mohs_execution (job_key, execution_id DESC);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_corr    ON mohs_execution (correlation_id);

CREATE TABLE IF NOT EXISTS mohs_attempt (
    execution_id VARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      VARCHAR(255) NOT NULL,
    started_at   TIMESTAMP    NOT NULL,
    finished_at  TIMESTAMP    NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,
    error_type   VARCHAR(500),
    error        TEXT,
    PRIMARY KEY (execution_id, number)
);
CREATE INDEX IF NOT EXISTS idx_mohs_attempt_throughput ON mohs_attempt (finished_at, outcome);

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      TIMESTAMP    NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);
