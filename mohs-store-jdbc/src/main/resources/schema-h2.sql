-- Schema JDBC do Mohs (io.mohs.store.jdbc) para H2 — dialeto embarcado/teste.
-- Mohs é biblioteca embarcada, compartilha banco/schema com a aplicação
-- hospedeira, não pode colidir com tabela dela. Durations viram VARCHAR
-- via Duration.toString()/Duration.parse() (ISO-8601, ex. "PT30S") —
-- mais legível pra debug manual que millis, nenhuma delas precisa de
-- range query. Sem execution_windows/runners: os dois são bean-resolved
-- ("predicados só existem em código", §5.8 do documento mestre), não
-- dado persistido. Um arquivo por dialeto — H2/Postgres eram
-- idênticos até agora, mas MySQL/SQL Server divergem o bastante pra não
-- fazer mais sentido um "schema.sql" genérico.
-- DBTUNE-1: toda coluna temporal guarda wall-clock em UTC — nenhuma tem
-- fuso (TIMESTAMP aqui é "without time zone"). Gravado/lido só via
-- io.mohs.store.jdbc.JdbcTimestamps (nunca java.sql.Timestamp.from(instant)/
-- Timestamp.toInstant() direto), que normaliza pra UTC independente do
-- fuso default da JVM de cada nó.

-- id é UUIDv7 (io.github.robsonkades:uuidv7), mesma geração de mohs_execution.execution_id
-- — surrogate key estável; job_key continua sendo a chave de negócio (única).
CREATE TABLE IF NOT EXISTS mohs_job_definitions (
    id              VARCHAR(255) PRIMARY KEY,
    job_key         VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(255),
    handler_type    VARCHAR(500) NOT NULL,
    schedule_type   VARCHAR(20)  NOT NULL, -- CRON | INTERVAL | ON_DEMAND
    cron_expression VARCHAR(255),
    cron_zone       VARCHAR(100),
    interval_duration      VARCHAR(50),
    interval_after_finish  BOOLEAN,
    runner          VARCHAR(255),
    window_name     VARCHAR(255),
    rate_limit      VARCHAR(255), -- nome do RateLimit cluster-wide
    misfire         VARCHAR(20)  NOT NULL,
    start_paused    BOOLEAN      NOT NULL DEFAULT FALSE, -- definicional: nasce pausado no 1º registro; 'paused' segue operacional
    allow_concurrent_executions BOOLEAN NOT NULL DEFAULT TRUE,
    max_concurrent_executions INT NOT NULL DEFAULT 0, -- só != 0 quando allow_concurrent_executions = FALSE; o cap deriva de mohs_lease
    retries         INT          NOT NULL DEFAULT 0,
    timeout         VARCHAR(50),
    retry_policy    VARCHAR(255),
    source          VARCHAR(20)  NOT NULL, -- ANNOTATION | PROGRAMMATIC
    orphaned        BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional
    paused          BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional
    retired         BOOLEAN      NOT NULL DEFAULT FALSE, -- aposentadoria explícita (Mohs.remove): some das leituras, a fila é drenada; a linha fica (histórico preservado em mohs_execution)
    next_fire_at    TIMESTAMP,   -- estado do trigger: NULL = nada a disparar (on-demand; fixed-delay aguardando o fim da execução anterior)
    created_at      TIMESTAMP    NOT NULL,
    updated_at      TIMESTAMP    NOT NULL
);

-- The firer reads this table on EVERY tick, ahead of the claim: without an index on
-- next_fire_at the due-trigger sweep is a full scan of every definition, per tick, per
-- node. Measured on PostgreSQL 18, 100k definitions: 7.63 ms / 1822 buffers ->
-- 0.229 ms / 48 buffers. The write side is unaffected - 100k trigger advances cost
-- 745 ms with the index and 752 ms without, and this table is written once per firing,
-- not once per execution.
CREATE INDEX IF NOT EXISTS idx_mohs_job_next_fire ON mohs_job_definitions (next_fire_at);

CREATE TABLE IF NOT EXISTS mohs_batches (
    id         VARCHAR(255) PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    total      INT NOT NULL DEFAULT 0,
    succeeded  INT NOT NULL DEFAULT 0,
    failed     INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_duration VARCHAR(50) NOT NULL,
    -- balde de tokens: capacidade = max_count, um token a cada window/max
    tokens          INT NOT NULL,
    refilled_at     TIMESTAMP NOT NULL
);

-- Heartbeat de node. Desde a Phase 4 do redesign deixou de ser só
-- informativa: o reaper consulta expires_at/last_heartbeat_at para decidir
-- quem está morto (aliveNodeIds do Engine).
CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at TIMESTAMP   NOT NULL,
    epoch             BIGINT      NOT NULL DEFAULT 0, -- encarnação do nó
    expires_at        TIMESTAMP                       -- lease do NÓ
);

-- ─── Phase 5: o hot path fora da história ────────────────────────────
-- Quatro perfis de escrita, quatro tabelas (racional na migração
-- V3__table_split.sql; H2 é o equivalente funcional Tier 3 — sem
-- partições, sem índice parcial/INCLUDE).

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

CREATE INDEX IF NOT EXISTS idx_mohs_idempotency_created ON mohs_idempotency (created_at);
