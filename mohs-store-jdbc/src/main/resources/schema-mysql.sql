-- Schema JDBC do Mohs (io.mohs.store.jdbc) para MySQL (8.0+).
-- Divergências reais do dialeto H2/Postgres: TIMESTAMP no MySQL tem
-- semântica própria de auto-init/auto-update e alcance limitado
-- (1970-2038) — usa DATETIME(6) pra guardar só o valor, sem armadilha;
-- (6) é obrigatório (DBTUNE-2): DATETIME puro é DATETIME(0), arredonda
-- pro segundo inteiro (não trunca — um instante ...500ms pode gravar até
-- 500ms no futuro), granularidade real menor que os outros 3 dialetos
-- (Postgres/H2 guardam microssegundo, SQL Server DATETIME2 guarda 100ns)
-- — sob rajada, ORDER BY scheduled_at do claim empataria dentro do mesmo
-- segundo. Connector/J já envia frações por default, sem mudança de bind.
-- CHARACTER SET utf8mb4 explícito por tabela — não depender do default
-- do servidor (nem todo MySQL 8 vem configurado utf8mb4 por padrão).
-- BOOLEAN/TRUE/FALSE funcionam igual (alias de TINYINT(1)/1/0); LIMIT e
-- FOR UPDATE SKIP LOCKED são nativos desde o MySQL 8.0, sem divergência
-- de query (ver JdbcDelegate).
-- MySQL não tem "CREATE INDEX IF NOT EXISTS" — cada índice é guardado por
-- information_schema + SQL dinâmico: a V1 precisa ser idempotente pra
-- ADOTAR instalação existente (DDL do MySQL comita implicitamente — uma
-- falha no meio deixaria a migração pela metade, sem como repeti-la).
-- DBTUNE-1: toda coluna DATETIME guarda wall-clock em UTC, gravado/lido
-- só via io.mohs.store.jdbc.JdbcTimestamps — ver schema-h2.sql para o porquê.

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
    start_paused    BOOLEAN      NOT NULL DEFAULT FALSE, -- definicional — ver schema-h2.sql
    allow_concurrent_executions BOOLEAN NOT NULL DEFAULT TRUE,
    max_concurrent_executions INT NOT NULL DEFAULT 0, -- só != 0 quando allow_concurrent_executions = FALSE; o cap deriva de mohs_lease
        retries         INT          NOT NULL DEFAULT 0,
    timeout         VARCHAR(50),
    retry_policy    VARCHAR(255),
    source          VARCHAR(20)  NOT NULL, -- ANNOTATION | PROGRAMMATIC
    orphaned        BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional
    paused          BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional
    retired         BOOLEAN      NOT NULL DEFAULT FALSE, -- aposentadoria explícita (Mohs.remove) — ver schema-h2.sql
    next_fire_at    DATETIME(6), -- estado do trigger — ver schema-h2.sql
    created_at      DATETIME(6)  NOT NULL,
    updated_at      DATETIME(6)  NOT NULL,
    INDEX idx_mohs_job_next_fire (next_fire_at) -- the per-tick due-trigger sweep; see the V9 delta for the measurement
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_batches (
    id         VARCHAR(255) PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    total      INT NOT NULL DEFAULT 0,
    succeeded  INT NOT NULL DEFAULT 0,
    failed     INT NOT NULL DEFAULT 0,
    created_at DATETIME(6) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;


CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_duration VARCHAR(50) NOT NULL,
    -- balde de tokens: capacidade = max_count, um token a cada window/max
    tokens          INT NOT NULL,
    refilled_at     DATETIME(6) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;

-- Heartbeat de node. Desde a Phase 4 do redesign deixou de ser só
-- informativa: o reaper consulta expires_at/last_heartbeat_at para decidir
-- quem está morto (aliveNodeIds do Engine).
CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at DATETIME(6) NOT NULL,
    epoch             BIGINT      NOT NULL DEFAULT 0, -- encarnação do nó
    expires_at        DATETIME(6)                     -- lease do NÓ
) DEFAULT CHARACTER SET utf8mb4;

-- --- Phase 5: o hot path fora da história -----------------------------
-- Quatro perfis de escrita, quatro tabelas (racional na migração
-- V3__table_split.sql; MySQL é o equivalente funcional Tier 2 — sem
-- partições nesta fase). Índices inline no CREATE: tabelas
-- novas dispensam as guardas PREPARE.

CREATE TABLE IF NOT EXISTS mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0,
    priority     INT          NOT NULL DEFAULT 20,
    attempt      INT          NOT NULL,
    visible_at   DATETIME(6)  NOT NULL,
    INDEX idx_mohs_ready_claim (shard, priority, visible_at)
) DEFAULT CHARACTER SET utf8mb4;

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
) DEFAULT CHARACTER SET utf8mb4;

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
    payload         MEDIUMTEXT   NOT NULL,
    payload_type    VARCHAR(500) NOT NULL,
    INDEX idx_mohs_execution_job (job_key, execution_id DESC), -- ORDER BY/cursor do findPage — ver o V3 do Postgres
    INDEX idx_mohs_execution_corr (correlation_id)
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_attempt (
    execution_id VARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      VARCHAR(255) NOT NULL,
    started_at   DATETIME(6)  NOT NULL,
    finished_at  DATETIME(6)  NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,
    error_type   VARCHAR(500),
    error        MEDIUMTEXT,
    PRIMARY KEY (execution_id, number),
    INDEX idx_mohs_attempt_throughput (finished_at, outcome)
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      DATETIME(6)  NOT NULL,
    PRIMARY KEY (job_key, idempotency_key),
    INDEX idx_mohs_idempotency_created (created_at) -- pruneIdempotencyBefore: sem ele o DELETE por retenção é full scan
) DEFAULT CHARACTER SET utf8mb4;
