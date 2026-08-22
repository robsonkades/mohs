-- Schema JDBC do Mohs (io.mohs.store.jdbc) para MySQL (8.0+ — ADR-0023).
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
-- de query (ver JdbcDialect, ADR-0023).
-- MySQL não tem "CREATE INDEX IF NOT EXISTS" — cada índice é guardado por
-- information_schema + SQL dinâmico (ADR-0048): a V1 do Flyway precisa ser
-- idempotente pra ADOTAR instalação existente (DDL do MySQL comita
-- implicitamente — uma falha no meio deixaria migração success=false e o
-- boot em loop, o 🔴 da bancada da Phase 2).
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
    rate_limit      VARCHAR(255), -- nome do RateLimit cluster-wide (ADR-0042)
    misfire         VARCHAR(20)  NOT NULL,
    start_paused    BOOLEAN      NOT NULL DEFAULT FALSE, -- definicional (ADR-0037) — ver schema-h2.sql
    allow_concurrent_executions BOOLEAN NOT NULL DEFAULT TRUE,
    max_concurrent_executions INT NOT NULL DEFAULT 0, -- só != 0 quando allow_concurrent_executions = FALSE (ADR-0020)
    running_execution_count INT NOT NULL DEFAULT 0, -- contador de mutex por job (ADR-0018/0020)
    retries         INT          NOT NULL DEFAULT 0,
    timeout         VARCHAR(50),
    retry_policy    VARCHAR(255),
    source          VARCHAR(20)  NOT NULL, -- ANNOTATION | PROGRAMMATIC
    orphaned        BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional (ADR-0006)
    paused          BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional (ADR-0006)
    retired         BOOLEAN      NOT NULL DEFAULT FALSE, -- aposentadoria explícita (Mohs.remove) — ver schema-h2.sql
    next_fire_at    DATETIME(6), -- estado do trigger (ADR-0035) — ver schema-h2.sql
    created_at      DATETIME(6)  NOT NULL,
    updated_at      DATETIME(6)  NOT NULL
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_batches (
    id         VARCHAR(255) PRIMARY KEY,
    total      INT NOT NULL DEFAULT 0,
    succeeded  INT NOT NULL DEFAULT 0,
    failed     INT NOT NULL DEFAULT 0,
    created_at DATETIME(6) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;

-- id é UUIDv7 (io.github.robsonkades:uuidv7) — time-ordered, mantém
-- inserts localizados no fim do índice da tabela mais quente do sistema.
CREATE TABLE IF NOT EXISTS mohs_executions (
    id               VARCHAR(255) PRIMARY KEY,
    job_key          VARCHAR(255) NOT NULL REFERENCES mohs_job_definitions(job_key),
    state            VARCHAR(20)  NOT NULL,
    scheduled_at     DATETIME(6)  NOT NULL,
    fired_at         DATETIME(6),
    actor            VARCHAR(255) NOT NULL,
    idempotency_key  VARCHAR(255),
    priority         INT          NOT NULL DEFAULT 20, -- Priority.value(); 20 = NORMAL
    node_id          VARCHAR(255),  -- claim, etapa 3 (ADR-0016)
    lease_expires_at DATETIME(6),   -- claim, etapa 3 (ADR-0012/0016)
    cancel_requested BOOLEAN      NOT NULL DEFAULT FALSE, -- cancel cooperativo (ADR-0034) — ver schema-h2.sql
    batch_id         VARCHAR(255) REFERENCES mohs_batches(id),
    payload          TEXT         NOT NULL, -- não CLOB: MySQL não tem
    payload_type     VARCHAR(500) NOT NULL,
    created_at       DATETIME(6)  NOT NULL
) DEFAULT CHARACTER SET utf8mb4;
-- MySQL não tem índice parcial/filtrado — Postgres e SQL Server usam
-- WHERE state IN ('ENQUEUED', 'RETRY_SCHEDULED') aqui (DBTUNE-5, ADR-0033); MySQL fica com a composta cheia.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_executions'
                            AND index_name = 'idx_mohs_executions_claim'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_executions_claim ON mohs_executions (state, priority, scheduled_at)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
-- Sem índice parcial (ver comentário acima) — composta cheia pro reaper
-- também (DBTUNE-10): state líder, igual à do claim.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_executions'
                            AND index_name = 'idx_mohs_executions_reaper'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_executions_reaper ON mohs_executions (state, lease_expires_at)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_executions'
                            AND index_name = 'idx_mohs_executions_job_key'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_executions_job_key ON mohs_executions (job_key)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
-- Idempotent Receiver (EIP, DBTUNE-8) — ver schema-h2.sql. Índice único
-- do MySQL admite múltiplos NULLs: execuções sem chave nunca colidem.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_executions'
                            AND index_name = 'uq_mohs_executions_idem'),
                   'SELECT 1',
                   'CREATE UNIQUE INDEX uq_mohs_executions_idem ON mohs_executions (job_key, idempotency_key)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_executions'
                            AND index_name = 'idx_mohs_executions_batch_id'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_executions_batch_id ON mohs_executions (batch_id)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

CREATE TABLE IF NOT EXISTS mohs_attempts (
    execution_id VARCHAR(255) NOT NULL REFERENCES mohs_executions(id),
    number       INT          NOT NULL,
    started_at   DATETIME(6)  NOT NULL,
    finished_at  DATETIME(6),
    outcome      VARCHAR(20)  NOT NULL,
    error        TEXT, -- não CLOB: MySQL não tem
    PRIMARY KEY (execution_id, number)
) DEFAULT CHARACTER SET utf8mb4;
-- Janela de vazão do GET /overview — ver schema-postgresql.sql pro raciocínio.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_attempts'
                            AND index_name = 'idx_mohs_attempts_throughput'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_attempts_throughput ON mohs_attempts (finished_at, outcome)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_duration VARCHAR(50) NOT NULL,
    -- balde de tokens (ADR-0042): capacidade = max_count, um token a cada window/max
    tokens          INT NOT NULL,
    refilled_at     DATETIME(6) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;

-- Heartbeat de node (ADR-0012). Desde a ADR-0051 deixou de ser só
-- informativa: o reaper consulta expires_at/last_heartbeat_at para decidir
-- quem está morto (anti-join de JdbcReaper).
CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at DATETIME(6) NOT NULL,
    epoch             BIGINT      NOT NULL DEFAULT 0, -- encarnação do nó (ADR-0051)
    expires_at        DATETIME(6)                     -- lease do NÓ (ADR-0051)
) DEFAULT CHARACTER SET utf8mb4;
-- Reaper dirigido por nó (ADR-0051) — guarda idêntica às da seção de índices acima
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_executions'
                            AND index_name = 'idx_mohs_executions_owner'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_executions_owner ON mohs_executions (state, node_id)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

-- --- Phase 5 (ADR-A): o hot path fora da história -----------------------------
-- Quatro perfis de escrita, quatro tabelas (racional na migração
-- V3__table_split.sql; MySQL é o equivalente funcional Tier 2 — sem
-- partições nesta fase). Em transição (PLAN.md): o engine flipa no S5.3;
-- as tabelas antigas caem no S5.4. Índices inline no CREATE: tabelas
-- novas dispensam as guardas PREPARE.

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
    payload         MEDIUMTEXT   NOT NULL,
    payload_type    VARCHAR(500) NOT NULL,
    INDEX idx_mohs_execution_created (created_at),
    INDEX idx_mohs_execution_job (job_key, created_at DESC),
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
    error        MEDIUMTEXT,
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
