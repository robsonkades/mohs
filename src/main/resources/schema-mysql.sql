-- Schema JDBC do Mohs (io.mohs.jdbc) para MySQL (8.0+ — ADR-0023).
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
-- de query (ver JdbcDialect, ADR-0023). MySQL não tem "CREATE INDEX IF
-- NOT EXISTS" (só CREATE TABLE) — os índices abaixo não têm guarda
-- porque MySqlTestSupport aplica este script uma vez só por container,
-- nunca reexecuta contra tabelas já existentes.
-- DBTUNE-1: toda coluna DATETIME guarda wall-clock em UTC, gravado/lido
-- só via io.mohs.jdbc.JdbcTimestamps — ver schema-h2.sql para o porquê.

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
    misfire         VARCHAR(20)  NOT NULL,
    allow_concurrent_executions BOOLEAN NOT NULL DEFAULT TRUE,
    max_concurrent_executions INT NOT NULL DEFAULT 0, -- só != 0 quando allow_concurrent_executions = FALSE (ADR-0020)
    running_execution_count INT NOT NULL DEFAULT 0, -- contador de mutex por job (ADR-0018/0020)
    retries         INT          NOT NULL DEFAULT 0,
    timeout         VARCHAR(50),
    retry_policy    VARCHAR(255),
    source          VARCHAR(20)  NOT NULL, -- ANNOTATION | PROGRAMMATIC
    orphaned        BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional (ADR-0006)
    paused          BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional (ADR-0006)
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
    batch_id         VARCHAR(255) REFERENCES mohs_batches(id),
    payload          TEXT         NOT NULL, -- não CLOB: MySQL não tem
    payload_type     VARCHAR(500) NOT NULL,
    created_at       DATETIME(6)  NOT NULL
) DEFAULT CHARACTER SET utf8mb4;
-- MySQL não tem índice parcial/filtrado — Postgres e SQL Server usam
-- WHERE state = 'ENQUEUED' aqui (DBTUNE-5); MySQL fica com a composta cheia.
CREATE INDEX idx_mohs_executions_claim ON mohs_executions (state, priority, scheduled_at);
CREATE INDEX idx_mohs_executions_job_key ON mohs_executions (job_key);
CREATE INDEX idx_mohs_executions_idempotency_key ON mohs_executions (idempotency_key);
CREATE INDEX idx_mohs_executions_batch_id ON mohs_executions (batch_id);

CREATE TABLE IF NOT EXISTS mohs_attempts (
    execution_id VARCHAR(255) NOT NULL REFERENCES mohs_executions(id),
    number       INT          NOT NULL,
    started_at   DATETIME(6)  NOT NULL,
    finished_at  DATETIME(6),
    outcome      VARCHAR(20)  NOT NULL,
    error        TEXT, -- não CLOB: MySQL não tem
    PRIMARY KEY (execution_id, number)
) DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_duration VARCHAR(50) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;

-- Heartbeat de node (ADR-0012) — só informativo, GET /nodes; nenhuma
-- lógica de claim/reclaim consulta esta tabela.
CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at DATETIME(6) NOT NULL
) DEFAULT CHARACTER SET utf8mb4;
