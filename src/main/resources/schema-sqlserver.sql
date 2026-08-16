-- Schema JDBC do Mohs (io.mohs.jdbc) para SQL Server (ADR-0023).
-- Divergências reais do dialeto H2/Postgres: T-SQL não tem
-- "CREATE TABLE/INDEX IF NOT EXISTS" — usa IF OBJECT_ID(...) IS NULL /
-- IF NOT EXISTS (SELECT ... FROM sys.indexes) guardando a instrução,
-- sem BEGIN/END (T-SQL aceita um único statement condicionado sem
-- bloco — evita o problema de ResourceDatabasePopulator dividir o
-- script em ";", que quebraria um BEGIN...END no meio). TIMESTAMP no
-- SQL Server é sinônimo de ROWVERSION (binário auto-incrementado, não
-- data/hora) — usa DATETIME2. Sem CLOB — usa NVARCHAR(MAX). Sem
-- BOOLEAN nativo — usa BIT + 1/0. VARCHAR não é Unicode por padrão —
-- usa NVARCHAR em tudo.
-- DBTUNE-1: toda coluna DATETIME2 guarda wall-clock em UTC, gravado/lido
-- só via io.mohs.jdbc.JdbcTimestamps — ver schema-h2.sql para o porquê.

IF OBJECT_ID('mohs_job_definitions', 'U') IS NULL
CREATE TABLE mohs_job_definitions (
    id              NVARCHAR(255) PRIMARY KEY,
    job_key         NVARCHAR(255) NOT NULL UNIQUE,
    name            NVARCHAR(255),
    handler_type    NVARCHAR(500) NOT NULL,
    schedule_type   NVARCHAR(20)  NOT NULL, -- CRON | INTERVAL | ON_DEMAND
    cron_expression NVARCHAR(255),
    cron_zone       NVARCHAR(100),
    interval_duration      NVARCHAR(50),
    interval_after_finish  BIT,
    runner          NVARCHAR(255),
    window_name     NVARCHAR(255),
    misfire         NVARCHAR(20)  NOT NULL,
    start_paused    BIT           NOT NULL DEFAULT 0, -- definicional (ADR-0037) — ver schema-h2.sql
    allow_concurrent_executions BIT NOT NULL DEFAULT 1,
    max_concurrent_executions INT NOT NULL DEFAULT 0, -- só != 0 quando allow_concurrent_executions = 0 (ADR-0020)
    running_execution_count INT NOT NULL DEFAULT 0, -- contador de mutex por job (ADR-0018/0020)
    retries         INT           NOT NULL DEFAULT 0,
    timeout         NVARCHAR(50),
    retry_policy    NVARCHAR(255),
    source          NVARCHAR(20)  NOT NULL, -- ANNOTATION | PROGRAMMATIC
    orphaned        BIT           NOT NULL DEFAULT 0, -- operacional (ADR-0006)
    paused          BIT           NOT NULL DEFAULT 0, -- operacional (ADR-0006)
    retired         BIT           NOT NULL DEFAULT 0, -- aposentadoria explícita (Mohs.remove) — ver schema-h2.sql
    next_fire_at    DATETIME2,    -- estado do trigger (ADR-0035) — ver schema-h2.sql
    created_at      DATETIME2     NOT NULL,
    updated_at      DATETIME2     NOT NULL
);

IF OBJECT_ID('mohs_batches', 'U') IS NULL
CREATE TABLE mohs_batches (
    id         NVARCHAR(255) PRIMARY KEY,
    total      INT NOT NULL DEFAULT 0,
    succeeded  INT NOT NULL DEFAULT 0,
    failed     INT NOT NULL DEFAULT 0,
    created_at DATETIME2 NOT NULL
);

-- id é UUIDv7 (io.github.robsonkades:uuidv7) — time-ordered, mantém
-- inserts localizados no fim do índice da tabela mais quente do sistema.
IF OBJECT_ID('mohs_executions', 'U') IS NULL
CREATE TABLE mohs_executions (
    id               NVARCHAR(255) PRIMARY KEY,
    job_key          NVARCHAR(255) NOT NULL REFERENCES mohs_job_definitions(job_key),
    state            NVARCHAR(20)  NOT NULL,
    scheduled_at     DATETIME2     NOT NULL,
    fired_at         DATETIME2,
    actor            NVARCHAR(255) NOT NULL,
    idempotency_key  NVARCHAR(255),
    priority         INT           NOT NULL DEFAULT 20, -- Priority.value(); 20 = NORMAL
    node_id          NVARCHAR(255),  -- claim, etapa 3 (ADR-0016)
    lease_expires_at DATETIME2,      -- claim, etapa 3 (ADR-0012/0016)
    cancel_requested BIT           NOT NULL DEFAULT 0, -- cancel cooperativo (ADR-0034) — ver schema-h2.sql
    batch_id         NVARCHAR(255) REFERENCES mohs_batches(id),
    payload          NVARCHAR(MAX) NOT NULL, -- não CLOB/TEXT: deprecado em SQL Server
    payload_type     NVARCHAR(500) NOT NULL,
    created_at       DATETIME2     NOT NULL
);

-- Índice filtrado: só o backlog ENQUEUED é candidato a claim — o resto
-- da tabela (execuções terminais) é peso morto que este índice não
-- carrega. state sai das colunas porque o WHERE já fixa esse valor
-- (DBTUNE-5, medido: -95.2% Postgres / -84.2% SQL Server no tamanho do
-- índice, throughput de claim estável — docs/performance/BASELINE.md).
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_mohs_executions_claim' AND object_id = OBJECT_ID('mohs_executions'))
CREATE INDEX idx_mohs_executions_claim ON mohs_executions (priority, scheduled_at) WHERE state IN ('ENQUEUED', 'RETRY_SCHEDULED'); -- ADR-0033: par de estados claimáveis, ver schema-postgresql.sql
-- Índice filtrado pro reaper (DBTUNE-10): só a execução RUNNING é
-- candidata a reclaim — mesmo raciocínio da DBTUNE-5, WHERE em vez de
-- coluna porque o predicado já fixa o state.
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_mohs_executions_reaper' AND object_id = OBJECT_ID('mohs_executions'))
CREATE INDEX idx_mohs_executions_reaper ON mohs_executions (lease_expires_at) WHERE state = 'RUNNING';
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_mohs_executions_job_key' AND object_id = OBJECT_ID('mohs_executions'))
CREATE INDEX idx_mohs_executions_job_key ON mohs_executions (job_key);
-- Idempotent Receiver (EIP, DBTUNE-8) — ver schema-h2.sql. Filtrado:
-- índice único do SQL Server rejeitaria o SEGUNDO NULL sem o WHERE.
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'uq_mohs_executions_idem' AND object_id = OBJECT_ID('mohs_executions'))
CREATE UNIQUE INDEX uq_mohs_executions_idem ON mohs_executions (job_key, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_mohs_executions_batch_id' AND object_id = OBJECT_ID('mohs_executions'))
CREATE INDEX idx_mohs_executions_batch_id ON mohs_executions (batch_id);

IF OBJECT_ID('mohs_attempts', 'U') IS NULL
CREATE TABLE mohs_attempts (
    execution_id NVARCHAR(255) NOT NULL REFERENCES mohs_executions(id),
    number       INT           NOT NULL,
    started_at   DATETIME2     NOT NULL,
    finished_at  DATETIME2,
    outcome      NVARCHAR(20)  NOT NULL,
    error        NVARCHAR(MAX), -- não CLOB/TEXT: deprecado em SQL Server
    PRIMARY KEY (execution_id, number)
);

IF OBJECT_ID('mohs_rate_limits', 'U') IS NULL
CREATE TABLE mohs_rate_limits (
    name            NVARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_duration NVARCHAR(50) NOT NULL
);

-- Heartbeat de node (ADR-0012) — só informativo, GET /nodes; nenhuma
-- lógica de claim/reclaim consulta esta tabela.
IF OBJECT_ID('mohs_nodes', 'U') IS NULL
CREATE TABLE mohs_nodes (
    node_id           NVARCHAR(255) PRIMARY KEY,
    state             NVARCHAR(20) NOT NULL,
    last_heartbeat_at DATETIME2    NOT NULL
);
