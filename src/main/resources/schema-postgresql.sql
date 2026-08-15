-- Schema JDBC do Mohs (io.mohs.jdbc) para PostgreSQL — idêntico ao
-- dialeto H2 (ver schema-h2.sql para o raciocínio de cada coluna;
-- ADR-0022/0023). Prefixo mohs_ em toda tabela — Mohs é biblioteca
-- embarcada, compartilha banco/schema com a aplicação hospedeira.

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
    retired         BOOLEAN      NOT NULL DEFAULT FALSE, -- aposentadoria explícita (Mohs.remove) — ver schema-h2.sql
    created_at      TIMESTAMP    NOT NULL,
    updated_at      TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS mohs_batches (
    id         VARCHAR(255) PRIMARY KEY,
    total      INT NOT NULL DEFAULT 0,
    succeeded  INT NOT NULL DEFAULT 0,
    failed     INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL
);

-- id é UUIDv7 (io.github.robsonkades:uuidv7) — time-ordered, mantém
-- inserts localizados no fim do índice da tabela mais quente do sistema.
CREATE TABLE IF NOT EXISTS mohs_executions (
    id               VARCHAR(255) PRIMARY KEY,
    job_key          VARCHAR(255) NOT NULL REFERENCES mohs_job_definitions(job_key),
    state            VARCHAR(20)  NOT NULL,
    scheduled_at     TIMESTAMP    NOT NULL,
    fired_at         TIMESTAMP,
    actor            VARCHAR(255) NOT NULL,
    idempotency_key  VARCHAR(255),
    priority         INT          NOT NULL DEFAULT 20, -- Priority.value(); 20 = NORMAL
    node_id          VARCHAR(255),  -- claim, etapa 3 (ADR-0016)
    lease_expires_at TIMESTAMP,     -- claim, etapa 3 (ADR-0012/0016)
    cancel_requested BOOLEAN      NOT NULL DEFAULT FALSE, -- cancel cooperativo (ADR-0034) — ver schema-h2.sql
    batch_id         VARCHAR(255) REFERENCES mohs_batches(id),
    payload          TEXT         NOT NULL, -- não CLOB: não existe em Postgres (DB-3)
    payload_type     VARCHAR(500) NOT NULL,
    created_at       TIMESTAMP    NOT NULL
);
-- Índice parcial: só o backlog ENQUEUED é candidato a claim — o resto da
-- tabela (execuções terminais) é peso morto que este índice não carrega.
-- state sai das colunas porque o WHERE já fixa esse valor (DBTUNE-5,
-- medido: -95.2% Postgres / -84.2% SQL Server no tamanho do índice,
-- throughput de claim estável — docs/performance/BASELINE.md).
-- ADR-0033: RETRY_SCHEDULED entrou no predicado do claim — índice parcial só é
-- elegível quando o predicado da query IMPLICA o do índice; IN (E, R) não
-- implica = E, e sem o par o plano degrada pra Seq Scan + Sort da tabela
-- inteira a cada tick.
CREATE INDEX IF NOT EXISTS idx_mohs_executions_claim ON mohs_executions (priority, scheduled_at) WHERE state IN ('ENQUEUED', 'RETRY_SCHEDULED');
-- Índice parcial pro reaper (DBTUNE-10): só a execução RUNNING é
-- candidata a reclaim — mesmo raciocínio da DBTUNE-5, WHERE em vez de
-- coluna porque o predicado já fixa o state.
CREATE INDEX IF NOT EXISTS idx_mohs_executions_reaper ON mohs_executions (lease_expires_at) WHERE state = 'RUNNING';
CREATE INDEX IF NOT EXISTS idx_mohs_executions_job_key ON mohs_executions (job_key);
-- Idempotent Receiver (EIP, DBTUNE-8) — ver schema-h2.sql. Parcial: só
-- linhas com chave entram no índice (NULL nunca colide e não pesa aqui).
CREATE UNIQUE INDEX IF NOT EXISTS uq_mohs_executions_idem ON mohs_executions (job_key, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mohs_executions_batch_id ON mohs_executions (batch_id);

CREATE TABLE IF NOT EXISTS mohs_attempts (
    execution_id VARCHAR(255) NOT NULL REFERENCES mohs_executions(id),
    number       INT          NOT NULL,
    started_at   TIMESTAMP    NOT NULL,
    finished_at  TIMESTAMP,
    outcome      VARCHAR(20)  NOT NULL,
    error        TEXT, -- não CLOB: não existe em Postgres (DB-3)
    PRIMARY KEY (execution_id, number)
);

CREATE TABLE IF NOT EXISTS mohs_rate_limits (
    name            VARCHAR(255) PRIMARY KEY,
    max_count       INT NOT NULL,
    window_duration VARCHAR(50) NOT NULL
);

-- Heartbeat de node (ADR-0012) — só informativo, GET /nodes; nenhuma
-- lógica de claim/reclaim consulta esta tabela.
CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at TIMESTAMP   NOT NULL
);
