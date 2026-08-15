-- Schema JDBC do Mohs (io.mohs.jdbc) para H2 — dialeto embarcado/teste.
-- Mohs é biblioteca embarcada, compartilha banco/schema com a aplicação
-- hospedeira, não pode colidir com tabela dela. Durations viram VARCHAR
-- via Duration.toString()/Duration.parse() (ISO-8601, ex. "PT30S") —
-- mais legível pra debug manual que millis, nenhuma delas precisa de
-- range query. Sem execution_windows/runners: os dois são bean-resolved
-- ("predicados só existem em código", §5.8 do documento mestre), não
-- dado persistido. ADR-0023: um arquivo por dialeto — H2/Postgres eram
-- idênticos até agora, mas MySQL/SQL Server divergem o bastante pra não
-- fazer mais sentido um "schema.sql" genérico.
-- DBTUNE-1: toda coluna temporal guarda wall-clock em UTC — nenhuma tem
-- fuso (TIMESTAMP aqui é "without time zone"). Gravado/lido só via
-- io.mohs.jdbc.JdbcTimestamps (nunca java.sql.Timestamp.from(instant)/
-- Timestamp.toInstant() direto), que normaliza pra UTC independente do
-- fuso default da JVM de cada nó.

-- id é UUIDv7 (io.github.robsonkades:uuidv7), mesma geração de mohs_executions.id
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
    retired         BOOLEAN      NOT NULL DEFAULT FALSE, -- aposentadoria explícita (Mohs.remove): some das leituras/claim, linha fica pela FK de mohs_executions (histórico preservado)
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
    batch_id         VARCHAR(255) REFERENCES mohs_batches(id),
    payload          TEXT         NOT NULL, -- não CLOB: não existe em Postgres, TEXT funciona em H2 e Postgres (DB-3)
    payload_type     VARCHAR(500) NOT NULL,
    created_at       TIMESTAMP    NOT NULL
);
-- H2 não tem índice parcial/filtrado — Postgres e SQL Server usam
-- WHERE state IN ('ENQUEUED', 'RETRY_SCHEDULED') aqui (DBTUNE-5, ADR-0033); H2 fica com a composta cheia.
CREATE INDEX IF NOT EXISTS idx_mohs_executions_claim ON mohs_executions (state, priority, scheduled_at);
-- Sem índice parcial (ver comentário acima) — composta cheia pro reaper
-- também (DBTUNE-10): state líder, igual à do claim.
CREATE INDEX IF NOT EXISTS idx_mohs_executions_reaper ON mohs_executions (state, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_mohs_executions_job_key ON mohs_executions (job_key);
-- Idempotent Receiver (EIP, DBTUNE-8): unicidade por (job_key, idempotency_key)
-- no lugar do índice comum — é o banco que sustenta o contrato do header
-- Idempotency-Key, nunca um SELECT prévio. NULLs são distintos no H2
-- (NULLS DISTINCT default): execuções sem chave nunca colidem.
CREATE UNIQUE INDEX IF NOT EXISTS uq_mohs_executions_idem ON mohs_executions (job_key, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_mohs_executions_batch_id ON mohs_executions (batch_id);

CREATE TABLE IF NOT EXISTS mohs_attempts (
    execution_id VARCHAR(255) NOT NULL REFERENCES mohs_executions(id),
    number       INT          NOT NULL,
    started_at   TIMESTAMP    NOT NULL,
    finished_at  TIMESTAMP,
    outcome      VARCHAR(20)  NOT NULL,
    error        TEXT, -- não CLOB: não existe em Postgres, TEXT funciona em H2 e Postgres (DB-3)
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
