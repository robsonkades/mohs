-- Schema JDBC do Mohs (io.mohs.store.jdbc) para PostgreSQL — idêntico ao
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
    rate_limit      VARCHAR(255), -- nome do RateLimit cluster-wide (ADR-0042)
    misfire         VARCHAR(20)  NOT NULL,
    start_paused    BOOLEAN      NOT NULL DEFAULT FALSE, -- definicional (ADR-0037) — ver schema-h2.sql
    allow_concurrent_executions BOOLEAN NOT NULL DEFAULT TRUE,
    max_concurrent_executions INT NOT NULL DEFAULT 0, -- só != 0 quando allow_concurrent_executions = FALSE (ADR-0020); o cap deriva de mohs_lease (ADR-D)
    retries         INT          NOT NULL DEFAULT 0,
    timeout         VARCHAR(50),
    retry_policy    VARCHAR(255),
    source          VARCHAR(20)  NOT NULL, -- ANNOTATION | PROGRAMMATIC
    orphaned        BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional (ADR-0006)
    paused          BOOLEAN      NOT NULL DEFAULT FALSE, -- operacional (ADR-0006)
    retired         BOOLEAN      NOT NULL DEFAULT FALSE, -- aposentadoria explícita (Mohs.remove) — ver schema-h2.sql
    next_fire_at    TIMESTAMP,   -- estado do trigger (ADR-0035) — ver schema-h2.sql
    created_at      TIMESTAMP    NOT NULL,
    updated_at      TIMESTAMP    NOT NULL
);

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
    -- balde de tokens (ADR-0042): capacidade = max_count, um token a cada window/max
    tokens          INT NOT NULL,
    refilled_at     TIMESTAMP NOT NULL
);

-- Heartbeat de node (ADR-0012). Desde a ADR-0051 deixou de ser só
-- informativa: o reaper consulta expires_at/last_heartbeat_at para decidir
-- quem está morto (aliveNodeIds do Engine).
CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at TIMESTAMP   NOT NULL,
    epoch             BIGINT      NOT NULL DEFAULT 0, -- encarnação do nó (ADR-0051)
    expires_at        TIMESTAMP                       -- lease do NÓ (ADR-0051); NULL = heartbeat de jar pré-Phase-4
);

-- ─── Phase 5 (ADR-A): o hot path fora da história ────────────────────────────
-- Quatro perfis de escrita, quatro tabelas (racional completo na migração
-- V3__table_split.sql — as duas cópias são mantidas idênticas pelo
-- guardião estrutural de MohsFlywayPostgresTest).

CREATE TABLE IF NOT EXISTS mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0,
    priority     INT          NOT NULL DEFAULT 20,
    attempt      INT          NOT NULL,
    visible_at   TIMESTAMPTZ  NOT NULL
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);
CREATE INDEX IF NOT EXISTS idx_mohs_ready_claim ON mohs_ready (shard, priority, visible_at);

CREATE TABLE IF NOT EXISTS mohs_lease (
    execution_id     VARCHAR(255) PRIMARY KEY,
    job_key          VARCHAR(255) NOT NULL,
    node_id          VARCHAR(255) NOT NULL,
    epoch            BIGINT       NOT NULL,
    attempt_number   INT          NOT NULL,
    priority         INT          NOT NULL DEFAULT 20, -- viaja fila->posse: o requeue do reaper reconstroi a entrada sem ler historia (S5.3)
    claimed_at       TIMESTAMPTZ  NOT NULL,
    cancel_requested BOOLEAN      NOT NULL DEFAULT FALSE
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);
CREATE INDEX IF NOT EXISTS idx_mohs_lease_node ON mohs_lease (node_id, epoch);
CREATE INDEX IF NOT EXISTS idx_mohs_lease_job  ON mohs_lease (job_key);

CREATE TABLE IF NOT EXISTS mohs_execution (
    execution_id    VARCHAR(255) NOT NULL,
    job_key         VARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL,
    scheduled_at    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    finished_at     TIMESTAMPTZ,
    actor           VARCHAR(255) NOT NULL,
    correlation_id  VARCHAR(255),
    idempotency_key VARCHAR(255),
    payload         TEXT         NOT NULL,
    payload_type    VARCHAR(500) NOT NULL,
    -- execution_id sozinho, como nos outros três dialetos: a coluna de tempo
    -- à frente era exigência do particionamento, e sem ele a PK não servia
    -- consulta nenhuma — o planner escolhia idx_mohs_execution_id e rebaixava
    -- created_at a Filter. Era um índice de duas colunas mantido em toda
    -- escrita só pela unicidade; agora a unicidade é a do id, que é o que o
    -- domínio sempre disse.
    PRIMARY KEY (execution_id)
);
-- (job_key, execution_id): serve o ORDER BY/cursor do findPage — ver o V3 do Postgres
CREATE INDEX IF NOT EXISTS idx_mohs_execution_job  ON mohs_execution (job_key, execution_id DESC);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_corr ON mohs_execution (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS mohs_attempt (
    execution_id VARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      VARCHAR(255) NOT NULL,
    started_at   TIMESTAMPTZ  NOT NULL,
    finished_at  TIMESTAMPTZ  NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,
    error_type   VARCHAR(500),
    error        TEXT,
    PRIMARY KEY (execution_id, number)
);
CREATE INDEX IF NOT EXISTS idx_mohs_attempt_throughput ON mohs_attempt (finished_at, outcome);
-- idx_mohs_attempt_exec saiu junto: (execution_id) é prefixo exato da PK acima

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_mohs_idempotency_created ON mohs_idempotency (created_at);

