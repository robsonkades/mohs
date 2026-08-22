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
    max_concurrent_executions INT NOT NULL DEFAULT 0, -- só != 0 quando allow_concurrent_executions = FALSE (ADR-0020)
    running_execution_count INT NOT NULL DEFAULT 0, -- contador de mutex por job (ADR-0018/0020)
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
-- ADR-0033: RETRY_WAITING entrou no predicado do claim — índice parcial só é
-- elegível quando o predicado da query IMPLICA o do índice; IN (E, R) não
-- implica = E, e sem o par o plano degrada pra Seq Scan + Sort da tabela
-- inteira a cada tick.
CREATE INDEX IF NOT EXISTS idx_mohs_executions_claim ON mohs_executions (priority, scheduled_at) WHERE state IN ('ENQUEUED', 'RETRY_WAITING');
-- Índice parcial pro reaper (DBTUNE-10): só a execução RUNNING é
-- candidata a reclaim — mesmo raciocínio da DBTUNE-5, WHERE em vez de
-- coluna porque o predicado já fixa o state.
-- lease_expires_at IS NOT NULL no predicado (DBTUNE-17): sem ele, todo
-- UPDATE com "WHERE id = ? AND state = 'RUNNING'" (CAS de conclusão,
-- renovação de lease, cancel-request) implica o predicado do índice e o
-- planner o escolhe no lugar do PK — full scan das entradas RUNNING por
-- statement (medido: 8.4 ms/conclusão vs 0.05 ms pelo PK, com o índice
-- inchado pelo churn claim→conclusão). Com IS NOT NULL, só quem restringe
-- lease_expires_at (o reaper, operador estrito ⇒ IS NOT NULL provado)
-- continua elegível; os CAS por id SEM fence caem no PK. O CAS fenced do
-- reclaim (lease_expires_at = :expectedLease, anti-ABA da ADR-0033) ainda
-- implica o predicado — caminho frio em operação normal, mas quente em
-- reclaim em massa: medir (EXPLAIN) antes de confiar que o planner
-- escolhe o PK ali. Bases existentes precisam
-- recriar: CREATE INDEX CONCURRENTLY com o predicado novo, DROP
-- CONCURRENTLY do antigo, RENAME — CREATE IF NOT EXISTS não altera
-- índice já existente.
CREATE INDEX IF NOT EXISTS idx_mohs_executions_reaper ON mohs_executions (lease_expires_at) WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL;
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
-- Janela de vazão do GET /overview (âncora de polling): COUNT por outcome
-- com finished_at >= agora - janela. finished_at líder = range scan curto,
-- custo proporcional à atividade da janela, nunca ao histórico ("barato
-- por construção", REST-API-DESIGN); outcome na segunda coluna serve o
-- GROUP BY sem heap fetch (index-only). finished_at cresce monotônico —
-- inserts no fim do índice, mesma localidade do UUIDv7 (ADR-0040).
CREATE INDEX IF NOT EXISTS idx_mohs_attempts_throughput ON mohs_attempts (finished_at, outcome);

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
-- quem está morto (anti-join de JdbcReaper).
CREATE TABLE IF NOT EXISTS mohs_nodes (
    node_id           VARCHAR(255) PRIMARY KEY,
    state             VARCHAR(20) NOT NULL,
    last_heartbeat_at TIMESTAMP   NOT NULL,
    epoch             BIGINT      NOT NULL DEFAULT 0, -- encarnação do nó (ADR-0051)
    expires_at        TIMESTAMP                       -- lease do NÓ (ADR-0051); NULL = heartbeat de jar pré-Phase-4
);
-- Reaper dirigido por nó (ADR-0051): "RUNNING deste node" — parcial pelo
-- mesmo racional dos índices de claim/reaper (DBTUNE-5/10).
-- lease_expires_at IS NOT NULL no predicado (DBTUNE-22): o mesmo truque da
-- DBTUNE-17, pelo mesmo motivo — o CAS de conclusão cercado ("WHERE id = ?
-- AND state = 'RUNNING' AND node_id = ?") implica "state = 'RUNNING'" E tem
-- igualdade na chave, e o planner (seletividades de node_id × state
-- multiplicadas como independentes, quando são correlacionadas — node_id
-- novo a cada boot) o escolhia no lugar do PK: medido 41 buffers/1.84 ms
-- por conclusão vs 6/0.11 ms pelo PK. Com o conjunct, o CAS (que não
-- menciona lease_expires_at) fica inelegível; o reaper carrega o conjunct
-- trivialmente verdadeiro na query (toda linha RUNNING tem lease gravada
-- pelo UPDATE de claim — o invariante de countActiveByState) e segue
-- elegível.
CREATE INDEX IF NOT EXISTS idx_mohs_executions_owner ON mohs_executions (node_id) WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL;

-- ─── Phase 5 (ADR-A): o hot path fora da história ────────────────────────────
-- Quatro perfis de escrita, quatro tabelas (racional completo na migração
-- V3__table_split.sql — as duas cópias são mantidas idênticas pelo
-- guardião estrutural de MohsFlywayPostgresTest). Em transição (PLAN.md):
-- o engine flipa no S5.3; as tabelas antigas caem no S5.4.

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
    PRIMARY KEY (created_at, execution_id)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_id   ON mohs_execution (execution_id);
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
    PRIMARY KEY (finished_at, execution_id, number)
) PARTITION BY RANGE (finished_at);
CREATE INDEX IF NOT EXISTS idx_mohs_attempt_throughput ON mohs_attempt (finished_at, outcome);
CREATE INDEX IF NOT EXISTS idx_mohs_attempt_exec ON mohs_attempt (execution_id);

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (job_key, idempotency_key)
);

CREATE TABLE IF NOT EXISTS mohs_execution_default PARTITION OF mohs_execution DEFAULT;
CREATE TABLE IF NOT EXISTS mohs_attempt_default PARTITION OF mohs_attempt DEFAULT;
