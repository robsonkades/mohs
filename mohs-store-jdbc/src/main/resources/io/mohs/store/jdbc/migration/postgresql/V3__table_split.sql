-- Phase 5 do redesign (ADR-A, §7.2 do plano): o hot path sai da tabela de
-- história. Quatro perfis de escrita, quatro tabelas:
--   mohs_ready   — A FILA. INSERT no enqueue/retry/requeue, DELETE no claim;
--                  tamanho = backlog, nunca história (§5.3).
--   mohs_lease   — A POSSE. INSERT no claim, DELETE na conclusão; fence
--                  (node_id, epoch) — §6.2/6.3, o sucessor do fence
--                  (node_id, fired_at) da ADR-0051.
--   mohs_execution / mohs_attempt — A HISTÓRIA. Append + UM update terminal,
--                  particionadas por tempo: retenção vira DROP de partição.
--   mohs_idempotency — a ÚNICA unicidade que atravessa partições (PG exige
--                  a partition key em todo índice único de tabela
--                  particionada, o que destruiria a semântica da dedup);
--                  o conflito de PK no insert É o check (Idempotent
--                  Receiver, EIP).
-- Expand da fase (PLAN.md S5.1): as tabelas novas nascem AO LADO das
-- antigas; o flip do engine é o S5.3 e o contract (drop das antigas) o
-- S5.4. Timestamps aqui são TIMESTAMPTZ (o que a ADR-0049 adiou para
-- estas tabelas); ids continuam VARCHAR (PLAN.md, decisão 7).
-- Idempotente como toda migração daqui (ADR-0048).

CREATE TABLE IF NOT EXISTS mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0, -- ownership chega na Phase 6 (ADR-F); 0 até lá
    priority     INT          NOT NULL DEFAULT 20,
    attempt      INT          NOT NULL,           -- o attempt que esta entrada VAI virar (§5.3)
    visible_at   TIMESTAMPTZ  NOT NULL
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);
-- A ordem (shard, priority, visible_at) É o ORDER BY do claim single-shard
-- (§5.4 — a lição do E2: multi-shard no predicado mata a ordenação do
-- índice). SEM o INCLUDE do plano (§5.3): FOR UPDATE exige LockRows, que
-- força heap access — a varredura de candidatos nunca é index-only. Medido
-- (PG 18, 50k backlog): plano e buffers idênticos com/sem INCLUDE, e o
-- INCLUDE custa 2.7x de índice (4208 vs 1552 kB) e +43% de WAL nos mesmos
-- 50k inserts (20 vs 14 MB) — só write amplification, zero leitura.
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
CREATE INDEX IF NOT EXISTS idx_mohs_lease_node ON mohs_lease (node_id, epoch); -- reaper + backstop de cancel
CREATE INDEX IF NOT EXISTS idx_mohs_lease_job  ON mohs_lease (job_key);        -- cap de concorrência derivado (§5.7)

CREATE TABLE IF NOT EXISTS mohs_execution (
    execution_id    VARCHAR(255) NOT NULL,
    job_key         VARCHAR(255) NOT NULL,
    shard           SMALLINT     NOT NULL DEFAULT 0,
    priority        INT          NOT NULL DEFAULT 20,
    state           VARCHAR(20)  NOT NULL, -- read model ADVISORY (§6.2): a verdade em voo é a lease
    scheduled_at    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    finished_at     TIMESTAMPTZ,
    actor           VARCHAR(255) NOT NULL,
    correlation_id  VARCHAR(255),           -- batch (ADR-0043) até a Phase 8 generalizar
    idempotency_key VARCHAR(255),
    payload         TEXT         NOT NULL,
    payload_type    VARCHAR(500) NOT NULL,
    PRIMARY KEY (created_at, execution_id)  -- a partition key precisa liderar a PK (§7.3)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_id   ON mohs_execution (execution_id); -- point lookup por id
-- (job_key, execution_id) e não (job_key, created_at): o único consumidor de
-- job_key aqui é o findPage, que ordena e pagina por execution_id (UUIDv7,
-- time-ordered) — igualdade primeiro, ORDER BY depois serve listagem E cursor
-- como Index Cond (medido no S5.3: 0.61 ms → 0.24 ms no job seletivo, e o
-- plano vira O(limit) em vez de O(linhas do job)); created_at na 2ª posição
-- não servia a query nenhuma (from/to filtra scheduled_at; retenção é DROP).
CREATE INDEX IF NOT EXISTS idx_mohs_execution_job  ON mohs_execution (job_key, execution_id DESC);
CREATE INDEX IF NOT EXISTS idx_mohs_execution_corr ON mohs_execution (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS mohs_attempt (
    execution_id VARCHAR(255) NOT NULL,
    number       INT          NOT NULL,
    node_id      VARCHAR(255) NOT NULL, -- forense de E6: QUEM executou cada attempt
    started_at   TIMESTAMPTZ  NOT NULL,
    finished_at  TIMESTAMPTZ  NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,
    error_type   VARCHAR(500),          -- classe da exceção — a query operacional nº 1 (§7.3)
    error        TEXT,
    PRIMARY KEY (finished_at, execution_id, number)
) PARTITION BY RANGE (finished_at);
CREATE INDEX IF NOT EXISTS idx_mohs_attempt_throughput ON mohs_attempt (finished_at, outcome);
-- Detail view (attempts de UMA execução): a PK particionada lidera por
-- finished_at e não serve igualdade em execution_id — sem este índice a
-- leitura é seq scan da história (medido: 19 ms/15.7k buffers em 1.1M
-- attempts vs 0.035 ms/7 buffers com ele). Nos outros dialetos a PK
-- (execution_id, number) já cobre.
CREATE INDEX IF NOT EXISTS idx_mohs_attempt_exec ON mohs_attempt (execution_id);

CREATE TABLE IF NOT EXISTS mohs_idempotency (
    job_key         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    execution_id    VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL, -- poda pela janela de idempotência, não pela retenção
    PRIMARY KEY (job_key, idempotency_key)
);

-- Só o esqueleto estático aqui: partições SEMANAIS são criadas adiante
-- pelo gestor de partições do engine (S5.2) — nomes dependem da data, e
-- SQL procedural (DO $$) não sobrevive aos DOIS caminhos de instalação
-- (o ResourceDatabasePopulator do schema-file quebra statements por ';'
-- sem entender dollar-quoting). A DEFAULT é o backstop de operabilidade:
-- um create-ahead perdido NUNCA derruba o enqueue; linha na default é
-- WARN do gestor, não outage.
CREATE TABLE IF NOT EXISTS mohs_execution_default PARTITION OF mohs_execution DEFAULT;
CREATE TABLE IF NOT EXISTS mohs_attempt_default PARTITION OF mohs_attempt DEFAULT;
