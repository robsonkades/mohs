-- Phase 5 do redesign (§7.2 do plano): o hot path sai da tabela de
-- história. Quatro perfis de escrita, quatro tabelas:
--   mohs_ready   — A FILA. INSERT no enqueue/retry/requeue, DELETE no claim;
--                  tamanho = backlog, nunca história (§5.3).
--   mohs_lease   — A POSSE. INSERT no claim, DELETE na conclusão; fence
--                  (node_id, epoch) — §6.2/6.3, o sucessor do fence
--                  (node_id, fired_at) da Phase 4.
--   mohs_execution / mohs_attempt — A HISTÓRIA. Append + UM update terminal,
--                  particionadas por tempo: retenção vira DROP de partição.
--   mohs_idempotency — a ÚNICA unicidade que atravessa partições (PG exige
--                  a partition key em todo índice único de tabela
--                  particionada, o que destruiria a semântica da dedup);
--                  o conflito de PK no insert É o check (Idempotent
--                  Receiver, EIP).
-- Expand da fase: as tabelas novas nascem AO LADO das antigas; o flip do
-- engine e o contract (drop das antigas) vêm depois. Timestamps aqui são
-- TIMESTAMPTZ (o que ficou adiado para estas tabelas); ids continuam VARCHAR.
-- Idempotente como toda migração daqui.

CREATE TABLE IF NOT EXISTS mohs_ready (
    execution_id VARCHAR(255) PRIMARY KEY,
    job_key      VARCHAR(255) NOT NULL,
    shard        SMALLINT     NOT NULL DEFAULT 0, -- ownership chega na Phase 6; 0 até lá
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
    correlation_id  VARCHAR(255),           -- batch até a Phase 8 generalizar
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

-- HISTÓRICO: quando esta migração foi escrita, as partições SEMANAIS eram
-- criadas adiante por um gestor no engine e a DEFAULT era o backstop de
-- operabilidade. O particionamento inteiro foi removido — gestor,
-- PARTITION BY e DEFAULT —, e a V5 converte estas duas tabelas em normais
-- logo em seguida. O que sobra aqui é a forma histórica, preservada porque
-- migração aplicada não se reescreve por estética.
-- Guardadas por "o pai é particionado?" desde essa remoção: quem já tinha o
-- schema aplicado a partir de schema-postgresql.sql e só depois aplica as
-- deltas chega aqui com as tabelas JÁ normais, e um CREATE ... PARTITION OF sobre
-- pai não-particionado é erro duro, não no-op — o IF NOT EXISTS não cobre
-- esse caso porque a queixa é sobre o pai. Em banco novo nada muda: o V3
-- acabou de criá-las particionadas e o V5 as converte em seguida.
-- `to_regclass` e não `relname`: a guarda TEM de resolver pelo MESMO
-- search_path que o DDL logo abaixo usa. Com `relname` sem namespace, um
-- schema vizinho que ainda tenha a tabela particionada faz a guarda dizer
-- "sim" para a tabela NORMAL do search_path — e o CREATE ... PARTITION OF
-- falha com exatamente o erro que a guarda existe para evitar (reproduzido
-- em PG 18). `to_regclass` devolve NULL para tabela inexistente, então
-- "banco vazio" também é no-op, sem precisar de segunda guarda.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass('mohs_execution')) THEN
        CREATE TABLE IF NOT EXISTS mohs_execution_default PARTITION OF mohs_execution DEFAULT;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass('mohs_attempt')) THEN
        CREATE TABLE IF NOT EXISTS mohs_attempt_default PARTITION OF mohs_attempt DEFAULT;
    END IF;
END $$;
