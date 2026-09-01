-- Consequência do NVARCHAR, decidida em vez de descoberta em produção.
--
-- Trocar VARCHAR por NVARCHAR (DB-5) dobra os bytes da chave, e mohs_idempotency
-- é a única do schema que isso empurra para fora de um limite: a PK clusterizada
-- (job_key, idempotency_key) passa a medir 2 x (255 x 2) = 1020 bytes contra o
-- teto de 900 do índice CLUSTERIZADO. O não-clusterizado subiu para 1700 no 2016+,
-- o clusterizado não. Medido: 225+225 caracteres entra, 256+255 falha com
--   Msg 1946 ... exceeds the maximum length of 900 bytes for clustered indexes
-- no INSERT do enqueue. idempotency_key vem de header do cliente, e o schema
-- declara aceitar 255 — o corte em ~450 somados seria uma armadilha silenciosa.
--
-- A saída é tirar a PK do papel de chave clusterizada, não estreitar a coluna
-- (divergiria dos outros três dialetos) nem deixar a tabela heap (o DELETE de
-- retenção num heap não desaloca páginas sem TABLOCK, e esta é justamente a
-- tabela que mais poda). Clusterizar por created_at NÃO é neutro, e a conta tem os
-- dois lados. A favor: o valor é monotônico (Clock injetado), então o
-- INSERT segue na cauda e a poda por retenção vira range delete na própria
-- clusterizada — o que torna idx_mohs_idempotency_created da V7 redundante NESTE
-- dialeto. Contra: o INSERT passa a manter DUAS estruturas, o SELECT de dedupe
-- vira seek + Key Lookup, e a mesma monotonicidade concentra todos os nós na
-- última página (PAGELATCH_EX). A troca é obrigatória de qualquer forma — 900
-- bytes é limite duro —, mas o SALDO ainda não foi medido.
IF EXISTS (SELECT 1 FROM sys.indexes
           WHERE object_id = OBJECT_ID('mohs_idempotency', 'U') AND is_primary_key = 1 AND type_desc = 'CLUSTERED')
BEGIN
    DECLARE @pk NVARCHAR(200) = (SELECT name FROM sys.indexes
        WHERE object_id = OBJECT_ID('mohs_idempotency', 'U') AND is_primary_key = 1);
    EXEC('ALTER TABLE mohs_idempotency DROP CONSTRAINT ' + @pk);
    CREATE CLUSTERED INDEX ix_mohs_idempotency_created ON mohs_idempotency (created_at);
    ALTER TABLE mohs_idempotency ADD CONSTRAINT pk_mohs_idempotency
        PRIMARY KEY NONCLUSTERED (job_key, idempotency_key);
END
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mohs_idempotency_created'
           AND object_id = OBJECT_ID('mohs_idempotency', 'U'))
    DROP INDEX idx_mohs_idempotency_created ON mohs_idempotency;
