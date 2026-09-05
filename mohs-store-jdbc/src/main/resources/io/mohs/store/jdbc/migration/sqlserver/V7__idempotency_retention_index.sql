-- Revisão de codebase de 2026-08-29 (sem ADR própria: é correção medida, não
-- decisão de arquitetura): ver o V7 do Postgres para o porquê.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mohs_idempotency_created'
               AND object_id = OBJECT_ID('mohs_idempotency', 'U'))
    CREATE INDEX idx_mohs_idempotency_created ON mohs_idempotency (created_at);
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mohs_execution_created'
           AND object_id = OBJECT_ID('mohs_execution', 'U'))
    DROP INDEX idx_mohs_execution_created ON mohs_execution;
