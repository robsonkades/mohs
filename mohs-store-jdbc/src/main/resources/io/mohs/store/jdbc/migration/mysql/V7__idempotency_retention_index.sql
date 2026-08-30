-- Revisão de codebase de 2026-08-29 (sem ADR própria: é correção medida, não
-- decisão de arquitetura): ver o V7 do Postgres para o porquê.
-- MySQL: CREATE/DROP INDEX sem IF [NOT] EXISTS — guarda por information_schema
-- + SQL dinâmico, mesma forma da V1/V2.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_idempotency'
                            AND index_name = 'idx_mohs_idempotency_created'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_idempotency_created ON mohs_idempotency (created_at)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_execution'
                            AND index_name = 'idx_mohs_execution_created'),
                   'DROP INDEX idx_mohs_execution_created ON mohs_execution',
                   'SELECT 1');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
