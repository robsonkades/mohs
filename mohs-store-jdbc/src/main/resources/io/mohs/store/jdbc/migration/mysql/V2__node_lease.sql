-- Phase 4 do redesign (ADR-0051) — ver o V2 do Postgres para o porquê.
-- MySQL: ADD COLUMN/CREATE INDEX sem IF NOT EXISTS — guardas por
-- information_schema + SQL dinâmico, mesma forma (e mesmo motivo: adoção
-- idempotente) das guardas de índice da V1.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.columns
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_nodes'
                            AND column_name = 'epoch'),
                   'SELECT 1',
                   'ALTER TABLE mohs_nodes ADD COLUMN epoch BIGINT NOT NULL DEFAULT 0');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.columns
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_nodes'
                            AND column_name = 'expires_at'),
                   'SELECT 1',
                   'ALTER TABLE mohs_nodes ADD COLUMN expires_at DATETIME(6)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_executions'
                            AND index_name = 'idx_mohs_executions_owner'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_executions_owner ON mohs_executions (state, node_id)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
