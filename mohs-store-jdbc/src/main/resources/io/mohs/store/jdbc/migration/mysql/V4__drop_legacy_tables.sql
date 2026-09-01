-- Phase 5, S5.4 (contract): a era da tabela única acaba. O engine roda
-- inteiro sobre o split (V3) desde o flip (S5.3); estas tabelas não têm
-- mais leitor nem escritor. mohs_attempts primeiro (FK para mohs_executions).
DROP TABLE IF EXISTS mohs_attempts;
DROP TABLE IF EXISTS mohs_executions;

-- O cap de concorrência deriva de mohs_lease (posse viva É a vaga
-- ocupada) — o contador de mutex da era anterior morre junto.
-- Guardado (mesma dança PREPARE do schema-mysql.sql): no caminho de
-- ADOÇÃO, o schema.sql pós-S5.4 nunca criou a coluna — MySQL não tem
-- DROP COLUMN IF EXISTS.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.columns
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_job_definitions'
                            AND column_name = 'running_execution_count'),
                   'ALTER TABLE mohs_job_definitions DROP COLUMN running_execution_count',
                   'SELECT 1');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
