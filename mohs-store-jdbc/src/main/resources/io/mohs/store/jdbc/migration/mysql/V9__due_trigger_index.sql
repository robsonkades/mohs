-- Codebase review of 2026-08-30 (no decision record of its own: this is a measured
-- correction, not an architecture decision): see the PostgreSQL V9 for the numbers.
-- MySQL has no CREATE INDEX IF NOT EXISTS - guarded by information_schema plus dynamic
-- SQL, the same shape as V1/V2/V7.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.statistics
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_job_definitions'
                            AND index_name = 'idx_mohs_job_next_fire'),
                   'SELECT 1',
                   'CREATE INDEX idx_mohs_job_next_fire ON mohs_job_definitions (next_fire_at)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
