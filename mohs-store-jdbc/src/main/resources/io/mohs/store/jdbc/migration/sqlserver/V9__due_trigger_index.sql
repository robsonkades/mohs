-- Codebase review of 2026-08-30 (no decision record of its own: this is a measured
-- correction, not an architecture decision): see the PostgreSQL V9 for the numbers.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mohs_job_next_fire'
               AND object_id = OBJECT_ID('mohs_job_definitions', 'U'))
    CREATE NONCLUSTERED INDEX idx_mohs_job_next_fire ON mohs_job_definitions (next_fire_at);
