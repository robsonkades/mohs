-- Phase 4 do redesign (ADR-0051) — ver o V2 do Postgres para o porquê,
-- inclusive o predicado do índice owner (DBTUNE-22: sem
-- lease_expires_at no filtro, o CAS de conclusão cercado o implicaria e
-- competiria com o seek pelo PK clustered; com ele, o PK vence por
-- construção). Guardas T-SQL de statement único (mesma forma da V1); a
-- guarda do índice é por FORMA do filtro, não por nome — owner de shape
-- transitório (pré-squash desta release) cai, o de shape final
-- (schema-sqlserver.sql atual) passa direto.
IF COL_LENGTH('mohs_nodes', 'epoch') IS NULL
ALTER TABLE mohs_nodes ADD epoch BIGINT NOT NULL DEFAULT 0;
IF COL_LENGTH('mohs_nodes', 'expires_at') IS NULL
ALTER TABLE mohs_nodes ADD expires_at DATETIME2;
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_mohs_executions_owner' AND object_id = OBJECT_ID('mohs_executions')
           AND (filter_definition IS NULL OR filter_definition NOT LIKE '%lease_expires_at%'))
DROP INDEX idx_mohs_executions_owner ON mohs_executions;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_mohs_executions_owner' AND object_id = OBJECT_ID('mohs_executions'))
CREATE INDEX idx_mohs_executions_owner ON mohs_executions (node_id) WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL;
