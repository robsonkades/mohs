-- Phase 4 do redesign (ADR-0051) — ver o V2 do Postgres para o porquê.
-- H2 não tem índice parcial: composta (state, node_id) no lugar do
-- filtrado, mesma divergência que os índices de claim/reaper já têm.
ALTER TABLE mohs_nodes ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE mohs_nodes ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;
CREATE INDEX IF NOT EXISTS idx_mohs_executions_owner ON mohs_executions (state, node_id);
