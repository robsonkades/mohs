-- Phase 4 do redesign: a liveness passa do lease por execução
-- (renovado a cada tick — Finding A: ~5 updates/execução medidos sob
-- in-flight sustentado) para o lease de NÓ: mohs_nodes ganha epoch
-- (encarnação do nó, monotônica por node_id) e expires_at (o lease em si,
-- renovado pelo heartbeat). O reaper passa a ser dirigido por nó morto —
-- o índice novo serve "RUNNING deste node". lease_expires_at em
-- mohs_executions NÃO sai: vira o token de encarnação da posse (fence de
-- toda conclusão) e sustenta rollback pro jar anterior sem migração.
-- Idempotente como toda migração daqui: schema-*.sql continua
-- existindo como caminho paralelo de instalação.
ALTER TABLE mohs_nodes ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE mohs_nodes ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;

-- DBTUNE-22: o predicado carrega lease_expires_at IS NOT NULL — o MESMO
-- truque da DBTUNE-17 no índice do reaper, pelo MESMO motivo: todo CAS de
-- conclusão cercado ("WHERE id = ? AND state = 'RUNNING' AND node_id = ?")
-- implicaria um predicado só de state E tem igualdade na coluna-chave, e o
-- planner (multiplicando seletividades de node_id × state, perfeitamente
-- correlacionadas — node_id novo a cada boot) o escolheria no
-- lugar do PK: medido 41 buffers/1.84 ms por conclusão vs 6 buffers/
-- 0.11 ms pelo PK. Com este predicado o CAS (que não menciona
-- lease_expires_at) fica INELEGÍVEL — o PK vence por construção, não por
-- sorte de custo. O reaper continua elegível porque a query dele carrega o
-- conjunct trivialmente verdadeiro (toda linha RUNNING tem lease gravada
-- pelo UPDATE de claim — o invariante que countActiveByState já usa desde
-- a DBTUNE-17).
-- Guarda por FORMA, não por nome: um owner de shape transitório
-- (pré-squash desta release, sem o predicado de lease) é derrubado antes;
-- o de shape final (schema-postgresql.sql atual) passa direto.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes
               WHERE schemaname = current_schema() AND tablename = 'mohs_executions'
                 AND indexname = 'idx_mohs_executions_owner'
                 AND indexdef NOT LIKE '%lease_expires_at%') THEN
        DROP INDEX idx_mohs_executions_owner;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_mohs_executions_owner ON mohs_executions (node_id)
    WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL;
