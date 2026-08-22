-- Phase 5, S5.4 (contract): a era da tabela única acaba. O engine roda
-- inteiro sobre o split (V3) desde o flip (S5.3); estas tabelas não têm
-- mais leitor nem escritor. mohs_attempts primeiro (FK para mohs_executions).
DROP TABLE IF EXISTS mohs_attempts;
DROP TABLE IF EXISTS mohs_executions;

-- ADR-D: o cap de concorrência deriva de mohs_lease (posse viva É a vaga
-- ocupada) — o contador de mutex da era ADR-0018/0020 morre junto.
ALTER TABLE mohs_job_definitions DROP COLUMN IF EXISTS running_execution_count;
