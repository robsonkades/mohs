-- Phase 5, S5.4 (contract): a era da tabela única acaba. O engine roda
-- inteiro sobre o split (V3) desde o flip (S5.3); estas tabelas não têm
-- mais leitor nem escritor. mohs_attempts primeiro (FK para mohs_executions).
DROP TABLE IF EXISTS mohs_attempts;
DROP TABLE IF EXISTS mohs_executions;

-- ADR-D: o cap de concorrência deriva de mohs_lease (posse viva É a vaga
-- ocupada) — o contador de mutex da era ADR-0018/0020 morre junto.
-- Guardado por COL_LENGTH: no caminho de ADOÇÃO, o schema.sql pós-S5.4
-- nunca criou a coluna. O DEFAULT inline da V1 virou constraint sem nome —
-- precisa cair antes da coluna, e o nome só se descobre em runtime.
IF COL_LENGTH('mohs_job_definitions', 'running_execution_count') IS NOT NULL
BEGIN
    DECLARE @constraint NVARCHAR(200);
    SELECT @constraint = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID('mohs_job_definitions') AND c.name = 'running_execution_count';
    IF @constraint IS NOT NULL EXEC('ALTER TABLE mohs_job_definitions DROP CONSTRAINT ' + @constraint);
    ALTER TABLE mohs_job_definitions DROP COLUMN running_execution_count;
END
