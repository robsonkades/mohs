-- ADR-0062: o nome do lote vira dado durável.
--
-- Mohs.batch(name, ...) exigia um nome, fazia requireNonNull nele e o
-- descartava: não ia para o store, não aparecia em BatchSnapshot, em
-- BatchCompleted nem em GET /batches/{id}. Quem escrevia
-- mohs.batch("nightly-invoices", ...) e abria o dashboard às 3h achava um
-- UUID e nada mais. Parâmetro em API pública é promessa.
--
-- Guarda de idempotência na mesma forma da V2 (adoção sobre base que já
-- tem o schema aplicado por schema-*.sql, onde a coluna já nasce).
-- Backfill antes do NOT NULL: lote antigo não tem nome a recuperar e
-- recebe o próprio id, para a migração ser segura com dados.
-- MySQL: ADD COLUMN sem IF NOT EXISTS — guarda por information_schema +
-- SQL dinâmico, mesma forma da V2.
SET @mohs_sql = IF(EXISTS(SELECT 1 FROM information_schema.columns
                          WHERE table_schema = DATABASE() AND table_name = 'mohs_batches'
                            AND column_name = 'name'),
                   'SELECT 1',
                   'ALTER TABLE mohs_batches ADD COLUMN name VARCHAR(255)');
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;
UPDATE mohs_batches SET name = id WHERE name IS NULL;
ALTER TABLE mohs_batches MODIFY name VARCHAR(255) NOT NULL;
