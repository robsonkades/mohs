-- Codebase review of 2026-09-04 (no decision record of its own: this is a correction to V3, not
-- an architecture decision). V1 declared DEFAULT CHARACTER SET utf8mb4 on every table it created,
-- with the reason written into its header: never depend on the server's default. V3 created the
-- five tables of the table split without the clause, so on a server whose character_set_database
-- is not utf8mb4 their job_key columns ended up in a different character set from
-- mohs_job_definitions.job_key — and the two statements that compare those columns across tables
-- (rearmExecutionByCas, pruneEmptyBatchesBefore) either fail with "Illegal mix of collations" or
-- coerce one side and lose the index. The installer and V3 now carry the clause; this delta
-- converts a database that ran the old V3. If you folded V3 into your own Flyway/Liquibase chain,
-- do NOT re-copy it (its checksum changed) — apply this file only.
--
-- Three things about its shape, all measured on MySQL 8.0 and 8.4 before it was written:
--
-- 1. The target is the COLLATION of the V1 tables, read from mohs_job_definitions, not "any
--    utf8mb4". DEFAULT CHARACTER SET utf8mb4 without COLLATE takes the charset's default collation
--    (utf8mb4_0900_ai_ci), while a table without the clause inherits collation_database — so on a
--    server whose default is utf8mb4 with another collation (utf8mb4_general_ci is common on older
--    images) the five tables are ALREADY utf8mb4 and the two statements STILL fail: two utf8mb4
--    collations mix as illegally as utf8mb4 and latin1. Each guard therefore compares collations
--    for equality, and each ALTER names the collation explicitly. The same equality is what makes
--    a second run a no-op (a fresh install, or a database already converted).
-- 2. mohs_execution and mohs_attempt are converted column by column (DEFAULT CHARACTER SET plus
--    MODIFY), not with CONVERT TO CHARACTER SET: CONVERT TO widens a MEDIUMTEXT to LONGTEXT so the
--    same number of characters still fits in 4-byte utf8mb4, and the migrated database would then
--    differ from the installer's on payload and error. A MODIFY repeats the column's full
--    definition on purpose — nullability and defaults come from V3, which is the source of truth.
--    The three tables without a TEXT column keep the shorter CONVERT TO form.
-- 3. The conversion is ALGORITHM=COPY, InnoDB refuses INPLACE for a column type change: the table
--    is rebuilt, writers block for the whole copy (an INSERT waited 19.5 s behind 300k rows of
--    mohs_execution — about 80 µs per row, minutes per million), and the exclusive metadata lock at
--    the end queues READERS behind any transaction that still holds the table. lock_wait_timeout
--    defaults to a year; 2 s is the mandatory pair of that lock, as lock_timeout is in the
--    PostgreSQL V5: a fast, visible failure that can be retried (each guard makes the re-run a
--    resume) rather than an outage behind an unrelated long transaction. Drain the cluster first —
--    see docs/06-data/migrations.md, "V10".
--
-- Widest key affected: mohs_idempotency's PRIMARY KEY (job_key, idempotency_key), 2 x 255 x 4 =
-- 2040 bytes, under InnoDB's 3072-byte limit for DYNAMIC rows.

SET SESSION lock_wait_timeout = 2;

SET @mohs_collation = (SELECT table_collation FROM information_schema.tables
                       WHERE table_schema = DATABASE() AND table_name = 'mohs_job_definitions');

SET @mohs_sql = IF((SELECT table_collation FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name = 'mohs_ready') = @mohs_collation,
                   'SELECT 1',
                   CONCAT('ALTER TABLE mohs_ready CONVERT TO CHARACTER SET utf8mb4 COLLATE ', @mohs_collation));
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

SET @mohs_sql = IF((SELECT table_collation FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name = 'mohs_lease') = @mohs_collation,
                   'SELECT 1',
                   CONCAT('ALTER TABLE mohs_lease CONVERT TO CHARACTER SET utf8mb4 COLLATE ', @mohs_collation));
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

SET @mohs_sql = IF((SELECT table_collation FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name = 'mohs_idempotency') = @mohs_collation,
                   'SELECT 1',
                   CONCAT('ALTER TABLE mohs_idempotency CONVERT TO CHARACTER SET utf8mb4 COLLATE ', @mohs_collation));
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

SET @mohs_sql = IF((SELECT table_collation FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name = 'mohs_execution') = @mohs_collation,
                   'SELECT 1',
                   CONCAT('ALTER TABLE mohs_execution DEFAULT CHARACTER SET utf8mb4 COLLATE ', @mohs_collation,
                          ', MODIFY execution_id    VARCHAR(255) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY job_key         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY state           VARCHAR(20)  CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY actor           VARCHAR(255) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY correlation_id  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation,
                          ', MODIFY idempotency_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation,
                          ', MODIFY payload         MEDIUMTEXT   CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY payload_type    VARCHAR(500) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL'));
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

SET @mohs_sql = IF((SELECT table_collation FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name = 'mohs_attempt') = @mohs_collation,
                   'SELECT 1',
                   CONCAT('ALTER TABLE mohs_attempt DEFAULT CHARACTER SET utf8mb4 COLLATE ', @mohs_collation,
                          ', MODIFY execution_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY node_id      VARCHAR(255) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY outcome      VARCHAR(20)  CHARACTER SET utf8mb4 COLLATE ', @mohs_collation, ' NOT NULL',
                          ', MODIFY error_type   VARCHAR(500) CHARACTER SET utf8mb4 COLLATE ', @mohs_collation,
                          ', MODIFY error        MEDIUMTEXT   CHARACTER SET utf8mb4 COLLATE ', @mohs_collation));
PREPARE mohs_stmt FROM @mohs_sql;
EXECUTE mohs_stmt;
DEALLOCATE PREPARE mohs_stmt;

SET SESSION lock_wait_timeout = DEFAULT;
