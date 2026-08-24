package io.mohs.store.jdbc;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.store.jdbc.dialect.PostgresJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O critério de validação da Phase 2 no Tier 1: migração aplicada a um
 * banco EXISTENTE — o container compartilhado já tem o schema aplicado
 * pelo {@code schema-postgresql.sql} (o caminho pré-Flyway), e a adoção
 * grava o {@code mohs_schema_history} com a V1 idempotente passando em
 * cima sem tocar nada.
 */
class MohsFlywayPostgresTest {

    /**
     * A V5 é a ÚNICA migração do projeto que MOVE LINHAS, e o caminho de
     * cópia dela só era exercitado com tabelas vazias: os outros testes ou
     * partem do schema já plano (onde a V5 é no-op) ou não passam por
     * Flyway. O guardião estrutural não cobre esta classe de defeito —
     * um par trocado entre colunas do MESMO tipo ({@code job_key}/
     * {@code actor}, {@code correlation_id}/{@code idempotency_key},
     * {@code error_type}/{@code error}) deixa a estrutura idêntica e
     * embaralha o banco do cliente em silêncio. Daí a concatenação
     * ordenada: ela pega a troca que a comparação de schema não vê.
     *
     * <p>É também o único ponto onde o laço de {@code RENAME CONSTRAINT} e
     * o ramo "estava particionada" rodam sobre dados.
     */
    @Test
    void v5CarriesEveryHistoryColumnAcrossTheDepartitioning() {
        DataSource dataSource = PostgresTestSupport.freshEmptyDatabase("mohs_v5_copy");
        Flyway.configure()
                .dataSource(dataSource)
                .table(MohsFlyway.HISTORY_TABLE)
                .locations(new PostgresJdbcDialect().migrationLocation())
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .target(MigrationVersion.fromVersion("3")) // para NA era particionada
                .load()
                .migrate();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.update("""
                INSERT INTO mohs_execution (execution_id, job_key, shard, priority, state, scheduled_at, created_at,
                                            finished_at, actor, correlation_id, idempotency_key, payload, payload_type)
                VALUES ('exec-1', 'job-a', 7, 5, 'SUCCEEDED', now(), now(), now(), 'alice', 'corr-1', 'idem-1', '{}', 'java.lang.Object')
                """);
        jdbc.update("""
                INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
                VALUES ('exec-1', 3, 'node-a', now(), now(), 'FAILED', 'java.io.IOException', 'boom')
                """);

        new MohsFlyway(dataSource, new PostgresJdbcDialect()).migrate();

        assertThat(jdbc.queryForObject("""
                SELECT job_key || '|' || actor || '|' || correlation_id || '|' || idempotency_key
                       || '|' || shard || '|' || priority
                  FROM mohs_execution WHERE execution_id = 'exec-1'
                """, String.class))
                .as("cada coluna tem de chegar na SUA coluna — troca entre colunas do mesmo tipo é invisível ao guardião estrutural")
                .isEqualTo("job-a|alice|corr-1|idem-1|7|5");
        assertThat(jdbc.queryForObject("""
                SELECT node_id || '|' || number || '|' || outcome || '|' || error_type || '|' || error
                  FROM mohs_attempt WHERE execution_id = 'exec-1'
                """, String.class)).isEqualTo("node-a|3|FAILED|java.io.IOException|boom");
        assertThat(jdbc.queryForList(
                "SELECT 1 FROM pg_partitioned_table WHERE partrelid = 'mohs_execution'::regclass"))
                .as("a conversão tem de ter acontecido de fato — senão o teste acima passaria sem a V5 fazer nada")
                .isEmpty();
    }

    @Test
    void adoptsThePreFlywaySchemaOnPostgres() {
        DataSource dataSource = PostgresTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("DROP TABLE IF EXISTS " + MohsFlyway.HISTORY_TABLE);

        new MohsFlyway(dataSource, new PostgresJdbcDialect()).migrate();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM \"" + MohsFlyway.HISTORY_TABLE + "\" WHERE \"success\"", Integer.class))
                .isGreaterThanOrEqualTo(1);
        // as tabelas pré-existentes seguem lá e utilizáveis (e a V4 dropou a era da tabela única)
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_execution", Integer.class)).isNotNull();
        assertThat(jdbc.queryForList("SELECT 1 FROM information_schema.tables WHERE table_name = 'mohs_executions'")).isEmpty();
    }

    /**
     * O guardião das duas cópias da verdade NO dialeto onde elas mais
     * divergem: {@code TIMESTAMPTZ}, storage options (V3/ADR-A) e a
     * DES-partição da ADR-0058 (a V5 recria as duas tabelas de história) só
     * existem em
     * Postgres — o guardião H2 de {@code MohsFlywayTest} não sabe
     * expressá-los. {@code pg_indexes.indexdef} carrega a forma completa —
     * um typo no {@code schema-postgresql.sql} ou uma V-script cujas
     * guardas comem a diferença falham AQUI, não viram no-op silencioso.
     */
    @Test
    void flywayChainMatchesTheSchemaFileStructurally() {
        DataSource fromSchemaFile = PostgresTestSupport.freshEmptyDatabase("mohs_struct_schema");
        new ResourceDatabasePopulator(new ClassPathResource("schema-postgresql.sql")).execute(fromSchemaFile);
        DataSource fromFlyway = PostgresTestSupport.freshEmptyDatabase("mohs_struct_flyway");
        new MohsFlyway(fromFlyway, new PostgresJdbcDialect()).migrate();

        assertThat(mohsStructure(fromFlyway)).isEqualTo(mohsStructure(fromSchemaFile));
    }

    /** Colunas + {@code indexdef} completo (inclui o predicado parcial) das tabelas {@code mohs_*}, fora o histórico do Flyway. */
    private static List<String> mohsStructure(DataSource dataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<String> structure = new ArrayList<>(jdbc.query("""
                SELECT table_name || '.' || column_name || ':' || data_type || ':' || is_nullable
                FROM information_schema.columns
                WHERE table_name LIKE 'mohs\\_%' AND table_name <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        structure.addAll(jdbc.query("""
                SELECT indexdef FROM pg_indexes
                WHERE tablename LIKE 'mohs\\_%' AND tablename <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        return structure;
    }
}
