package io.mohs.store.jdbc;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

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
     * divergem: particionamento de {@code mohs_execution}/{@code mohs_attempt},
     * {@code TIMESTAMPTZ} e storage options (V3/ADR-A) só existem em
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
