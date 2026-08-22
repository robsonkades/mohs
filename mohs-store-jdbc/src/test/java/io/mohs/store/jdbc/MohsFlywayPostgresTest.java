package io.mohs.store.jdbc;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

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
        // as tabelas pré-existentes seguem lá e utilizáveis
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_executions", Integer.class)).isNotNull();
    }
}
