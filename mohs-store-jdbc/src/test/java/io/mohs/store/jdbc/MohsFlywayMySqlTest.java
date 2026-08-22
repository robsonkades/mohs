package io.mohs.store.jdbc;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.store.jdbc.dialect.MySqlJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-0048 no Tier 2 MySQL — o cenário que a bancada da Phase 2 pegou
 * quebrado: os {@code CREATE INDEX} do MySQL não tinham guarda ({@code IF
 * NOT EXISTS} não existe pra índice), a adoção de instalação pré-Flyway
 * falhava com 1061 no MEIO do script (DDL comita implicitamente), ficava
 * {@code success=false} no histórico e todo boot seguinte morria em
 * validação — loop permanente. A V1 guardada por {@code
 * information_schema} + SQL dinâmico é a correção; a adoção aqui é a
 * regressão.
 */
class MohsFlywayMySqlTest {

    @Test
    void adoptsThePreFlywaySchemaOnMySql() {
        DataSource dataSource = MySqlTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("DROP TABLE IF EXISTS " + MohsFlyway.HISTORY_TABLE);
        MohsFlyway flyway = new MohsFlyway(dataSource, new MySqlJdbcDialect());

        flyway.migrate();
        // re-execução: no-op pelo histórico — se a V1 tivesse falhado no meio,
        // esta segunda chamada seria o loop de boot do achado
        flyway.migrate();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM " + MohsFlyway.HISTORY_TABLE + " WHERE success = 1", Integer.class))
                .isGreaterThanOrEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_execution", Integer.class)).isNotNull();
    }
}
