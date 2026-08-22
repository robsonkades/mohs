package io.mohs.store.jdbc;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.mohs.store.jdbc.dialect.SqlServerJdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-0048 no Tier 2 SQL Server — adoção de instalação pré-Flyway: a V1
 * ({@code IF OBJECT_ID ... CREATE}) passa como no-op transacional por cima
 * do schema existente e o histórico nasce. Mesma rede de proteção de
 * {@code MohsFlywayMySqlTest} — o dialeto onde ela pegou o defeito.
 */
class MohsFlywaySqlServerTest {

    @Test
    void adoptsThePreFlywaySchemaOnSqlServer() {
        DataSource dataSource = SqlServerTestSupport.freshSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("IF OBJECT_ID('" + MohsFlyway.HISTORY_TABLE + "', 'U') IS NOT NULL DROP TABLE "
                + MohsFlyway.HISTORY_TABLE);
        MohsFlyway flyway = new MohsFlyway(dataSource, new SqlServerJdbcDialect());

        flyway.migrate();
        flyway.migrate();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM " + MohsFlyway.HISTORY_TABLE + " WHERE success = 1", Integer.class))
                .isGreaterThanOrEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_executions", Integer.class)).isNotNull();
    }
}
