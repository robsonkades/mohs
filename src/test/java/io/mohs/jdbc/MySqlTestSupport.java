package io.mohs.jdbc;

import javax.sql.DataSource;

import com.mysql.cj.jdbc.MysqlDataSource;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.mysql.MySQLContainer;

/**
 * Container MySQL compartilhado — mesmo desenho de {@link
 * PostgresTestSupport} (ADR-0023): singleton container, schema aplicado
 * uma vez só quando o container sobe (MySQL não tem {@code CREATE INDEX
 * IF NOT EXISTS} — reaplicar o script a cada teste quebraria no segundo
 * teste em diante, índice já existe). Isolamento por {@code DELETE} em
 * vez de {@code TRUNCATE} (MySQL exige desabilitar {@code
 * FOREIGN_KEY_CHECKS} pra truncar tabela referenciada por FK — {@code
 * DELETE} respeitando a ordem das FKs é mais simples e não depende de
 * estado de sessão).
 */
final class MySqlTestSupport {

    private static final MySQLContainer CONTAINER = new MySQLContainer("mysql:8.0");

    static {
        CONTAINER.start();
        new ResourceDatabasePopulator(new ClassPathResource("schema-mysql.sql")).execute(dataSource());
    }

    private MySqlTestSupport() {
    }

    private static DataSource dataSource() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(CONTAINER.getJdbcUrl());
        dataSource.setUser(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        return dataSource;
    }

    /** Limpa todas as tabelas — o schema já foi aplicado uma vez quando o container subiu. */
    static DataSource freshSchema() {
        DataSource dataSource = dataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("DELETE FROM mohs_attempts");
        jdbcTemplate.execute("DELETE FROM mohs_executions");
        jdbcTemplate.execute("DELETE FROM mohs_job_definitions");
        jdbcTemplate.execute("DELETE FROM mohs_batches");
        jdbcTemplate.execute("DELETE FROM mohs_rate_limits");
        jdbcTemplate.execute("DELETE FROM mohs_nodes");
        return dataSource;
    }
}
