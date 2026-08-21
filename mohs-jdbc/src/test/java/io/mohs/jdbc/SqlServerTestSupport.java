package io.mohs.jdbc;

import javax.sql.DataSource;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

/**
 * Container SQL Server compartilhado — mesmo desenho de {@link
 * PostgresTestSupport} (ADR-0023): singleton container, schema aplicado
 * uma vez só quando o container sobe. Isolamento por {@code DELETE}
 * (T-SQL proíbe {@code TRUNCATE} numa tabela referenciada por FK, mesmo
 * vazia na tabela filha — não é só uma checagem contornável como no
 * MySQL).
 */
final class SqlServerTestSupport {

    private static final MSSQLServerContainer CONTAINER = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2025-latest")
            .acceptLicense();

    static {
        CONTAINER.start();
        new ResourceDatabasePopulator(new ClassPathResource("schema-sqlserver.sql")).execute(dataSource());
    }

    private SqlServerTestSupport() {
    }

    private static DataSource dataSource() {
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(CONTAINER.getJdbcUrl());
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
