package io.mohs.store.jdbc;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Container Postgres compartilhado entre as classes de teste que validam
 * contra um banco real (ADR-0022) — singleton container: sobe uma vez por
 * JVM de teste, nunca é parado explicitamente (Ryuk do Testcontainers
 * cuida do cleanup no fim da JVM), evitando o custo de subir um container
 * novo por classe. Isolamento entre testes não vem de um banco novo por
 * teste (como {@code freshH2DataSource()} faz via URL única em memória) —
 * vem de {@code TRUNCATE ... CASCADE} antes de cada teste, no mesmo
 * schema compartilhado.
 */
final class PostgresTestSupport {

    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:16-alpine");

    static {
        CONTAINER.start();
        DataSource dataSource = dataSource();
        // schema aplicado uma vez só, aqui — não em freshSchema(), que
        // roda antes de cada teste contra o mesmo container: reaplicar
        // schema-postgresql.sql é idempotente (CREATE TABLE IF NOT
        // EXISTS), mas não precisa, é trabalho à toa repetido.
        new ResourceDatabasePopulator(new ClassPathResource("schema-postgresql.sql")).execute(dataSource);
    }

    private PostgresTestSupport() {
    }

    private static DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(CONTAINER.getJdbcUrl());
        dataSource.setUser(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        return dataSource;
    }

    /** Conexão CRUA pro listener de NOTIFY — a conexão de LISTEN é dedicada, fora de qualquer pool/DataSource (S6.3). */
    static String jdbcUrl() {
        return CONTAINER.getJdbcUrl();
    }

    static String username() {
        return CONTAINER.getUsername();
    }

    static String password() {
        return CONTAINER.getPassword();
    }

    /** Limpa todas as tabelas — o schema já foi aplicado uma vez quando o container subiu. */
    static DataSource freshSchema() {
        DataSource dataSource = dataSource();
        new JdbcTemplate(dataSource).execute(
                "TRUNCATE TABLE mohs_job_definitions, mohs_batches, mohs_rate_limits, mohs_nodes, mohs_ready, mohs_lease, mohs_execution, mohs_attempt, mohs_idempotency CASCADE");
        return dataSource;
    }

    /**
     * Um database NOVO e vazio no mesmo container — pro guardião estrutural
     * (schema-file × cadeia Flyway), que precisa de dois schemas construídos
     * do zero por caminhos diferentes, coisa que o database compartilhado
     * (schema já aplicado no static) não oferece.
     */
    static DataSource freshEmptyDatabase(String name) {
        new JdbcTemplate(dataSource()).execute("DROP DATABASE IF EXISTS " + name + "; CREATE DATABASE " + name);
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerNames(new String[] { CONTAINER.getHost() });
        dataSource.setPortNumbers(new int[] { CONTAINER.getMappedPort(5432) });
        dataSource.setDatabaseName(name);
        dataSource.setUser(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        return dataSource;
    }
}
