package io.mohs.store.jdbc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.mohs.store.jdbc.dialect.H2JdbcDialect;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-0048 — os três cenários que a Phase 2 exige provar: banco novo
 * (migra do zero), banco de instalação EXISTENTE (tabelas criadas à mão
 * pelos {@code schema-*.sql}, sem histórico — a V1 idempotente adota sem
 * quebrar) e re-execução (no-op pelo histórico). O caso Postgres
 * equivalente mora em {@code MohsFlywayPostgresTest} (Testcontainers).
 */
class MohsFlywayTest {

    private static DataSource freshH2() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:mohs-flyway-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        h2.setPassword("");
        return h2;
    }

    /** Identificador quoted: o Flyway cria a tabela de histórico case-sensitive minúscula, e o H2 sobe identificador não-quoted pra maiúsculas. */
    private static int historyRows(DataSource dataSource) {
        Integer rows = new JdbcTemplate(dataSource).queryForObject(
                "SELECT count(*) FROM \"" + MohsFlyway.HISTORY_TABLE + "\" WHERE \"success\" = TRUE", Integer.class);
        return rows == null ? 0 : rows;
    }

    @Test
    void migratesAFreshDatabaseAndRecordsTheHistory() {
        DataSource dataSource = freshH2();

        new MohsFlyway(dataSource, new H2JdbcDialect()).migrate();

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_executions", Integer.class)).isZero();
        assertThat(historyRows(dataSource)).isGreaterThanOrEqualTo(1);
    }

    /** A adoção: instalação pré-Flyway (schema aplicado à mão) ganha o histórico sem a V1 quebrar em nada que já existe. */
    @Test
    void adoptsAnExistingInstallationWhoseSchemaWasCreatedByHand() {
        DataSource dataSource = freshH2();
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(dataSource);
        new JdbcTemplate(dataSource).update(
                "INSERT INTO mohs_batches (id, total, succeeded, failed, created_at) VALUES ('b1', 1, 0, 0, CURRENT_TIMESTAMP)");

        new MohsFlyway(dataSource, new H2JdbcDialect()).migrate();

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        // os dados pré-existentes sobrevivem à adoção — a V1 é no-op sobre eles
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_batches", Integer.class)).isEqualTo(1);
        assertThat(historyRows(dataSource)).isGreaterThanOrEqualTo(1);
    }

    /**
     * O guardião das duas cópias da mesma verdade (ADR-0048): enquanto a
     * V1 for o schema verbatim, igualdade literal — sem isto, uma V1
     * divergente viraria no-op silencioso na adoção (as guardas comem a
     * diferença) e produção teria um schema que o fast-path dos testes
     * nunca viu. Quando a V2 nascer, este teste vira a comparação
     * estrutural (schema ≡ V1+V2).
     */
    @ParameterizedTest
    @ValueSource(strings = { "h2", "postgresql", "mysql", "sqlserver" })
    void v1BaselineIsTheSchemaVerbatim(String dialect) throws IOException {
        String schema = new ClassPathResource("schema-" + dialect + ".sql")
                .getContentAsString(StandardCharsets.UTF_8);
        String baseline = new ClassPathResource(
                "io/mohs/store/jdbc/migration/" + dialect + "/V1__mohs_baseline.sql")
                .getContentAsString(StandardCharsets.UTF_8);
        assertThat(baseline).isEqualTo(schema);
    }

    @Test
    void aSecondMigrateIsANoOp() {
        DataSource dataSource = freshH2();
        MohsFlyway flyway = new MohsFlyway(dataSource, new H2JdbcDialect());
        flyway.migrate();
        int after = historyRows(dataSource);

        flyway.migrate();

        assertThat(historyRows(dataSource)).isEqualTo(after);
    }
}
