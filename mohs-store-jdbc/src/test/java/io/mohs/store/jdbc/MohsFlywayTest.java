package io.mohs.store.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
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
        assertThat(jdbc.queryForObject("SELECT count(*) FROM mohs_execution", Integer.class)).isZero();
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
     * O guardião das duas cópias da mesma verdade (ADR-0048), na forma que
     * a V2 exigiu: {@code schema-h2.sql} num banco e a cadeia Flyway
     * (V1+V2) noutro têm que produzir a MESMA estrutura — colunas e
     * índices das tabelas {@code mohs_*}. Sem isto, uma migração
     * divergente viraria no-op silencioso na adoção (as guardas comem a
     * diferença) e produção teria um schema que o fast-path dos testes
     * nunca viu. H2 é o guardião rápido; Postgres tem o próprio
     * ({@code MohsFlywayPostgresTest}) porque o predicado parcial do índice
     * owner (V2, DBTUNE-22) não existe em H2. MySQL/SQL Server ficam com os
     * testes de adoção (cadeia POR CIMA do schema-file, falham em conflito)
     * — SQL Server coberto por procuração do guardião PG na forma do filtro.
     */
    @Test
    void flywayChainMatchesTheSchemaFileStructurally() {
        DataSource fromSchemaFile = freshH2();
        new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")).execute(fromSchemaFile);
        DataSource fromFlyway = freshH2();
        new MohsFlyway(fromFlyway, new H2JdbcDialect()).migrate();

        assertThat(mohsStructure(fromFlyway)).isEqualTo(mohsStructure(fromSchemaFile));
    }

    /** Colunas + índices das tabelas mohs_* (fora o histórico do Flyway), num shape comparável por igualdade. */
    private static List<String> mohsStructure(DataSource dataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        List<String> structure = new ArrayList<>(jdbc.query("""
                SELECT LOWER(table_name) || '.' || LOWER(column_name) || ':' || data_type || ':' || is_nullable
                FROM information_schema.columns
                WHERE LOWER(table_name) LIKE 'mohs\\_%' AND LOWER(table_name) <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        // nomes de índice de PK e de constraint UNIQUE são auto-gerados pelo
        // H2 (sufixo posicional — mudou quando a V4 passou a criar e dropar
        // objetos a mais na cadeia) — normalizados; o que se compara é
        // "a PK/unicidade existe nestas colunas"
        structure.addAll(jdbc.query("""
                SELECT LOWER(i.table_name) || '.'
                       || CASE WHEN LOWER(i.index_name) LIKE 'primary\\_key%' THEN 'primary_key'
                               WHEN LOWER(i.index_name) LIKE 'constraint%' THEN 'unique_constraint'
                               ELSE LOWER(i.index_name) END
                       || '(' || LOWER(ic.column_name) || '@' || ic.ordinal_position || ')'
                FROM information_schema.indexes i
                JOIN information_schema.index_columns ic
                  ON ic.index_name = i.index_name AND ic.table_name = i.table_name
                WHERE LOWER(i.table_name) LIKE 'mohs\\_%' AND LOWER(i.table_name) <> ?
                ORDER BY 1
                """, (rs, _) -> rs.getString(1), MohsFlyway.HISTORY_TABLE));
        return structure;
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
