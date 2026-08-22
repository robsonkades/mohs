package io.mohs.store.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * As migrações de schema do Mohs, via Flyway PRÓPRIO da biblioteca
 * (ADR-0048, Phase 2 do redesign) — instância e histórico
 * ({@value #HISTORY_TABLE}) separados dos do host de propósito: o Mohs é
 * biblioteca embarcada e compartilha o banco com a aplicação hospedeira,
 * que pode ter o Flyway dela com o {@code flyway_schema_history} dela;
 * sequestrar (ou colidir com) a cadeia de migração do host seria o defeito
 * clássico da lib embarcada.
 *
 * <p>A {@code V1__mohs_baseline} de cada dialeto é o schema pré-Phase-2
 * VERBATIM e, como ele, é <b>idempotente</b> ({@code IF NOT EXISTS}/
 * {@code IF OBJECT_ID}): uma instalação existente (tabelas criadas à mão
 * pelos {@code schema-*.sql}, sem histórico) adota o Flyway rodando a V1
 * como no-op e ganhando o histórico. {@code baselineOnMigrate} com
 * {@code baselineVersion = 0} é a combinação deliberada: o Flyway exige
 * baseline para migrar schema não-vazio sem histórico — e "não-vazio" aqui
 * é a REGRA, não a exceção, porque o schema é compartilhado com o host —
 * mas baseline em 0 não pula migração nenhuma (a armadilha seria o default
 * 1, que marcaria a V1 como aplicada num banco onde só existem as tabelas
 * do host e nunca criaria as do Mohs). Migrações V2+ são delta normal, uma
 * vez só, pelo histórico.
 */
public final class MohsFlyway {

    private static final Logger log = LoggerFactory.getLogger(MohsFlyway.class);

    /** Nunca {@code flyway_schema_history} — ver Javadoc da classe. */
    public static final String HISTORY_TABLE = "mohs_schema_history";

    private final DataSource dataSource;
    private final JdbcDialect dialect;

    public MohsFlyway(DataSource dataSource, JdbcDialect dialect) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    public void migrate() {
        var result = Flyway.configure()
                .dataSource(dataSource)
                .table(HISTORY_TABLE)
                .locations(dialect.migrationLocation())
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate();
        if (result.migrationsExecuted > 0) {
            log.info("mohs schema migrated: {} migration(s) applied up to version {}",
                    result.migrationsExecuted, result.targetSchemaVersion);
        }
    }
}
