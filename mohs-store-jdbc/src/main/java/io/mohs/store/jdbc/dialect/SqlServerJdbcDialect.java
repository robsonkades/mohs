package io.mohs.store.jdbc.dialect;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * SQL Server: sem {@code LIMIT} — usa {@code TOP (:batchSize)} logo após
 * {@code SELECT}, mudando a posição na query, não só o texto. Sem
 * {@code SKIP LOCKED} — usa o hint de tabela {@code WITH (UPDLOCK,
 * ROWLOCK, READPAST)}, confirmado via jOOQ (é o que jOOQ gera pra emular
 * {@code SKIP LOCKED} em SQL Server — ADR-0023). {@code BIT} compara com
 * {@code 1}, não {@code TRUE}.
 */
public final class SqlServerJdbcDialect implements JdbcDialect {

    @Override
    public String migrationLocation() {
        return "classpath:io/mohs/store/jdbc/migration/sqlserver";
    }

    @Override
    public String topClause() {
        return "TOP (:limit) ";
    }

    @Override
    public String limitClause() {
        return "";
    }

    /**
     * Varredura de candidatos do claim (§5.4) na forma T-SQL: {@code TOP}
     * + hint de tabela no lugar de {@code LIMIT … FOR UPDATE SKIP LOCKED}
     * — a emulação que o jOOQ confirma (ver Javadoc da classe). O resto do
     * claim (DELETE + INSERT da posse) segue o default portátil da
     * interface. Duas constantes porque {@code NOT IN} de lista vazia não
     * expande — a filtrada derivada por {@code replace}, como o
     * {@code CLAIM_READY_FILTERED} do Postgres.
     */
    private static final String TSQL_READY_CANDIDATES = """
            SELECT TOP (:limit) execution_id, job_key, attempt, priority
            FROM mohs_ready WITH (UPDLOCK, ROWLOCK, READPAST)
            WHERE shard = :shard AND visible_at <= :now
            ORDER BY priority, visible_at
            """;

    private static final String TSQL_READY_CANDIDATES_FILTERED = TSQL_READY_CANDIDATES.replace(
            "WHERE shard = :shard AND visible_at <= :now",
            "WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)");

    static {
        // guarda do replace: se a âncora do WHERE mudar e o replace no-opar,
        // o filtro de inadmissíveis sumiria em silêncio (review S5.2)
        if (!TSQL_READY_CANDIDATES_FILTERED.contains(":inadmissible")) {
            throw new ExceptionInInitializerError("TSQL_READY_CANDIDATES_FILTERED lost its :inadmissible predicate — the replace anchor drifted");
        }
    }

    @Override
    public List<ClaimedReady> selectReadyCandidates(NamedParameterJdbcTemplate jdbcTemplate, int shard, int limit,
            Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(TSQL_READY_CANDIDATES, params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(TSQL_READY_CANDIDATES_FILTERED,
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
    }

    /**
     * {@code NOLOCK} (read uncommitted), não {@code READPAST}: skip de
     * linha lockada subconta sistematicamente sob carga. O erro aceito é o
     * do pior caso do mecanismo, não só "±1 em transição": sem ordem
     * exigida ({@code COUNT}/{@code GROUP BY}) o otimizador pode escolher
     * allocation-order scan, que sob page split concorrente conta linha
     * duas vezes ou perde — erro proporcional ao churn de escrita; e o
     * scan pode falhar com o erro 601 ("data movement"), que aqui vira
     * falha transitória da leitura (500 no GET; WARN + retry no próximo
     * tick no stream) — aceito, sem retry automático. Deployment com RCSI
     * ({@code READ_COMMITTED_SNAPSHOT ON}) torna o hint redundante —
     * decisão do operador, não da biblioteca.
     */
    @Override
    public String lockFreeReadHint() {
        return "WITH (NOLOCK) ";
    }
}
