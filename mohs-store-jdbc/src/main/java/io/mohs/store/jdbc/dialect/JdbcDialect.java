package io.mohs.store.jdbc.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.store.jdbc.JdbcTimestamps;

/**
 * As poucas divergências reais de dialeto que {@code io.mohs.store.jdbc}
 * precisa (ADR-0023) — modelo na forma do {@code LimitHandler}/
 * {@code LockingStrategy} do Hibernate (interfaces pequenas, uma
 * preocupação cada), sem trazer o Hibernate como dependência: essas
 * duas interfaces vivem dentro do {@code Dialect}, que só existe depois
 * de inicializar {@code SessionFactory}/{@code ServiceRegistry} —
 * usá-las isoladamente significaria adotar boa parte do framework mesmo
 * assim.
 *
 * <p>Cada implementação é dona do template SQL inteiro do claim
 * ({@link #claimReady}/{@link #selectReadyCandidates}), não de fragmentos
 * concatenáveis — o {@code TOP} do SQL Server muda de <b>posição</b> na
 * query (logo após {@code SELECT}, não no fim como {@code LIMIT}), então
 * uma composição de fragmentos genéricos não fecha limpo. Mesmo padrão
 * que o próprio Quartz usa ({@code StdJDBCDelegate}/{@code MSSQLDelegate}:
 * cada Delegate tem o SQL completo de cada operação) e como Hibernate
 * realmente implementa {@code LimitHandler} por baixo (recebe o SQL e
 * devolve o SQL reescrito, não um fragmento).
 *
 * <p>Escolha explícita, nunca auto-detecção — mesmo padrão do Quartz
 * ({@code org.quartz.jobStore.driverDelegateClass}): detectar por
 * {@code Connection.getMetaData()} é frágil entre forks/versões de
 * driver.
 */
public interface JdbcDialect {

    /**
     * Onde vivem as migrações Flyway deste dialeto (ADR-0048) — caminho de
     * classpath fora do {@code db/migration} default de propósito, para
     * jamais ser varrido pelo Flyway do host. Cada dialeto conhece a
     * própria pasta pela mesma razão de conhecer o próprio SQL.
     */
    String migrationLocation();

    /**
     * Teto de linhas portátil ({@code io.mohs.store.jdbc.JdbcHistoryStore#findPage},
     * {@code JdbcLeaseStore#findOrphaned}): {@code :limit} é o mesmo
     * parâmetro nomeado nos dois métodos; só a posição do texto na query
     * muda. Default cobre H2/Postgres/MySQL ({@code LIMIT} no fim);
     * SQL Server sobrescreve os dois juntos ({@code TOP} logo após
     * {@code SELECT}).
     */
    default String topClause() {
        return "";
    }

    default String limitClause() {
        return "LIMIT :limit";
    }

    /**
     * Hint de tabela pras leituras que não podem tomar lock: o princípio é
     * que monitoramento e sondagem jamais disputem com o caminho quente do
     * claim/conclusão. Default vazio — em H2/Postgres/MySQL um {@code
     * SELECT} MVCC já não toma lock de linha (leitura consistente). SQL
     * Server sobrescreve: sob {@code READ COMMITTED} default (sem RCSI)
     * todo {@code SELECT} toma shared locks que bloqueiam e são bloqueados
     * pelos {@code UPDATE}s do motor. As anomalias aceitas (linha em
     * transição, dupla contagem/perda sob page split, erro 601 como falha
     * transitória) estão no Javadoc do override do SQL Server; nunca usar
     * em leitura que hidrata entidade.
     *
     * <p>Hoje o ÚNICO chamador é a sonda do gate ocioso
     * ({@code JdbcWorkQueue#hasVisibleWork}, S6.5) — daí "Read" e não
     * "Count" no nome. Ela é ainda mais tolerante que uma contagem: perder
     * linha sob page split só vira falso negativo se o scan perder TODAS
     * as visíveis, e o desfecho é um poll; o erro 601 cai no fallback
     * fail-open do {@code Engine}, que devolve o tick ao lap.
     *
     * <p>As contagens do {@code GET /overview} ({@code
     * JdbcHistoryStore#countActiveByState}/{@code
     * countTerminalOutcomesSince}) foram reescritas sobre as tabelas do
     * split na Phase 5 e NÃO usam mais o hint — em SQL Server sem RCSI
     * elas voltaram a tomar shared locks nas três tabelas quentes.
     * Pendência com gatilho no PLAN.md; não é regressão deste método.
     */
    default String lockFreeReadHint() {
        return "";
    }

    // ─── Phase 5 (ADR-A): as costuras das tabelas do split ──────────────────

    /**
     * Travessia temporal das tabelas do split (PLAN.md, decisão 6): elas
     * nasceram {@code TIMESTAMPTZ} no Postgres (§7.2 — o que a ADR-0049
     * adiou) e permanecem sem fuso nos equivalentes funcionais. Default =
     * a travessia {@code LocalDateTime} UTC de sempre; Postgres sobrescreve
     * com {@code OffsetDateTime} — {@code LocalDateTime} numa coluna
     * tz-aware seria interpretado pelo fuso da SESSÃO, a classe de bug que
     * a ADR-0049 matou. As tabelas de control plane ({@code
     * mohs_job_definitions}/{@code mohs_nodes}/{@code mohs_batches}) seguem
     * {@link JdbcTimestamps} direto — reshape delas é fase própria.
     */
    default Object splitTimestamp(Instant instant) {
        return JdbcTimestamps.toUtcLocalDateTime(instant);
    }

    default @Nullable Instant readSplitTimestamp(ResultSet rs, String column) throws SQLException {
        LocalDateTime value = rs.getObject(column, LocalDateTime.class);
        return value == null ? null : JdbcTimestamps.fromUtcLocalDateTime(value);
    }

    /**
     * Template portátil da varredura de candidatos do claim da Phase 5
     * (§5.4): single-shard por statement (lição do E2 — multi-shard no
     * predicado mata a ordenação do índice), ordenado por
     * {@code (priority, visible_at)} que o índice {@code idx_mohs_ready_claim}
     * fornece sem Sort. Compartilhado por H2/MySQL; SQL Server sobrescreve
     * {@link #selectReadyCandidates} ({@code TOP} + lock por hint);
     * Postgres sobrescreve {@link #claimReady} inteiro (statement único).
     * Duas constantes em vez de concatenação: {@code NOT IN} de lista
     * vazia não expande.
     */
    String ANSI_READY_CANDIDATES = """
            SELECT execution_id, job_key, attempt, priority
            FROM mohs_ready
            WHERE shard = :shard AND visible_at <= :now
            ORDER BY priority, visible_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """;

    String ANSI_READY_CANDIDATES_FILTERED = """
            SELECT execution_id, job_key, attempt, priority
            FROM mohs_ready
            WHERE shard = :shard AND visible_at <= :now AND job_key NOT IN (:inadmissible)
            ORDER BY priority, visible_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """;

    String READY_DELETE = "DELETE FROM mohs_ready WHERE execution_id IN (:ids)";

    String LEASE_INSERT = """
            INSERT INTO mohs_lease (execution_id, job_key, node_id, epoch, attempt_number, priority, claimed_at)
            VALUES (:executionId, :jobKey, :nodeId, :epoch, :attempt, :priority, :now)
            """;

    default List<ClaimedReady> selectReadyCandidates(NamedParameterJdbcTemplate jdbcTemplate, int shard, int limit,
            Collection<String> inadmissibleJobKeys, Instant now) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("shard", shard)
                .addValue("now", splitTimestamp(now))
                .addValue("limit", limit);
        if (inadmissibleJobKeys.isEmpty()) {
            return jdbcTemplate.query(ANSI_READY_CANDIDATES, params, ClaimedReady::fromReadyRow);
        }
        return jdbcTemplate.query(ANSI_READY_CANDIDATES_FILTERED,
                params.addValue("inadmissible", inadmissibleJobKeys), ClaimedReady::fromReadyRow);
    }

    /**
     * O claim da Phase 5 (§5.4/§6.2): remove da fila e insere a posse — o
     * CHAMADOR ({@code JdbcWorkQueue}) garante a transação que torna os
     * dois efeitos atômicos (não existe instante "nem na fila nem
     * possuído"). Default portátil em três statements (SELECT com lock →
     * DELETE → INSERT em lote); Postgres sobrescreve com o statement único
     * {@code WITH picked … DELETE … RETURNING → INSERT}.
     */
    default List<ClaimedReady> claimReady(NamedParameterJdbcTemplate jdbcTemplate, int shard, String nodeId, long epoch,
            int limit, Collection<String> inadmissibleJobKeys, Instant now) {
        List<ClaimedReady> picked = selectReadyCandidates(jdbcTemplate, shard, limit, inadmissibleJobKeys, now);
        if (picked.isEmpty()) {
            return picked;
        }
        jdbcTemplate.update(READY_DELETE, new MapSqlParameterSource()
                .addValue("ids", picked.stream().map(ClaimedReady::executionId).toList()));
        MapSqlParameterSource[] leases = picked.stream()
                .map(row -> new MapSqlParameterSource()
                        .addValue("executionId", row.executionId())
                        .addValue("jobKey", row.jobKey())
                        .addValue("nodeId", nodeId)
                        .addValue("epoch", epoch)
                        .addValue("attempt", row.attempt())
                        .addValue("priority", row.priority())
                        .addValue("now", splitTimestamp(now)))
                .toArray(MapSqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(LEASE_INSERT, leases);
        return picked;
    }
}
