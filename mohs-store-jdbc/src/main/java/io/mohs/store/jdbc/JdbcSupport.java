package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.engine.WorkQueue;
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * Convenções JDBC compartilhadas entre os stores de {@code io.mohs.store.jdbc} —
 * mesmo padrão idiomático já usado em {@code io.mohs.cron}
 * ({@code Assert}/{@code StringUtils}: classe final package-private,
 * métodos estáticos.
 */
final class JdbcSupport {

    /**
     * DBTUNE-7: sem isto, {@code queryForStream} não é um cursor de verdade
     * nos drivers reais — pgjdbc materializa a consulta inteira em memória
     * no {@code executeQuery} sem {@code fetchSize > 0} (e ainda exige
     * {@code autoCommit = false} pra valer; Connector/J precisa da mesma
     * coisa, ou {@code useCursorFetch=true}). O valor é um meio-termo
     * (100-500 é a faixa recomendada) — nunca medido neste projeto, só
     * fecha a lacuna entre o Javadoc de {@code HistoryStore#findAll} e o
     * que os drivers realmente fazem sem isto configurado.
     */
    static final int STREAM_FETCH_SIZE = 200;

    /** Bem abaixo do teto de 2100 parâmetros do SQL Server pro {@code IN (:ids)} (DB-11) — o in-flight de um nó passa fácil de 1k ids. */
    static final int MAX_IDS_PER_QUERY = 1000;

    /**
     * {@code INSERT} em {@code mohs_ready} — enqueue, retry e requeue são a
     * MESMA operação com {@code visibleAt} diferente (§5.8 do redesign):
     * {@code JdbcWorkQueue#offer}/{@code #requeue} e o renascimento de retry
     * na transação de conclusão de {@code JdbcLeaseStore#complete}
     * compartilham este statement e {@link #readyEntryParams}.
     */
    static final String READY_INSERT = """
            INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
            VALUES (:executionId, :jobKey, :shard, :priority, :attempt, :visibleAt)
            """;

    /**
     * {@code DELETE} de {@code mohs_lease} cercado por {@code (node_id,
     * epoch)} — o fencing token do §6.3: a lease só cai se ainda for da
     * encarnação observada. O MESMO statement decide o fence no requeue
     * ({@code JdbcWorkQueue}) e na conclusão ({@code JdbcLeaseStore}) —
     * compartilhado para a semântica jamais divergir entre os dois.
     */
    static final String FENCED_LEASE_DELETE = """
            DELETE FROM mohs_lease
            WHERE execution_id = :executionId AND node_id = :nodeId AND epoch = :epoch
            """;

    private JdbcSupport() {
    }

    /** Fatias de no máximo {@link #MAX_IDS_PER_QUERY} ids pro {@code IN (:ids)} — views de {@code subList}, sem cópia. */
    static List<List<String>> chunksOf(List<String> ids) {
        List<List<String>> chunks = new ArrayList<>();
        for (int start = 0; start < ids.size(); start += MAX_IDS_PER_QUERY) {
            chunks.add(ids.subList(start, Math.min(start + MAX_IDS_PER_QUERY, ids.size())));
        }
        return chunks;
    }

    /** Parâmetros de {@link #READY_INSERT} — a travessia temporal de {@code visibleAt} é do dialeto ({@code splitTimestamp}). */
    static MapSqlParameterSource readyEntryParams(WorkQueue.ReadyEntry entry, JdbcDialect dialect) {
        return new MapSqlParameterSource()
                .addValue("executionId", entry.executionId().value())
                .addValue("jobKey", entry.jobKey().value())
                .addValue("shard", entry.shard())
                .addValue("priority", entry.priority())
                .addValue("attempt", entry.attempt())
                .addValue("visibleAt", dialect.splitTimestamp(entry.visibleAt()));
    }

    /** Parâmetros de {@link #FENCED_LEASE_DELETE}. */
    static MapSqlParameterSource fencedLeaseDeleteParams(String executionId, String nodeId, long epoch) {
        return new MapSqlParameterSource()
                .addValue("executionId", executionId)
                .addValue("nodeId", nodeId)
                .addValue("epoch", epoch);
    }

    /** {@code NamedParameterJdbcTemplate} com {@link #STREAM_FETCH_SIZE} — todo store que tem método {@code queryForStream} usa este construtor, não {@code new NamedParameterJdbcTemplate(dataSource)} direto. */
    static NamedParameterJdbcTemplate namedTemplateWithStreamFetchSize(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(STREAM_FETCH_SIZE);
        return new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    @FunctionalInterface
    interface SingleRowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    /**
     * Busca uma linha por uma condição que casa no máximo uma (chave
     * primária ou única) — {@code ResultSetExtractor} guardado por
     * {@code rs.next()} lê essa linha direto, sem passar por
     * {@code List}/{@code stream}/{@code findFirst}, e sem o risco de
     * {@code queryForObject} (que lança {@code EmptyResultDataAccessException}
     * em vez de devolver vazio quando não há linha nenhuma).
     */
    static <T> Optional<T> findOne(NamedParameterJdbcTemplate jdbcTemplate, String sql, MapSqlParameterSource params, SingleRowMapper<T> mapper) {
        T result = jdbcTemplate.query(sql, params, rs -> rs.next() ? mapper.map(rs) : null);
        return Optional.ofNullable(result);
    }
}
