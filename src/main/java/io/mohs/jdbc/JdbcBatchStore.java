package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import io.mohs.engine.BatchCounters;
import io.mohs.engine.BatchStore;

/** {@link BatchStore} sobre {@code mohs_batches} (Data Mapper, PoEAA). */
public final class JdbcBatchStore implements BatchStore {

    private static final String INCREMENT_SUCCEEDED =
            "UPDATE mohs_batches SET succeeded = succeeded + 1 WHERE id = :id";

    private static final String INCREMENT_FAILED =
            "UPDATE mohs_batches SET failed = failed + 1 WHERE id = :id";

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Clock clock;

    public JdbcBatchStore(DataSource dataSource, Clock clock) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void insert(String batchId, int total) {
        Objects.requireNonNull(batchId, "batchId");
        if (total < 0) {
            throw new IllegalArgumentException("total must not be negative");
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", batchId)
                .addValue("total", total)
                .addValue("createdAt", JdbcTimestamps.toUtcTimestamp(clock.instant()));
        jdbcTemplate.update("""
                INSERT INTO mohs_batches (id, total, succeeded, failed, created_at)
                VALUES (:id, :total, 0, 0, :createdAt)
                """, params);
    }

    /** DBTUNE-6: colunas explícitas em vez de {@code SELECT *} — {@code created_at} é forense de operador, nunca lido aqui, e esta releitura roda em TODA conclusão de membro. */
    @Override
    public Optional<BatchCounters> find(String batchId) {
        Objects.requireNonNull(batchId, "batchId");
        return JdbcSupport.findOne(jdbcTemplate,
                "SELECT id, total, succeeded, failed FROM mohs_batches WHERE id = :id",
                new MapSqlParameterSource("id", batchId),
                JdbcBatchStore::mapRow);
    }

    @Override
    public BatchCounters incrementSucceeded(String batchId) {
        return incrementAndRead(batchId, INCREMENT_SUCCEEDED);
    }

    @Override
    public BatchCounters incrementFailed(String batchId) {
        return incrementAndRead(batchId, INCREMENT_FAILED);
    }

    /**
     * {@code UPDATE} e depois {@code SELECT}, em vez do {@code UPDATE ...
     * RETURNING} que a ADR-0043 nomeia: o H2 não suporta a cláusula (medido
     * ao implementar) e o MySQL também não, então este é o único caminho que
     * serve os quatro dialetos. Uma ida a mais no Postgres e no SQL Server é
     * o preço, e a medição da ADR mostrou os candidatos separados por menos
     * que um round trip — otimizar por dialeto antes de um número que
     * justifique seria generalização prematura.
     *
     * <p>A releitura é estável porque o row lock do {@code UPDATE} é mantido
     * até o commit: nenhuma outra transação consegue comitar sobre a linha
     * entre os dois statements, então o que volta é exatamente o que ESTA
     * transação escreveu. Isso depende dos dois viverem na MESMA transação —
     * que é o que a conclusão garante (ADR-0003, cláusula 4). Fora de uma,
     * duas conclusões concorrentes podem ler o mesmo saldo final e as duas se
     * acharem a fechadora do lote.
     */
    private BatchCounters incrementAndRead(String batchId, String incrementSql) {
        Objects.requireNonNull(batchId, "batchId");
        MapSqlParameterSource params = new MapSqlParameterSource("id", batchId);
        if (jdbcTemplate.update(incrementSql, params) == 0) {
            // A FK de mohs_executions.batch_id torna isto impossível por construção;
            // se acontecer, contar em silêncio perderia a conclusão do lote para sempre.
            throw new IllegalStateException("no batch '" + batchId + "' to count a member into");
        }
        return find(batchId).orElseThrow(() ->
                new IllegalStateException("batch '" + batchId + "' vanished between increment and read"));
    }

    private static BatchCounters mapRow(ResultSet rs) throws SQLException {
        return new BatchCounters(rs.getString("id"), rs.getInt("total"), rs.getInt("succeeded"), rs.getInt("failed"));
    }
}
