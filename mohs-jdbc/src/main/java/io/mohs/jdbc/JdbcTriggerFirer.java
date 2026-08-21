package io.mohs.jdbc;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.Execution;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.TriggerFirer;

/**
 * {@link TriggerFirer} sobre {@code mohs_job_definitions}/{@code
 * mohs_executions} (ADR-0035). Mesma forma de transação própria de
 * {@link JdbcClaimer}/{@link JdbcReaper}: quem chama (o tick do motor)
 * não tem transação ativa; {@code executionStore} precisa apontar pro
 * mesmo {@code DataSource} passado aqui — é assim que {@code insert}
 * participa da transação do CAS (cláusula 4 da ADR-0003).
 *
 * <p>O CAS compara {@code next_fire_at} com o valor que
 * {@code findDueRecurring} LEU da própria coluna — nunca um instante
 * calculado na JVM que não passou pelo banco: precisão temporal não faz
 * round-trip garantido entre JVM e os 4 dialetos (nanos do {@code
 * Instant} vs micros da coluna — mesma lição do
 * {@code confirmRenewalsBySelect}), mas valor lido e re-serializado por
 * {@link JdbcTimestamps} compara igual por construção.
 */
public final class JdbcTriggerFirer implements TriggerFirer {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final ExecutionStore executionStore;

    public JdbcTriggerFirer(DataSource dataSource, ExecutionStore executionStore) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // mesmo raciocínio da DBTUNE-4 em JdbcClaimer: CAS guardado assume
        // "última escrita vence" (READ COMMITTED), não herda o default do banco.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
    }

    @Override
    public boolean fire(JobKey key, Instant observedNextFireAt, @Nullable Instant newNextFireAt,
            List<Execution> occurrences, Object payload) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(observedNextFireAt, "observedNextFireAt");
        Objects.requireNonNull(occurrences, "occurrences");
        Objects.requireNonNull(payload, "payload");
        return Boolean.TRUE.equals(transactionTemplate.execute(_ -> {
            // retired no predicado: um Mohs.remove entre a varredura e este CAS já
            // cancelou as ENQUEUED existentes — inserir ocorrências DEPOIS dessa
            // varredura as deixaria zumbis até uma eventual ressurreição.
            int advanced = jdbcTemplate.update("""
                    UPDATE mohs_job_definitions SET next_fire_at = :newNextFireAt
                    WHERE job_key = :jobKey AND next_fire_at = :observedNextFireAt AND retired = :retired
                    """,
                    new MapSqlParameterSource()
                            .addValue("jobKey", key.value())
                            .addValue("observedNextFireAt", JdbcTimestamps.toUtcTimestamp(observedNextFireAt))
                            .addValue("newNextFireAt", newNextFireAt == null ? null : JdbcTimestamps.toUtcTimestamp(newNextFireAt))
                            .addValue("retired", false));
            if (advanced == 0) {
                return false;
            }
            for (Execution occurrence : occurrences) {
                executionStore.insert(occurrence, payload);
            }
            return true;
        }));
    }
}
