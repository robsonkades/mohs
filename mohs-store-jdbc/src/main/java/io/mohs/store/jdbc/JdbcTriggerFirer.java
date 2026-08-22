package io.mohs.store.jdbc;

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
import io.mohs.engine.HistoryStore;
import io.mohs.engine.TriggerFirer;
import io.mohs.engine.WorkQueue;

/**
 * {@link TriggerFirer} sobre {@code mohs_job_definitions} + as mesas da
 * Phase 5 (ADR-0035 no split): o CAS de avanço do trigger e o nascimento
 * das ocorrências — história ({@code record}) e fila ({@code offer}) —
 * numa única transação, exatamente a unidade de enqueue do §7.5-1 com o
 * CAS como guarda de exclusão mútua cluster-wide. {@code historyStore}/
 * {@code workQueue} precisam apontar pro mesmo {@code DataSource} passado
 * aqui — é assim que participam da transação (mesmo padrão da era
 * anterior).
 *
 * <p>O CAS compara {@code next_fire_at} com o valor que
 * {@code findDueRecurring} LEU da própria coluna — nunca um instante
 * calculado na JVM que não passou pelo banco (precisão temporal não faz
 * round-trip garantido entre JVM e os 4 dialetos; valor lido e
 * re-serializado por {@link JdbcTimestamps} compara igual por construção).
 */
public final class JdbcTriggerFirer implements TriggerFirer {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final HistoryStore historyStore;
    private final WorkQueue workQueue;

    public JdbcTriggerFirer(DataSource dataSource, HistoryStore historyStore, WorkQueue workQueue) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // mesmo raciocínio da DBTUNE-4 em JdbcWorkQueue: CAS guardado assume
        // "última escrita vence" (READ COMMITTED), não herda o default do banco.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.historyStore = Objects.requireNonNull(historyStore, "historyStore");
        this.workQueue = Objects.requireNonNull(workQueue, "workQueue");
    }

    @Override
    public boolean fire(JobKey key, Instant observedNextFireAt, @Nullable Instant newNextFireAt,
            List<Execution> occurrences, Object payload, Instant now) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(observedNextFireAt, "observedNextFireAt");
        Objects.requireNonNull(occurrences, "occurrences");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(now, "now");
        return Boolean.TRUE.equals(transactionTemplate.execute(_ -> {
            // retired no predicado: um Mohs.remove entre a varredura e este CAS já
            // cancelou o que estava na fila — inserir ocorrências DEPOIS dessa
            // varredura as deixaria zumbis até uma eventual ressurreição.
            int advanced = jdbcTemplate.update("""
                    UPDATE mohs_job_definitions SET next_fire_at = :newNextFireAt
                    WHERE job_key = :jobKey AND next_fire_at = :observedNextFireAt AND retired = :retired
                    """,
                    new MapSqlParameterSource()
                            .addValue("jobKey", key.value())
                            .addValue("observedNextFireAt", JdbcTimestamps.toUtcLocalDateTime(observedNextFireAt))
                            .addValue("newNextFireAt", newNextFireAt == null ? null : JdbcTimestamps.toUtcLocalDateTime(newNextFireAt))
                            .addValue("retired", false));
            if (advanced == 0) {
                return false;
            }
            // createdAt = now (o instante do disparo, a chave de partição —
            // scheduledAt pode estar no passado num FIRE_ALL de misfire e
            // apontaria uma partição que a retenção pode já ter dropado);
            // visible_at = scheduledAt: a ocorrência entra na fila já devida
            historyStore.record(occurrences.stream()
                    .map(occurrence -> new HistoryStore.NewExecution(occurrence.id(), occurrence.jobKey(), 0,
                            occurrence.priority().value(), occurrence.scheduledAt(), now, occurrence.actor(),
                            occurrence.batchId(), occurrence.idempotencyKey(), payload))
                    .toList());
            workQueue.offer(occurrences.stream()
                    .map(occurrence -> new WorkQueue.ReadyEntry(occurrence.id(), occurrence.jobKey(), 0,
                            occurrence.priority().value(), 1, occurrence.scheduledAt()))
                    .toList());
            return true;
        }));
    }
}
