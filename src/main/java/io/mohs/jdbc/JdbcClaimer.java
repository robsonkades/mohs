package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.engine.Claimer;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.QueueStore;

/**
 * {@link Claimer} sobre {@code mohs_executions}/{@code mohs_job_definitions}/
 * {@code mohs_job_queues} (ADR-0016, ADR-0017). Cruza três tabelas numa
 * única transação própria — {@link TransactionTemplate} sobre um
 * {@link DataSourceTransactionManager} dedicado, já que claim é disparado
 * pelo loop de poll do motor, não por um chamador que já tem transação
 * ativa (diferente do "insert do terminal" de {@link JdbcExecutionStore},
 * que participa por conveniência — ADR-0003 cláusula 4). {@code queueStore}
 * e {@code executionStore} precisam apontar pro mesmo {@code DataSource}
 * passado aqui — é assim que eles enxergam a mesma transação (participação
 * por {@code DataSource}, não por instância de template).
 */
public final class JdbcClaimer implements Claimer {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final ExecutionStore executionStore;
    private final QueueStore queueStore;
    private final Duration leaseTtl;

    public JdbcClaimer(DataSource dataSource, Clock clock, ExecutionStore executionStore, QueueStore queueStore, Duration leaseTtl) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.queueStore = Objects.requireNonNull(queueStore, "queueStore");
        this.leaseTtl = Objects.requireNonNull(leaseTtl, "leaseTtl");
    }

    @Override
    public List<Execution> claim(String nodeId, int batchSize) {
        Objects.requireNonNull(nodeId, "nodeId");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }

        List<String> claimedIds = transactionTemplate.execute(status -> claimWithinTransaction(nodeId, batchSize));

        return claimedIds.stream()
                .map(id -> executionStore.find(ExecutionId.of(id)).orElseThrow())
                .toList();
    }

    /**
     * Seleciona candidatos travando execução e definição juntas (mutex por
     * job entre transações, ADR-0017), aplica admissão de queue candidato a
     * candidato via {@link QueueStore#tryIncrementRunning} e grava o
     * {@code UPDATE} único pra {@code RUNNING} (ADR-0016) só nos que
     * sobraram.
     *
     * <p>{@code claimedJobKeys} fecha a outra metade do mutex: o
     * {@code NOT EXISTS} da consulta só enxerga execuções já {@code RUNNING}
     * de transações anteriores — dois siblings {@code ENQUEUED} do mesmo job
     * aparecem como candidatos igualmente válidos nesta mesma consulta
     * (nenhum dos dois está {@code RUNNING} ainda). Sem essa marca em
     * memória, esta própria chamada poderia reivindicar os dois no mesmo
     * lote e violar a exclusão mútua sozinha, sem precisar de um segundo nó.
     */
    private List<String> claimWithinTransaction(String nodeId, int batchSize) {
        Instant now = clock.instant();
        List<Candidate> candidates = selectCandidates(now, batchSize);

        List<String> claimedIds = new ArrayList<>();
        Set<String> claimedJobKeys = new HashSet<>();
        for (Candidate candidate : candidates) {
            if (!candidate.allowConcurrentExecutions() && claimedJobKeys.contains(candidate.jobKey())) {
                continue;
            }
            boolean admitted = candidate.queueName() == null || queueStore.tryIncrementRunning(candidate.queueName());
            if (admitted) {
                claimedIds.add(candidate.id());
                if (!candidate.allowConcurrentExecutions()) {
                    claimedJobKeys.add(candidate.jobKey());
                }
            }
        }

        if (!claimedIds.isEmpty()) {
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("leaseExpiresAt", Timestamp.from(now.plus(leaseTtl)))
                    .addValue("nodeId", nodeId)
                    .addValue("ids", claimedIds);
            jdbcTemplate.update("""
                    UPDATE mohs_executions
                    SET state = 'RUNNING', lease_expires_at = :leaseExpiresAt, node_id = :nodeId
                    WHERE id IN (:ids)
                    """, params);
        }
        return claimedIds;
    }

    private List<Candidate> selectCandidates(Instant now, int batchSize) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("now", Timestamp.from(now))
                .addValue("batchSize", batchSize);
        return jdbcTemplate.query("""
                SELECT e.id AS id, e.job_key AS job_key, j.queue_name AS queue_name,
                       j.allow_concurrent_executions AS allow_concurrent_executions
                FROM mohs_executions e
                JOIN mohs_job_definitions j ON j.job_key = e.job_key
                WHERE e.state = 'ENQUEUED'
                  AND e.scheduled_at <= :now
                  AND (j.allow_concurrent_executions = TRUE
                       OR NOT EXISTS (
                           SELECT 1 FROM mohs_executions r
                           WHERE r.job_key = e.job_key AND r.state = 'RUNNING'
                       ))
                ORDER BY
                  CASE e.priority
                    WHEN 'CRITICAL' THEN 5 WHEN 'HIGH' THEN 4 WHEN 'NORMAL' THEN 3
                    WHEN 'LOW' THEN 2 WHEN 'BACKGROUND' THEN 1 ELSE 3
                  END DESC,
                  e.scheduled_at ASC
                LIMIT :batchSize
                FOR UPDATE OF e, j SKIP LOCKED
                """, params, JdbcClaimer::mapCandidate);
    }

    private static Candidate mapCandidate(ResultSet rs, int rowNum) throws SQLException {
        return new Candidate(rs.getString("id"), rs.getString("job_key"), rs.getString("queue_name"),
                rs.getBoolean("allow_concurrent_executions"));
    }

    private record Candidate(String id, String jobKey, @Nullable String queueName, boolean allowConcurrentExecutions) {
    }
}
