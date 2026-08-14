package io.mohs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Claimer;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.JobStore;
import io.mohs.engine.QueueStore;

/**
 * {@link Claimer} sobre {@code mohs_executions}/{@code mohs_job_definitions}/
 * {@code mohs_job_queues} (ADR-0016, ADR-0018). Cruza três tabelas numa
 * única transação própria — {@link TransactionTemplate} sobre um
 * {@link DataSourceTransactionManager} dedicado, já que claim é disparado
 * pelo loop de poll do motor, não por um chamador que já tem transação
 * ativa (diferente do "insert do terminal" de {@link JdbcExecutionStore},
 * que participa por conveniência — ADR-0003 cláusula 4). {@code queueStore}
 * e {@code executionStore} precisam apontar pro mesmo {@code DataSource}
 * passado aqui — é assim que eles enxergam a mesma transação (participação
 * por {@code DataSource}, não por instância de template).
 *
 * <p>A ADR-0018 substitui a ADR-0017: o {@code SELECT ... FOR UPDATE OF e
 * SKIP LOCKED} continua existindo, mas só como otimização (reduz quantos
 * candidatos perdedores fazem trabalho à toa) — a garantia de corretude
 * real vem inteiramente de uma cadeia de {@code UPDATE} guardados
 * ({@link JobStore#tryIncrementRunningExecutions}, {@link
 * QueueStore#tryIncrementRunning}, {@link #tryTransitionToRunning}), cada
 * um atômico por construção porque é uma escrita simples, não uma leitura
 * com lock especializado. Motivo: {@code SELECT ... FOR UPDATE SKIP
 * LOCKED} do H2 2.4.240 tem uma corrida real sob contenção genuína —
 * confirmado empiricamente (duas conexões JDBC cruas, sem nenhum código
 * Spring, disputando a MESMA linha via barrier: ~33% das vezes as duas
 * obtêm o lock). Travar só {@code e} (não mais {@code j}) também resolve o
 * custo que jobs com {@code allowConcurrentExecutions = true} pagavam à
 * toa na ADR-0017. A ADR-0020 generaliza o mutex de "um dono" pra um
 * contador com teto ({@link JobStore#tryIncrementRunningExecutions}/
 * {@link JobStore#decrementRunningExecutions}) — mesmo mecanismo, mesma
 * disciplina de CAS guardado, agora reaproveitando {@link JobStore} em vez
 * de SQL cru local (mesma forma que {@link QueueStore} já tinha).
 */
public final class JdbcClaimer implements Claimer {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    private final QueueStore queueStore;
    private final Duration leaseTtl;

    public JdbcClaimer(DataSource dataSource, Clock clock, ExecutionStore executionStore, JobStore jobStore, QueueStore queueStore, Duration leaseTtl) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
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
        if (claimedIds.isEmpty()) {
            return List.of();
        }

        // uma consulta em lote, não uma por id (N+1) — a ordem de retorno de
        // findByIds não é garantida, então reordena pelos ids já ordenados
        // por prioridade/scheduled_at que claimWithinTransaction produziu.
        Map<String, Execution> byId = executionStore.findByIds(claimedIds.stream().map(ExecutionId::of).toList())
                .stream()
                .collect(Collectors.toMap(e -> e.id().value(), Function.identity()));
        return claimedIds.stream()
                .map(id -> {
                    Execution execution = byId.get(id);
                    if (execution == null) {
                        throw new IllegalStateException("claimed execution " + id + " not found after claim — should be unreachable");
                    }
                    return execution;
                })
                .toList();
    }

    private List<String> claimWithinTransaction(String nodeId, int batchSize) {
        Instant now = clock.instant();
        Instant leaseExpiresAt = now.plus(leaseTtl);
        List<Candidate> candidates = selectCandidates(now, batchSize);

        List<String> claimedIds = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (tryClaimCandidate(candidate, nodeId, leaseExpiresAt)) {
                claimedIds.add(candidate.id());
            }
        }
        return claimedIds;
    }

    /**
     * Reivindica um candidato através da cadeia completa de guardas
     * atômicas, desfazendo qualquer reserva parcial se um passo posterior
     * falhar — nunca deixa mutex de job ou vaga de queue presos por um
     * candidato que no fim não foi reivindicado.
     */
    private boolean tryClaimCandidate(Candidate candidate, String nodeId, Instant leaseExpiresAt) {
        boolean acquiredJobSlot = false;
        if (!candidate.allowConcurrentExecutions()) {
            if (!jobStore.tryIncrementRunningExecutions(JobKey.of(candidate.jobKey()))) {
                return false;
            }
            acquiredJobSlot = true;
        }

        boolean acquiredQueueSlot = false;
        if (candidate.queueName() != null) {
            if (!queueStore.tryIncrementRunning(candidate.queueName())) {
                if (acquiredJobSlot) {
                    jobStore.decrementRunningExecutions(JobKey.of(candidate.jobKey()));
                }
                return false;
            }
            acquiredQueueSlot = true;
        }

        boolean claimed = tryTransitionToRunning(candidate.id(), nodeId, leaseExpiresAt);
        if (!claimed) {
            if (acquiredJobSlot) {
                jobStore.decrementRunningExecutions(JobKey.of(candidate.jobKey()));
            }
            if (acquiredQueueSlot) {
                queueStore.decrementRunning(candidate.queueName());
            }
        }
        return claimed;
    }

    /** CAS final pra RUNNING — a garantia real contra double-claim, independente do lock do SELECT. */
    private boolean tryTransitionToRunning(String executionId, String nodeId, Instant leaseExpiresAt) {
        int updated = jdbcTemplate.update("""
                UPDATE mohs_executions
                SET state = 'RUNNING', lease_expires_at = :leaseExpiresAt, node_id = :nodeId
                WHERE id = :id AND state = 'ENQUEUED'
                """, new MapSqlParameterSource()
                .addValue("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
                .addValue("nodeId", nodeId)
                .addValue("id", executionId));
        return updated == 1;
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
                  AND (j.allow_concurrent_executions = TRUE OR j.running_execution_count < j.max_concurrent_executions)
                ORDER BY
                  CASE e.priority
                    WHEN 'CRITICAL' THEN 5 WHEN 'HIGH' THEN 4 WHEN 'NORMAL' THEN 3
                    WHEN 'LOW' THEN 2 WHEN 'BACKGROUND' THEN 1 ELSE 3
                  END DESC,
                  e.scheduled_at ASC
                LIMIT :batchSize
                FOR UPDATE OF e SKIP LOCKED
                """, params, JdbcClaimer::mapCandidate);
    }

    private static Candidate mapCandidate(ResultSet rs, int rowNum) throws SQLException {
        return new Candidate(rs.getString("id"), rs.getString("job_key"), rs.getString("queue_name"),
                rs.getBoolean("allow_concurrent_executions"));
    }

    private record Candidate(String id, String jobKey, @Nullable String queueName, boolean allowConcurrentExecutions) {
    }
}
