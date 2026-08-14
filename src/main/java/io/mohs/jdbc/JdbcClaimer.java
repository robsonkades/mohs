package io.mohs.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.Claimer;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.JobStore;
import io.mohs.jdbc.dialect.Candidate;
import io.mohs.jdbc.dialect.JdbcDialect;
import io.mohs.jdbc.dialect.SqlServerJdbcDialect;

/**
 * {@link Claimer} sobre {@code mohs_executions}/{@code mohs_job_definitions}
 * (ADR-0016, ADR-0018). Cruza duas tabelas numa única transação própria —
 * {@link TransactionTemplate} sobre um {@link DataSourceTransactionManager}
 * dedicado, já que claim é disparado pelo loop de poll do motor, não por um
 * chamador que já tem transação ativa (diferente do "insert do terminal" de
 * {@link JdbcExecutionStore}, que participa por conveniência — ADR-0003
 * cláusula 4). {@code jobStore} e {@code executionStore} precisam apontar
 * pro mesmo {@code DataSource} passado aqui — é assim que eles enxergam a
 * mesma transação (participação por {@code DataSource}, não por instância
 * de template).
 *
 * <p>A ADR-0018 substitui a ADR-0017: o lock otimista da consulta de
 * candidatos (ver {@link JdbcDialect}) continua existindo, mas só como
 * otimização (reduz quantos candidatos perdedores fazem trabalho à toa)
 * — a garantia de corretude real vem inteiramente de uma cadeia de
 * {@code UPDATE} guardados ({@link JobStore#tryIncrementRunningExecutions},
 * {@link #tryTransitionToRunning}), cada um atômico por construção
 * porque é uma escrita simples, não uma leitura com lock especializado.
 * Motivo: {@code SELECT ... FOR UPDATE SKIP LOCKED} do H2 2.4.240 tem
 * uma corrida real sob contenção genuína — confirmado empiricamente
 * (duas conexões JDBC cruas, sem nenhum código Spring, disputando a
 * MESMA linha via barrier: ~33% das vezes as duas obtêm o lock). Travar
 * só {@code e} (não mais {@code j}) também resolve o custo que jobs com
 * {@code allowConcurrentExecutions = true} pagavam à toa na ADR-0017. A
 * ADR-0020 generaliza o mutex de "um dono" pra um contador com teto
 * ({@link JobStore#tryIncrementRunningExecutions}/{@link
 * JobStore#decrementRunningExecutions}); a ADR-0021 remove a admissão de
 * queue que cruzava uma terceira tabela aqui. A ADR-0023 extrai a
 * consulta de candidatos (o único ponto sensível a dialeto: {@code
 * LIMIT}/{@code TOP}, {@code SKIP LOCKED}/hint de tabela) pra {@link
 * JdbcDialect} — a garantia de corretude acima vale igual em qualquer
 * dialeto, já que nunca dependeu do lock.
 */
public final class JdbcClaimer implements Claimer {

    /**
     * SQL Server (sem {@code SKIP LOCKED} de verdade — {@link
     * SqlServerJdbcDialect} usa hints pessimistas) escolhe uma
     * transação "vítima" e a mata sob deadlock genuíno entre dois nós
     * disputando o mesmo mutex de job — comportamento normal e
     * documentado do motor, não um bug daqui (Postgres/MySQL/H2 não
     * precisam disto na prática, mas o retry é inofensivo pra eles —
     * nunca dispara). Reexecuta o claim inteiro do zero (SELECT de novo
     * — a transação abortada não deixa candidatos válidos pra reusar).
     */
    private static final int MAX_DEADLOCK_RETRIES = 3;

    /**
     * Duas transações que deadlockam e retentam no mesmo instante têm alta
     * chance de deadlockar de novo — dezenas de ms de espera é a
     * recomendação da própria Microsoft pro deadlock victim de SQL Server
     * (DBTUNE-13). {@code Thread.sleep} aqui é espera deliberada de
     * operação, não sincronização de teste — desmonta o carrier.
     */
    private static final Duration DEADLOCK_RETRY_BASE_DELAY = Duration.ofMillis(20);
    private static final long DEADLOCK_RETRY_JITTER_MILLIS = 30;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final JdbcDialect dialect;
    private final Clock clock;
    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    private final Duration leaseTtl;

    public JdbcClaimer(DataSource dataSource, JdbcDialect dialect, Clock clock, ExecutionStore executionStore, JobStore jobStore, Duration leaseTtl) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // DBTUNE-4: sem isto, o isolamento herdava o default do banco —
        // REPEATABLE READ no MySQL/InnoDB, diferente dos outros 3 dialetos,
        // sem nenhuma linha de código declarando a divergência. A ADR-0018
        // raciocina o CAS guardado em termos de "última escrita vence", que
        // é semântica READ COMMITTED.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.leaseTtl = Objects.requireNonNull(leaseTtl, "leaseTtl");
    }

    @Override
    public List<Execution> claim(String nodeId, int batchSize) {
        Objects.requireNonNull(nodeId, "nodeId");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }

        List<String> claimedIds = claimIdsWithDeadlockRetry(nodeId, batchSize);
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

    private List<String> claimIdsWithDeadlockRetry(String nodeId, int batchSize) {
        PessimisticLockingFailureException lastFailure = null;
        for (int attempt = 0; attempt < MAX_DEADLOCK_RETRIES; attempt++) {
            try {
                // TransactionCallback aqui nunca devolve null (claimWithinTransaction
                // sempre retorna uma lista) — requireNonNull só documenta esse invariante
                // pro @NullMarked, já que o contrato de execute() em si é @Nullable (JAVA-8).
                return Objects.requireNonNull(transactionTemplate.execute(status -> claimWithinTransaction(nodeId, batchSize)));
            } catch (PessimisticLockingFailureException e) {
                lastFailure = e;
                if (attempt < MAX_DEADLOCK_RETRIES - 1) {
                    backoffBeforeDeadlockRetry();
                }
            }
        }
        throw lastFailure;
    }

    private static void backoffBeforeDeadlockRetry() {
        try {
            Thread.sleep(DEADLOCK_RETRY_BASE_DELAY.plusMillis(ThreadLocalRandom.current().nextLong(DEADLOCK_RETRY_JITTER_MILLIS)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private List<String> claimWithinTransaction(String nodeId, int batchSize) {
        Instant now = clock.instant();
        Instant leaseExpiresAt = now.plus(leaseTtl);
        List<Candidate> candidates = selectCandidates(now, batchSize);

        List<String> claimedIds = new ArrayList<>(candidates.size());
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
     * falhar — nunca deixa mutex de job preso por um candidato que no fim
     * não foi reivindicado.
     */
    private boolean tryClaimCandidate(Candidate candidate, String nodeId, Instant leaseExpiresAt) {
        boolean acquiredJobSlot = false;
        if (!candidate.allowConcurrentExecutions()) {
            if (!jobStore.tryIncrementRunningExecutions(JobKey.of(candidate.jobKey()))) {
                return false;
            }
            acquiredJobSlot = true;
        }

        boolean claimed = tryTransitionToRunning(candidate.id(), nodeId, leaseExpiresAt);
        if (!claimed && acquiredJobSlot) {
            jobStore.decrementRunningExecutions(JobKey.of(candidate.jobKey()));
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
                .addValue("leaseExpiresAt", JdbcTimestamps.toUtcTimestamp(leaseExpiresAt))
                .addValue("nodeId", nodeId)
                .addValue("id", executionId));
        return updated == 1;
    }

    private List<Candidate> selectCandidates(Instant now, int batchSize) {
        return dialect.selectCandidates(jdbcTemplate, now, batchSize);
    }
}
