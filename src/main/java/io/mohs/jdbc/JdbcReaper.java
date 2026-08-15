package io.mohs.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;
import io.mohs.engine.ExecutionStore;
import io.mohs.engine.JobStore;
import io.mohs.engine.Reaper;
import io.mohs.engine.RetrySchedule;

/**
 * {@link Reaper} sobre {@code mohs_executions} (ADR-0012/0025/0033).
 * Mesma forma de transação própria de {@link JdbcClaimer} — quem chama
 * {@link #reclaimExpired} não tem transação ativa (disparado pelo ciclo
 * de poll do motor, não por um chamador com contexto transacional já
 * aberto). {@code executionStore}/{@code jobStore} precisam apontar pro
 * mesmo {@code DataSource} passado aqui — é assim que participam da
 * mesma transação (mesmo padrão de {@link JdbcClaimer}).
 *
 * <p>DBTUNE-14: lê ({@link ExecutionStore#findByIds}) e escreve ({@link
 * ExecutionStore#completeAll}) em lote, não candidato a candidato — medido
 * em {@code LivenessLoadHarness}: o índice {@code idx_mohs_executions_reaper}
 * (DBTUNE-10) deixou a seleção de candidatos ~42x mais rápida no Postgres,
 * mas o throughput fim-a-fim quase não mudeu, porque o gargalo real era o
 * round-trip por candidato daqui, não a query de seleção.
 */
public final class JdbcReaper implements Reaper {

    private static final String LEASE_EXPIRED_ERROR = "lease expired — node presumed dead";

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final ExecutionStore executionStore;
    private final JobStore jobStore;

    public JdbcReaper(DataSource dataSource, Clock clock, ExecutionStore executionStore, JobStore jobStore) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // mesmo raciocínio da DBTUNE-4 em JdbcClaimer: CAS guardado assume
        // "última escrita vence" (READ COMMITTED), não herda o default do banco.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
    }

    @Override
    public List<Execution> reclaimExpired() {
        // TransactionCallback aqui nunca devolve null (reclaimWithinTransaction
        // sempre retorna uma lista) — requireNonNull só documenta esse invariante
        // pro @NullMarked, já que o contrato de execute() em si é @Nullable (JAVA-8).
        return Objects.requireNonNull(transactionTemplate.execute(status -> reclaimWithinTransaction()));
    }

    private List<Execution> reclaimWithinTransaction() {
        Instant now = clock.instant();
        List<ExpiredCandidate> candidates = selectExpiredCandidates(now);
        if (candidates.isEmpty()) {
            return List.of();
        }

        Map<String, Execution> executionsById = executionStore.findByIds(candidates.stream().map(c -> ExecutionId.of(c.id())).toList())
                .stream().collect(Collectors.toMap(e -> e.id().value(), Function.identity()));

        Map<String, ExecutionStore.CompletionRequest> requestById = LinkedHashMap.newLinkedHashMap(candidates.size());
        for (ExpiredCandidate candidate : candidates) {
            Execution execution = executionsById.get(candidate.id());
            if (execution == null) {
                throw new IllegalStateException("candidate " + candidate.id() + " vanished during reclaim — should be unreachable");
            }
            requestById.put(candidate.id(), reclaimRequest(candidate, execution, now));
        }

        Set<ExecutionId> completedIds = executionStore.completeAll(List.copyOf(requestById.values()), jobStore);
        return buildReclaimedList(candidates, executionsById, requestById, completedIds);
    }

    /**
     * ADR-0033: o attempt sintético do reclaim consome orçamento como
     * qualquer outro — mesma decisão ({@link RetrySchedule}) do Dispatcher:
     * orçamento restante reagenda com backoff, esgotado é FAILED terminal.
     * Job aposentado ({@code retired}) nunca reagenda: {@code RETRY_SCHEDULED}
     * de job removido ficaria preso pra sempre (claim filtra retired, o
     * cancel do remove já passou). Todo request leva a lease observada como
     * fence anti-ABA: se um re-claim concorrente já deu lease nova à
     * execução, o CAS do reclaim perde — nunca mata a encarnação nova
     * saudável (DDIA cap. 8: estado reentrante exige fencing token).
     */
    private static ExecutionStore.CompletionRequest reclaimRequest(ExpiredCandidate candidate, Execution execution, Instant now) {
        int attemptNumber = execution.attempts().size() + 1;
        Attempt attempt = new Attempt(
                attemptNumber,
                Objects.requireNonNullElse(execution.firedAt(), now),
                now,
                ExecutionState.FAILED,
                LEASE_EXPIRED_ERROR);
        ExecutionId id = ExecutionId.of(candidate.id());
        JobKey jobKey = JobKey.of(candidate.jobKey());
        if (candidate.retired()) {
            return new ExecutionStore.CompletionRequest(id, jobKey, attempt, ExecutionState.FAILED, null, candidate.leaseExpiresAt());
        }
        return RetrySchedule.nextRetryAt(attemptNumber, candidate.retries(), now)
                .map(retryAt -> new ExecutionStore.CompletionRequest(id, jobKey, attempt, ExecutionState.RETRY_SCHEDULED, retryAt, candidate.leaseExpiresAt()))
                .orElseGet(() -> new ExecutionStore.CompletionRequest(id, jobKey, attempt, ExecutionState.FAILED, null, candidate.leaseExpiresAt()));
    }

    /** Reconstrói o resultado localmente (sem consulta extra) — {@link ExecutionStore#completeAll} só confirma quais ids venceram o CAS. */
    private static List<Execution> buildReclaimedList(List<ExpiredCandidate> candidates, Map<String, Execution> executionsById,
            Map<String, ExecutionStore.CompletionRequest> requestById, Set<ExecutionId> completedIds) {
        List<Execution> reclaimed = new ArrayList<>(completedIds.size());
        for (ExpiredCandidate candidate : candidates) {
            ExecutionId id = ExecutionId.of(candidate.id());
            if (!completedIds.contains(id)) {
                continue;
            }
            Execution execution = executionsById.get(candidate.id());
            ExecutionStore.CompletionRequest request = requestById.get(candidate.id());
            List<Attempt> attempts = new ArrayList<>(execution.attempts());
            attempts.add(request.attempt());
            Instant scheduledAt = Objects.requireNonNullElse(request.retryAt(), execution.scheduledAt());
            reclaimed.add(new Execution(id, JobKey.of(candidate.jobKey()), request.newState(), scheduledAt, execution.firedAt(), attempts,
                    execution.actor(), execution.priority(), execution.idempotencyKey()));
        }
        return reclaimed;
    }

    /**
     * {@code j.retries}/{@code j.retired} entram no mesmo SELECT — a decisão
     * de orçamento não paga uma consulta por candidato. {@code lease_expires_at}
     * sai junto porque é o fence do CAS de conclusão (ver {@link #reclaimRequest}).
     */
    private List<ExpiredCandidate> selectExpiredCandidates(Instant now) {
        // ORDER BY e.id: reapers concorrentes acordam juntos numa morte de nó e
        // adquirem os mesmos row locks — ordem global determinística (JCIP cap.
        // 10, lock ordering) evita deadlock entre eles; a cadeia até o
        // batchUpdate preserva encounter order.
        return jdbcTemplate.query("""
                SELECT e.id AS id, e.job_key AS job_key, e.lease_expires_at AS lease_expires_at,
                       j.retries AS retries, j.retired AS retired
                FROM mohs_executions e
                JOIN mohs_job_definitions j ON j.job_key = e.job_key
                WHERE e.state = 'RUNNING' AND e.lease_expires_at < :now
                ORDER BY e.id
                """,
                new MapSqlParameterSource("now", JdbcTimestamps.toUtcTimestamp(now)),
                (rs, _) -> new ExpiredCandidate(rs.getString("id"), rs.getString("job_key"),
                        JdbcTimestamps.fromUtcTimestamp(rs.getTimestamp("lease_expires_at")),
                        rs.getInt("retries"), rs.getBoolean("retired")));
    }

    private record ExpiredCandidate(String id, String jobKey, Instant leaseExpiresAt, int retries, boolean retired) {
    }
}
