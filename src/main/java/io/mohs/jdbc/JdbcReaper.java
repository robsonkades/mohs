package io.mohs.jdbc;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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

/**
 * {@link Reaper} sobre {@code mohs_executions} (ADR-0012/0025/0026).
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
        List<Candidate> candidates = selectExpiredCandidates(now);
        if (candidates.isEmpty()) {
            return List.of();
        }

        Map<String, Execution> executionsById = executionStore.findByIds(candidates.stream().map(c -> ExecutionId.of(c.id())).toList())
                .stream().collect(Collectors.toMap(e -> e.id().value(), Function.identity()));

        Map<String, Attempt> synthesizedAttemptById = new HashMap<>();
        List<ExecutionStore.CompletionRequest> requests = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            Execution execution = executionsById.get(candidate.id());
            if (execution == null) {
                throw new IllegalStateException("candidate " + candidate.id() + " vanished during reclaim — should be unreachable");
            }
            Attempt attempt = new Attempt(
                    execution.attempts().size() + 1,
                    execution.firedAt() != null ? execution.firedAt() : now,
                    now,
                    ExecutionState.FAILED,
                    LEASE_EXPIRED_ERROR);
            synthesizedAttemptById.put(candidate.id(), attempt);
            requests.add(new ExecutionStore.CompletionRequest(
                    ExecutionId.of(candidate.id()), JobKey.of(candidate.jobKey()), attempt, ExecutionState.FAILED));
        }

        Set<ExecutionId> completedIds = executionStore.completeAll(requests, jobStore);
        return buildReclaimedList(candidates, executionsById, synthesizedAttemptById, completedIds);
    }

    /** Reconstrói o resultado localmente (sem consulta extra) — {@link ExecutionStore#completeAll} só confirma quais ids venceram o CAS. */
    private static List<Execution> buildReclaimedList(List<Candidate> candidates, Map<String, Execution> executionsById,
            Map<String, Attempt> synthesizedAttemptById, Set<ExecutionId> completedIds) {
        List<Execution> reclaimed = new ArrayList<>(completedIds.size());
        for (Candidate candidate : candidates) {
            ExecutionId id = ExecutionId.of(candidate.id());
            if (!completedIds.contains(id)) {
                continue;
            }
            Execution execution = executionsById.get(candidate.id());
            Attempt attempt = synthesizedAttemptById.get(candidate.id());
            List<Attempt> attempts = new ArrayList<>(execution.attempts());
            attempts.add(attempt);
            reclaimed.add(new Execution(id, JobKey.of(candidate.jobKey()), ExecutionState.FAILED, execution.scheduledAt(), execution.firedAt(), attempts, execution.actor()));
        }
        return reclaimed;
    }

    private List<Candidate> selectExpiredCandidates(Instant now) {
        return jdbcTemplate.query(
                "SELECT id, job_key FROM mohs_executions WHERE state = 'RUNNING' AND lease_expires_at < :now",
                new MapSqlParameterSource("now", JdbcTimestamps.toUtcTimestamp(now)),
                (rs, _) -> new Candidate(rs.getString("id"), rs.getString("job_key")));
    }

    private record Candidate(String id, String jobKey) {
    }
}
