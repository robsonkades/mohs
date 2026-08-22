package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.job.JobKey;
import io.mohs.engine.LeaseStore;
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * {@link LeaseStore} sobre {@code mohs_lease} (Phase 5, ADR-A). A
 * conclusão é a transação do §7.5-3: {@code DELETE} cercado por
 * {@code (node_id, epoch)} — deletar a lease É liberar a vaga (não existe
 * mais contador a decrementar, ADR-0025 morre por construção) —,
 * {@code INSERT} dos attempts confirmados, {@code UPDATE} terminal
 * advisory da história (podado por partição via igualdade de
 * {@code created_at}) e, pros resultados não-terminais, o renascimento na
 * fila na MESMA transação (ver Javadoc de
 * {@link LeaseStore.CompletionResult#retry}).
 */
public final class JdbcLeaseStore implements LeaseStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final JdbcDialect dialect;

    public JdbcLeaseStore(DataSource dataSource, JdbcDialect dialect) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // §7.5: conclusão é transação PRÓPRIA, sempre — ver o comentário de
        // REQUIRES_NEW em JdbcWorkQueue (mesma razão, mesmo hazard de
        // isolação herdada em silêncio)
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        // DBTUNE-4: fence assume "última escrita vence" em READ COMMITTED explícito
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    private static final String ATTEMPT_INSERT = """
            INSERT INTO mohs_attempt (execution_id, number, node_id, started_at, finished_at, outcome, error_type, error)
            VALUES (:executionId, :number, :nodeId, :startedAt, :finishedAt, :outcome, :errorType, :error)
            """;

    private static final String TERMINAL_UPDATE = """
            UPDATE mohs_execution SET state = :state, finished_at = :finishedAt
            WHERE execution_id = :executionId AND created_at = :createdAt
            """;

    @Override
    public Set<ExecutionId> complete(List<CompletionResult> results) {
        if (results.isEmpty()) {
            return Set.of();
        }
        // requireNonNull documenta o invariante pro @NullMarked (JAVA-8)
        return Objects.requireNonNull(transactionTemplate.execute(_ -> completeWithinTransaction(results)));
    }

    private Set<ExecutionId> completeWithinTransaction(List<CompletionResult> results) {
        List<CompletionResult> winners = deleteLeasesKeepingFenceWinners(results);
        Set<ExecutionId> owned = winners.stream()
                .map(CompletionResult::executionId)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (winners.isEmpty()) {
            return owned;
        }
        insertAttempts(winners);
        updateTerminalStates(winners);
        offerRetries(winners);
        return owned;
    }

    private List<CompletionResult> deleteLeasesKeepingFenceWinners(List<CompletionResult> results) {
        int[] deleted = jdbcTemplate.batchUpdate(JdbcSupport.FENCED_LEASE_DELETE, results.stream()
                .map(r -> JdbcSupport.fencedLeaseDeleteParams(r.executionId().value(), r.nodeId(), r.epoch()))
                .toArray(MapSqlParameterSource[]::new));
        List<CompletionResult> winners = new ArrayList<>(results.size());
        for (int i = 0; i < deleted.length; i++) {
            if (deleted[i] == Statement.SUCCESS_NO_INFO) {
                // sem contagem não há como saber quem o fence descartou — e um
                // fallback por presença é ambíguo (linha ausente pode ser vitória
                // NOSSA ou re-claim alheio já concluído). Nenhum driver dos 4
                // dialetos faz isso pra DELETE; se um passar a fazer, o caminho
                // por-linha é implementado de propósito, não improvisado.
                throw new IllegalStateException("driver returned SUCCESS_NO_INFO for the fenced lease delete batch — "
                        + "completion cannot tell fence winners apart; implement the per-row path for this driver");
            }
            if (deleted[i] == 1) {
                winners.add(results.get(i));
            }
        }
        return winners;
    }

    private void insertAttempts(List<CompletionResult> winners) {
        jdbcTemplate.batchUpdate(ATTEMPT_INSERT, winners.stream()
                .map(this::attemptParams)
                .toArray(MapSqlParameterSource[]::new));
    }

    private MapSqlParameterSource attemptParams(CompletionResult result) {
        return new MapSqlParameterSource()
                .addValue("executionId", result.executionId().value())
                .addValue("number", result.attemptNumber())
                .addValue("nodeId", result.nodeId())
                .addValue("startedAt", dialect.splitTimestamp(result.startedAt()))
                .addValue("finishedAt", dialect.splitTimestamp(result.finishedAt()))
                .addValue("outcome", result.outcome().name())
                .addValue("errorType", result.errorType())
                .addValue("error", result.error());
    }

    private void updateTerminalStates(List<CompletionResult> winners) {
        MapSqlParameterSource[] terminals = winners.stream()
                .filter(r -> r.terminalState() != null)
                .map(this::terminalUpdateParams)
                .toArray(MapSqlParameterSource[]::new);
        if (terminals.length > 0) {
            jdbcTemplate.batchUpdate(TERMINAL_UPDATE, terminals);
        }
    }

    private MapSqlParameterSource terminalUpdateParams(CompletionResult result) {
        return new MapSqlParameterSource()
                .addValue("state", Objects.requireNonNull(result.terminalState()).name())
                .addValue("finishedAt", dialect.splitTimestamp(result.finishedAt()))
                .addValue("executionId", result.executionId().value())
                .addValue("createdAt", dialect.splitTimestamp(result.executionCreatedAt()));
    }

    private void offerRetries(List<CompletionResult> winners) {
        MapSqlParameterSource[] retries = winners.stream()
                .filter(r -> r.retry() != null)
                .map(r -> JdbcSupport.readyEntryParams(Objects.requireNonNull(r.retry()), dialect))
                .toArray(MapSqlParameterSource[]::new);
        if (retries.length > 0) {
            jdbcTemplate.batchUpdate(JdbcSupport.READY_INSERT, retries);
        }
    }

    @Override
    public List<Lease> findByNodes(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return List.of();
        }
        return jdbcTemplate.query("""
                SELECT execution_id, job_key, node_id, epoch, attempt_number, claimed_at, cancel_requested
                FROM mohs_lease WHERE node_id IN (:nodeIds)
                """, new MapSqlParameterSource("nodeIds", nodeIds), (rs, _) -> mapLease(rs));
    }

    @Override
    public Map<JobKey, Integer> countByJob(Collection<JobKey> jobKeys) {
        if (jobKeys.isEmpty()) {
            return Map.of();
        }
        Map<JobKey, Integer> counts = LinkedHashMap.newLinkedHashMap(jobKeys.size());
        jdbcTemplate.query("""
                SELECT job_key, COUNT(*) AS leases FROM mohs_lease
                WHERE job_key IN (:jobKeys) GROUP BY job_key
                """, new MapSqlParameterSource("jobKeys", jobKeys.stream().map(JobKey::value).toList()),
                rs -> {
                    counts.put(JobKey.of(rs.getString("job_key")), rs.getInt("leases"));
                });
        return counts;
    }

    @Override
    public boolean requestCancellation(ExecutionId id) {
        Objects.requireNonNull(id, "id");
        return jdbcTemplate.update("UPDATE mohs_lease SET cancel_requested = :flag WHERE execution_id = :executionId",
                new MapSqlParameterSource().addValue("flag", true).addValue("executionId", id.value())) == 1;
    }

    @Override
    public Set<ExecutionId> findCancelRequested(Collection<ExecutionId> ids) {
        if (ids.isEmpty()) {
            return Set.of();
        }
        Set<ExecutionId> flagged = new LinkedHashSet<>();
        for (List<String> chunk : JdbcSupport.chunksOf(ids.stream().map(ExecutionId::value).toList())) {
            flagged.addAll(jdbcTemplate.query("""
                    SELECT execution_id FROM mohs_lease
                    WHERE execution_id IN (:ids) AND cancel_requested = :flag
                    """, new MapSqlParameterSource().addValue("ids", chunk).addValue("flag", true),
                    (rs, _) -> ExecutionId.of(rs.getString("execution_id"))));
        }
        return flagged;
    }

    private Lease mapLease(ResultSet rs) throws SQLException {
        return new Lease(
                ExecutionId.of(rs.getString("execution_id")),
                JobKey.of(rs.getString("job_key")),
                rs.getString("node_id"),
                rs.getLong("epoch"),
                rs.getInt("attempt_number"),
                Objects.requireNonNull(dialect.readSplitTimestamp(rs, "claimed_at"), "claimed_at"),
                rs.getBoolean("cancel_requested"));
    }
}
