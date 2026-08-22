package io.mohs.store.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
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
import io.mohs.store.jdbc.dialect.JdbcDialect;

/**
 * {@link Reaper} sobre {@code mohs_executions} (ADR-0012/0025/0033;
 * critério de morte revisto pela ADR-0051). Mesma forma de transação
 * própria de {@link JdbcClaimer} — quem chama
 * {@link #reclaimExpired} não tem transação ativa (disparado pelo ciclo
 * de poll do motor, não por um chamador com contexto transacional já
 * aberto). {@code executionStore}/{@code jobStore} precisam apontar pro
 * mesmo {@code DataSource} passado aqui — é assim que participam da
 * mesma transação (mesmo padrão de {@link JdbcClaimer}).
 *
 * <p><b>ADR-0051:</b> candidato a reclaim é execução {@code RUNNING} de
 * NODE MORTO — aquele cuja promessa em {@code mohs_nodes.expires_at}
 * venceu — não mais a execução cuja própria lease expirou: a liveness
 * mora no nó, e handler saudável de node vivo nunca é reclamado, sem
 * custar uma renovação por execução. Linha de node de jar antigo
 * (sem {@code expires_at}, gravada antes da migração V2) responde pela
 * staleness do {@code last_heartbeat_at} com corte de {@code lease-ttl}
 * — tolerância de versão mista durante rolling upgrade.
 *
 * <p>DBTUNE-14: lê ({@link ExecutionStore#findByIds}) e escreve ({@link
 * ExecutionStore#completeAll}) em lote, não candidato a candidato — medido
 * em {@code LivenessLoadHarness}: o índice {@code idx_mohs_executions_reaper}
 * (DBTUNE-10) deixou a seleção de candidatos ~42x mais rápida no Postgres,
 * mas o throughput fim-a-fim quase não mudeu, porque o gargalo real era o
 * round-trip por candidato daqui, não a query de seleção.
 */
public final class JdbcReaper implements Reaper {

    private static final String LEASE_EXPIRED_ERROR = "node lease expired — node presumed dead";

    /**
     * Teto de reclaims por ciclo — morte de nó em massa não vira uma
     * transação sem limite de locks (backpressure em toda borda); o
     * excedente drena nos ticks seguintes, mais antigo primeiro (o
     * {@code ORDER BY e.id} é UUIDv7, ordenado no tempo). Package-private
     * pro teste de fronteira.
     */
    static final int RECLAIM_LIMIT = 500;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final JdbcDialect dialect;
    private final Clock clock;
    private final ExecutionStore executionStore;
    private final JobStore jobStore;
    /** Corte de staleness pra linha de node de jar antigo, sem {@code expires_at} (tolerância de versão mista, ADR-0051). */
    private final Duration legacyNodeStaleness;

    public JdbcReaper(DataSource dataSource, JdbcDialect dialect, Clock clock, ExecutionStore executionStore, JobStore jobStore,
            Duration legacyNodeStaleness) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        // mesmo raciocínio da DBTUNE-4 em JdbcClaimer: CAS guardado assume
        // "última escrita vence" (READ COMMITTED), não herda o default do banco.
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executionStore = Objects.requireNonNull(executionStore, "executionStore");
        this.jobStore = Objects.requireNonNull(jobStore, "jobStore");
        this.legacyNodeStaleness = Objects.requireNonNull(legacyNodeStaleness, "legacyNodeStaleness");
    }

    @Override
    public List<Reaper.Reclaimed> reclaimExpired() {
        // TransactionCallback aqui nunca devolve null (reclaimWithinTransaction
        // sempre retorna uma lista) — requireNonNull só documenta esse invariante
        // pro @NullMarked, já que o contrato de execute() em si é @Nullable (JAVA-8).
        return Objects.requireNonNull(transactionTemplate.execute(_ -> reclaimWithinTransaction()));
    }

    private List<Reaper.Reclaimed> reclaimWithinTransaction() {
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

        Map<ExecutionId, ExecutionStore.Completion> verdicts =
                executionStore.completeAll(List.copyOf(requestById.values()), jobStore);
        Set<ExecutionId> completedIds = verdicts.entrySet().stream()
                .filter(e -> e.getValue().applied())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        return buildReclaimedList(candidates, executionsById, requestById, completedIds);
    }

    /**
     * ADR-0033: o attempt sintético do reclaim consome orçamento como
     * qualquer outro — mesma decisão ({@link RetrySchedule}) do Dispatcher:
     * orçamento restante reagenda com backoff, esgotado é FAILED terminal.
     * Duas ordens vencem o orçamento (ADR-0034): {@code cancel_requested}
     * termina {@code CANCELLED} — o nó morreu, mas a ordem do operador já
     * estava dada; reagendar seria desobedecê-la (attempt {@code CANCELLED}
     * com error nulo, invariante de {@link Attempt}) — e job aposentado
     * ({@code retired}) nunca reagenda: {@code RETRY_SCHEDULED} de job
     * removido ficaria preso pra sempre (claim filtra retired, o cancel do
     * remove já passou). Todo request leva a posse observada —
     * {@code (node_id, fired_at)} — como fence anti-ABA (ADR-0051): se um
     * re-claim concorrente já deu nova encarnação à execução, o
     * CAS do reclaim perde — nunca mata a encarnação nova saudável (DDIA
     * cap. 8: estado reentrante exige fencing token).
     */
    private static ExecutionStore.CompletionRequest reclaimRequest(ExpiredCandidate candidate, Execution execution, Instant now) {
        int attemptNumber = execution.attempts().size() + 1;
        Instant startedAt = Objects.requireNonNullElse(execution.firedAt(), now);
        ExecutionId id = ExecutionId.of(candidate.id());
        JobKey jobKey = JobKey.of(candidate.jobKey());
        // ADR-0035: reclaim terminal de ocorrência do scheduler em job fixed-delay
        // rearma a corrente — o "fim" de um zumbi é desconhecido, `now` (a
        // observação do reaper) ancora. Execução manual não é a corrente; job
        // aposentado nunca rearma (não dispara; a ressurreição via upsert cura).
        Instant rearmNextFireAt = !candidate.retired() && Execution.SCHEDULER_ACTOR.equals(execution.actor())
                ? candidate.rearmNextFireAt(now)
                : null;
        // ADR-0043: o membro que morre por lease expirada conta no lote como
        // qualquer outro — quem conta é a conclusão, e este é um caminho dela.
        String batchId = execution.batchId();
        if (candidate.cancelRequested()) {
            Attempt attempt = new Attempt(attemptNumber, startedAt, now, ExecutionState.CANCELLED, null);
            return new ExecutionStore.CompletionRequest(id, jobKey, attempt, ExecutionState.CANCELLED, null,
                    candidate.fence(), rearmNextFireAt).inBatch(batchId);
        }
        Attempt attempt = new Attempt(attemptNumber, startedAt, now, ExecutionState.FAILED, LEASE_EXPIRED_ERROR);
        if (candidate.retired()) {
            return new ExecutionStore.CompletionRequest(id, jobKey, attempt, ExecutionState.FAILED, null,
                    candidate.fence()).inBatch(batchId);
        }
        return RetrySchedule.nextRetryAt(attemptNumber, candidate.retries(), now)
                .map(retryAt -> new ExecutionStore.CompletionRequest(id, jobKey, attempt,
                        ExecutionState.RETRY_SCHEDULED, retryAt, candidate.fence()))
                .orElseGet(() -> new ExecutionStore.CompletionRequest(id, jobKey, attempt, ExecutionState.FAILED, null,
                        candidate.fence(), rearmNextFireAt))
                .inBatch(batchId);
    }

    /** Reconstrói o resultado localmente (sem consulta extra) — {@link ExecutionStore#completeAll} só confirma quais ids venceram o CAS. O flag de orçamento vem da mesma decisão de {@link #reclaimRequest}: terminal sem ser aposentado = orçamento esgotado. */
    private static List<Reaper.Reclaimed> buildReclaimedList(List<ExpiredCandidate> candidates, Map<String, Execution> executionsById,
            Map<String, ExecutionStore.CompletionRequest> requestById, Set<ExecutionId> completedIds) {
        List<Reaper.Reclaimed> reclaimed = new ArrayList<>(completedIds.size());
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
            boolean attemptsExhausted = request.newState() == ExecutionState.FAILED && !candidate.retired();
            reclaimed.add(new Reaper.Reclaimed(
                    new Execution(id, JobKey.of(candidate.jobKey()), request.newState(), scheduledAt, execution.firedAt(), attempts,
                            execution.actor(), execution.priority(), execution.idempotencyKey()),
                    attemptsExhausted));
        }
        return reclaimed;
    }

    /**
     * ADR-0051: candidato é execução {@code RUNNING} cujo dono NÃO está no
     * conjunto de nodes vivos — vivo é {@code expires_at > now} (a promessa
     * do heartbeat) ou, em linha de jar antigo sem {@code expires_at},
     * {@code last_heartbeat_at} mais novo que o corte de {@code lease-ttl};
     * node ausente de {@code mohs_nodes} (linha purgada, ADR-0041) é morto
     * por definição. {@code j.retries}/{@code j.retired} entram no mesmo
     * SELECT — a decisão de orçamento não paga uma consulta por candidato.
     * {@code e.node_id}/{@code e.fired_at} saem junto porque são o fence do
     * CAS de conclusão (ver {@link #reclaimRequest}).
     * Teto de {@link #RECLAIM_LIMIT} por ciclo via {@code TOP}/{@code LIMIT}
     * do dialeto (mesmo par de cláusulas do {@code findPage}).
     *
     * <p>{@code e.lease_expires_at IS NOT NULL} não filtra nada de verdade
     * (toda linha {@code RUNNING} tem lease gravada pelo UPDATE de claim —
     * o invariante que {@code countActiveByState} já usa desde a
     * DBTUNE-17): existe só pra IMPLICAR o predicado do índice parcial
     * {@code idx_mohs_executions_owner} em Postgres/SQL Server (DBTUNE-22
     * — sem o conjunct no predicado do índice, o CAS de conclusão cercado
     * o implicaria e o planner o escolhia no lugar do PK).
     */
    private List<ExpiredCandidate> selectExpiredCandidates(Instant now) {
        // ORDER BY e.id: reapers concorrentes acordam juntos numa morte de nó e
        // adquirem os mesmos row locks — ordem global determinística (JCIP cap.
        // 10, lock ordering) evita deadlock entre eles; a cadeia até o
        // batchUpdate preserva encounter order. Com o LIMIT, também garante
        // drenagem mais-antigo-primeiro (UUIDv7 ordena no tempo).
        return jdbcTemplate.query("""
                SELECT %se.id AS id, e.job_key AS job_key, e.node_id AS node_id, e.fired_at AS fired_at,
                       e.cancel_requested AS cancel_requested, j.retries AS retries, j.retired AS retired,
                       j.interval_duration AS interval_duration, j.interval_after_finish AS interval_after_finish
                FROM mohs_executions e
                JOIN mohs_job_definitions j ON j.job_key = e.job_key
                WHERE e.state = 'RUNNING' AND e.lease_expires_at IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM mohs_nodes n
                    WHERE n.node_id = e.node_id
                      AND (n.expires_at > :now
                           OR (n.expires_at IS NULL AND n.last_heartbeat_at > :legacyCutoff))
                )
                ORDER BY e.id
                %s""".formatted(dialect.topClause(), dialect.limitClause()),
                new MapSqlParameterSource()
                        .addValue("now", JdbcTimestamps.toUtcLocalDateTime(now))
                        .addValue("legacyCutoff", JdbcTimestamps.toUtcLocalDateTime(now.minus(legacyNodeStaleness)))
                        .addValue("limit", RECLAIM_LIMIT),
                (rs, _) -> new ExpiredCandidate(rs.getString("id"), rs.getString("job_key"),
                        rs.getString("node_id"), readFiredAt(rs),
                        rs.getBoolean("cancel_requested"), rs.getInt("retries"), rs.getBoolean("retired"),
                        rs.getString("interval_duration"), rs.getBoolean("interval_after_finish")));
    }

    private static @Nullable Instant readFiredAt(ResultSet rs) throws SQLException {
        LocalDateTime firedAt = rs.getObject("fired_at", LocalDateTime.class);
        return firedAt == null ? null : JdbcTimestamps.fromUtcLocalDateTime(firedAt);
    }

    /** As colunas de agenda saem no mesmo SELECT pelo mesmo motivo de {@code retries}/{@code retired}: a decisão de rearme (ADR-0035) não paga uma consulta por candidato. */
    private record ExpiredCandidate(String id, String jobKey, @Nullable String nodeId, @Nullable Instant firedAt,
            boolean cancelRequested, int retries,
            boolean retired, @Nullable String intervalDuration, boolean intervalAfterFinish) {

        /**
         * A posse da encarnação morta, como fence do CAS (ADR-0051) —
         * {@code null} só em linha anômala sem dono/fired_at, que degrada
         * pra conclusão não cercada (o CAS de estado ainda protege).
         */
        ExecutionStore.@Nullable OwnerFence fence() {
            return nodeId != null && firedAt != null ? new ExecutionStore.OwnerFence(nodeId, firedAt) : null;
        }

        /** {@code fim + interval} de uma agenda fixed-delay; {@code null} nas demais (nada a rearmar). */
        @Nullable Instant rearmNextFireAt(Instant now) {
            return intervalAfterFinish && intervalDuration != null ? now.plus(Duration.parse(intervalDuration)) : null;
        }
    }
}
