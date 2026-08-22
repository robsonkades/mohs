package io.mohs.engine;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * A POSSE ({@code mohs_lease}) — porta da Phase 5 (ADR-A, §6.2/§6.3/
 * §18.3 do redesign). A tabela contém exatamente o trabalho em execução
 * no cluster, limitada por {@code nodes × dispatch-concurrency} — milhares
 * de linhas, nunca milhões: é o que torna o cap de concorrência DERIVADO
 * ({@link #countByJob}, §5.7) um index-only scan sempre em cache e mata o
 * contador quente {@code running_execution_count} (ADR-0018/0020/0025).
 *
 * <p>O fence é {@code (node_id, epoch)} (§6.3, o fencing token do DDIA
 * cap. 8 — sucessor do {@code (node_id, fired_at)} da ADR-0051): toda
 * escrita sobre trabalho possuído carrega o par, e um zumbi (reapado que
 * voltou) perde TODAS porque carrega epoch velho. Deletar a lease É
 * liberar a vaga — não existe mais slot a devolver.
 */
public interface LeaseStore {

    /** Uma lease viva. {@code attemptNumber} veio da entrada de fila que o claim consumiu (§5.3). */
    record Lease(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber,
            Instant claimedAt, boolean cancelRequested) {
        public Lease {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(claimedAt, "claimedAt");
        }
    }

    /**
     * O resultado de um attempt, pronto pra durabilidade. {@code outcome}
     * é o desfecho DO ATTEMPT ({@code SUCCEEDED}/{@code FAILED}/
     * {@code CANCELLED} — o vocabulário que {@code Attempt} aceita), tipado
     * de propósito: um typo em string gravaria limpo e explodiria meses
     * depois no {@code valueOf} do detail view. {@code terminalState} é o
     * estado advisory gravado na história e {@code null} quando o desfecho
     * NÃO é terminal — nesse caso {@code retry} é obrigatório e o PRÓPRIO
     * {@link #complete} reinsere a entrada na fila, NA MESMA transação
     * (§4.3: retry = insert na fila; fora da transação, um crash entre o
     * commit da conclusão e o insert deixaria a execução sem lease, sem
     * fila e não-terminal — órfã invisível pra sempre). O chamador NUNCA
     * chama {@link WorkQueue#offer} pra um retry: duplicaria a PK de
     * {@code mohs_ready}.
     * {@code executionCreatedAt} poda a partição do UPDATE advisory —
     * carregado em memória desde a leitura de payload (PLAN.md S5.1: a
     * poda é por IGUALDADE; derivar do id só funciona pra UUIDv7 real).
     */
    record CompletionResult(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber,
            Instant startedAt, Instant finishedAt, ExecutionState outcome, @Nullable String errorType, @Nullable String error,
            @Nullable ExecutionState terminalState, Instant executionCreatedAt,
            WorkQueue.@Nullable ReadyEntry retry) {
        public CompletionResult {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(startedAt, "startedAt");
            Objects.requireNonNull(finishedAt, "finishedAt");
            Objects.requireNonNull(executionCreatedAt, "executionCreatedAt");
            if (terminalState != null && retry != null) {
                throw new IllegalArgumentException("a result is terminal OR schedules a retry — never both");
            }
            if (terminalState == null && retry == null) {
                throw new IllegalArgumentException("a non-terminal result must carry the retry entry — "
                        + "without it the execution would end up owned by nobody and queued nowhere");
            }
        }
    }

    /**
     * A transação de conclusão (§7.5-3, em lote §7.6): {@code DELETE} das
     * leases cercado por {@code (node_id, epoch)} — o {@code RETURNING}
     * diz exatamente quais este chamador ainda possuía —, {@code INSERT}
     * dos attempts confirmados e {@code UPDATE} terminal advisory da
     * história (podado por partição via {@code executionCreatedAt}).
     * Devolve os ids cujo fence VENCEU; um resultado fora do conjunto foi
     * de encarnação perdida (reaper/requeue passou antes) e é descartado —
     * detectado, nunca silenciosamente perdido (§7.6).
     */
    Set<ExecutionId> complete(List<CompletionResult> results);

    /** As leases destes nós — a matéria-prima do reaper (nós mortos) e do drain visível em {@code GET /nodes}. */
    List<Lease> findByNodes(Collection<String> nodeIds);

    /** Cap derivado (§5.7): contagem por job das leases vivas, lida UMA vez por rodada — nunca por candidato. */
    Map<JobKey, Integer> countByJob(Collection<JobKey> jobKeys);

    /** Cancel cooperativo (ADR-0034) sobre a posse: liga a flag se a lease existe; {@code false} = não está em execução. */
    boolean requestCancellation(ExecutionId id);

    /** O poll do tick (ADR-0034): destes ids em voo, quais têm ordem de cancel pendente. */
    Set<ExecutionId> findCancelRequested(Collection<ExecutionId> ids);
}
