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

    /** Uma lease viva. {@code attemptNumber} e {@code priority} vieram da entrada de fila que o claim consumiu (§5.3) — é o que deixa o reaper reconstruir a entrada de requeue sem ler a história. */
    record Lease(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber, int priority,
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
     * {@code executionCreatedAt} casa a linha do UPDATE advisory (ele lidera a PK) —
     * carregado em memória desde a leitura de payload (PLAN.md S5.1: a
     * poda é por IGUALDADE; derivar do id só funciona pra UUIDv7 real);
     * {@code null} = atualizar sem poda (caminho frio do reaper, que não
     * carregou a história — o índice de {@code execution_id} sonda as
     * partições). {@code batchId} faz a conclusão terminal contar no lote
     * (ADR-0043) na MESMA transação, como sempre; {@code rearmNextFireAt}
     * rearma a corrente fixed-delay (ADR-0035) idem — escrita separada
     * entre o commit e um crash se perderia.
     */
    record CompletionResult(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber,
            Instant startedAt, Instant finishedAt, ExecutionState outcome, @Nullable String errorType, @Nullable String error,
            @Nullable ExecutionState terminalState, @Nullable Instant executionCreatedAt,
            WorkQueue.@Nullable ReadyEntry retry, @Nullable String batchId, @Nullable Instant rearmNextFireAt) {
        public CompletionResult {
            Objects.requireNonNull(executionId, "executionId");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(startedAt, "startedAt");
            Objects.requireNonNull(finishedAt, "finishedAt");
            if (terminalState != null && retry != null) {
                throw new IllegalArgumentException("a result is terminal OR schedules a retry — never both");
            }
            if (terminalState == null && retry == null) {
                throw new IllegalArgumentException("a non-terminal result must carry the retry entry — "
                        + "without it the execution would end up owned by nobody and queued nowhere");
            }
            if (terminalState == null && rearmNextFireAt != null) {
                throw new IllegalArgumentException("rearmNextFireAt only applies to terminal results — the retry chain is still alive");
            }
        }

        /** Forma sem lote nem rearme — a maioria das conclusões. */
        public CompletionResult(ExecutionId executionId, JobKey jobKey, String nodeId, long epoch, int attemptNumber,
                Instant startedAt, Instant finishedAt, ExecutionState outcome, @Nullable String errorType,
                @Nullable String error, @Nullable ExecutionState terminalState, @Nullable Instant executionCreatedAt,
                WorkQueue.@Nullable ReadyEntry retry) {
            this(executionId, jobKey, nodeId, epoch, attemptNumber, startedAt, finishedAt, outcome, errorType, error,
                    terminalState, executionCreatedAt, retry, null, null);
        }
    }

    /**
     * O que a conclusão produziu (mesmo contrato da era pré-split,
     * ADR-0043/0047): {@code owned} é o veredito do fence — falso quando a
     * encarnação já era de outro (reaper/requeue passou antes) e NADA foi
     * gravado; {@code closedBatch} só vem preenchido pra ÚNICA conclusão
     * que zerou os pendentes do lote — o saldo sobe da própria transação,
     * nunca de releitura (duas releituras concorrentes se achariam ambas a
     * fechadora).
     */
    record Completion(boolean owned, @Nullable BatchCounters closedBatch) {

        public static final Completion FENCED_OUT = new Completion(false, null);

        public static Completion owned(@Nullable BatchCounters closedBatch) {
            return new Completion(true, closedBatch);
        }
    }

    /**
     * A transação de conclusão (§7.5-3, em lote §7.6): {@code DELETE} das
     * leases cercado por {@code (node_id, epoch)} — a contagem diz
     * exatamente quais este chamador ainda possuía —, {@code INSERT} dos
     * attempts confirmados, {@code UPDATE} terminal advisory da história
     * (casado por {@code executionCreatedAt}, que lidera a PK), contagem de
     * lote (ADR-0043) e rearme fixed-delay (ADR-0035) — tudo ou nada.
     * Resultado com {@code owned = false} foi de encarnação perdida e é
     * descartado — detectado, nunca silenciosamente perdido (§7.6).
     * {@code jobStore} entra pelo mesmo motivo da era pré-split: o rearme
     * participa da transação por compartilhar o {@code DataSource}.
     */
    Map<ExecutionId, Completion> complete(List<CompletionResult> results, JobStore jobStore);

    /** As leases destes nós — a matéria-prima do drain visível em {@code GET /nodes}. */
    List<Lease> findByNodes(Collection<String> nodeIds);

    /**
     * As leases cujo dono NÃO está em {@code aliveNodeIds} — a seleção de
     * candidatos do reaper (ADR-0051 sobre a mesa nova): node ausente da
     * lista é morto por definição (linha purgada, promessa vencida — quem
     * decide vivo/morto é o chamador, lendo {@code mohs_nodes}). Limitada
     * a {@code limit} por chamada, mais antiga primeiro ({@code claimed_at})
     * — morte em massa drena em passadas, nunca numa transação sem teto.
     */
    List<Lease> findOrphaned(Collection<String> aliveNodeIds, int limit);

    /** Cap derivado (§5.7): contagem por job das leases vivas, lida UMA vez por rodada — nunca por candidato. */
    Map<JobKey, Integer> countByJob(Collection<JobKey> jobKeys);

    /** Cancel cooperativo (ADR-0034) sobre a posse: liga a flag se a lease existe; {@code false} = não está em execução. */
    boolean requestCancellation(ExecutionId id);

    /** O poll do tick (ADR-0034): destes ids em voo, quais têm ordem de cancel pendente. */
    Set<ExecutionId> findCancelRequested(Collection<ExecutionId> ids);
}
