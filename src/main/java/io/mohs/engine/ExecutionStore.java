package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import io.mohs.core.execution.Attempt;
import io.mohs.core.execution.Execution;
import io.mohs.core.execution.ExecutionId;
import io.mohs.core.execution.ExecutionState;
import io.mohs.core.job.JobKey;

/**
 * Persistência de {@link Execution} — Repository (PoEAA), porta que
 * {@code io.mohs.jdbc} implementa (Data Mapper). {@link #insert} é o
 * "insert do terminal" da cláusula 4 da ADR-0003 (transacional por
 * participação): entra na transação já ativa do chamador quando existe
 * uma (mesmo {@code DataSource}), ou auto-commit sem ela — nenhuma
 * transação própria é aberta aqui.
 *
 * <p>Transição para {@code RUNNING} continua fora desta porta —
 * {@code JdbcClaimer} cruza {@code mohs_executions} e {@code
 * mohs_job_definitions} numa única query, não cabe numa porta de
 * entidade só (ADR-0016/ADR-0018/ADR-0020). {@link #complete} é a
 * transição de conclusão (ADR-0024): toca só {@code mohs_executions} +
 * {@code mohs_attempts}, dono natural é esta porta. Payload não é campo
 * de {@link Execution} (não é parte do contrato M1) — {@link #insert}
 * grava, {@link #findPayload} lê de volta; nenhum outro método desta
 * porta o expõe.
 */
public interface ExecutionStore {

    /** Grava a execução e o payload serializado; {@code execution.attempts()} deve estar vazio (ainda não disparou). */
    Execution insert(Execution execution, Object payload);

    Optional<Execution> find(ExecutionId id);

    /**
     * A execução já gravada para {@code (jobKey, idempotencyKey)}, se
     * houver — a leitura do Idempotent Receiver (EIP): quando o índice
     * único do schema rejeita um {@link #insert} duplicado, o chamador
     * recupera por aqui a execução original pra devolver o mesmo recibo.
     * A unicidade é do banco, nunca de um SELECT prévio (corrida TOCTOU).
     */
    Optional<Execution> findByIdempotencyKey(JobKey jobKey, String idempotencyKey);

    /**
     * Reconstitui o payload gravado por {@link #insert} — o único jeito de
     * {@link Engine} obter o que passar pra {@link Dispatcher#dispatch}.
     * Vazio se a execução não existir; lança se a classe do payload
     * (coluna {@code payload_type}) sumiu do classpath — {@link Engine}
     * trata isso como falha terminal da execução, não deixa o ciclo
     * inteiro cair por causa de uma execução com payload corrompido.
     */
    Optional<Object> findPayload(ExecutionId id);

    /**
     * Grava {@code fired_at} — metadado, não transição de estado (sem CAS
     * guardado, ao contrário de {@link #complete}). {@link Execution#firedAt}
     * fica {@code null} enquanto a execução não disparou de verdade
     * (claim/lease não conta); {@link Dispatcher} chama isto no início do
     * dispatch, antes de invocar o handler.
     */
    void markFired(ExecutionId id, Instant firedAt);

    /**
     * Transiciona uma {@code Execution RUNNING} para um estado terminal ou
     * para {@code RETRY_SCHEDULED} (ADR-0033), grava o {@link Attempt} e
     * libera a vaga de concorrência do job na mesma operação
     * ({@code JobStore.decrementRunningExecutions}, ADR-0025) — único
     * caminho de conclusão, usado tanto pelo reclaim do reaper (ADR-0012)
     * quanto pelo dispatch normal. Quando o destino é
     * {@code RETRY_SCHEDULED}, {@link CompletionRequest#retryAt} vira o
     * novo {@code scheduled_at} na mesma transição: o backoff aterrissa
     * junto do CAS, nunca numa escrita separada que poderia se perder.
     *
     * <p>CAS por {@code WHERE state = 'RUNNING'}, sempre — não existe
     * chamador legítimo concluindo a partir de outro estado, por isso
     * não é parâmetro. Grava o {@link Attempt} só se o CAS passar: uma
     * conclusão concorrente (outro caminho já mudou o estado) não deixa
     * Attempt órfão.
     *
     * @return {@code true} se a transição ocorreu; {@code false} se a
     *         execução já não estava mais {@code RUNNING}.
     */
    boolean complete(CompletionRequest request, JobStore jobStore);

    /**
     * Como {@link #complete}, para muitas execuções de uma vez — o
     * reclaim do reaper (ADR-0012) transiciona dezenas/centenas de
     * execuções órfãs na mesma chamada; um {@link #complete} por execução
     * é round-trip por execução (DBTUNE-14, medido em {@code
     * LivenessLoadHarness}: o índice do reaper deixou a query de seleção
     * ~42x mais rápida no Postgres, mas o throughput fim-a-fim quase não
     * mudeu — o gargalo tinha migrado pra cá). Mesma disciplina de CAS
     * por request: uma execução que perdeu a corrida (conclusão
     * concorrente por outro caminho) sai silenciosamente do retorno, sem
     * erro — nunca grava {@link Attempt} nem libera vaga de concorrência
     * pra quem não transicionou de verdade.
     *
     * @return os ids das execuções cuja transição realmente ocorreu —
     *         subconjunto de {@code requests}, possivelmente vazio.
     */
    Set<ExecutionId> completeAll(List<CompletionRequest> requests, JobStore jobStore);

    /**
     * Um pedido de conclusão — a forma única de {@link #complete} e
     * {@link #completeAll}. {@code retryAt} anda junto do estado que o
     * exige: {@code RETRY_SCHEDULED} sem hora de retry seria re-claim
     * imediato sem backoff, e hora de retry num estado terminal seria
     * escrita silenciosamente ignorada — as duas combinações são bug do
     * chamador, rejeitadas na construção.
     *
     * <p>{@code expectedLeaseExpiresAt} é o fence anti-ABA (ADR-0033, DDIA
     * cap. 8): com {@code RETRY_SCHEDULED} re-claimável, "state = RUNNING"
     * deixou de identificar <em>qual</em> encarnação está rodando — o
     * reaper preenche com a lease que observou expirada, e o CAS só vence
     * se ela ainda for a mesma (re-claim concorrente troca a lease e
     * protege a encarnação nova). {@code null} = sem fence — o caminho do
     * dispatcher, que conclui a própria encarnação que executou.
     *
     * <p>{@code rearmNextFireAt} rearma a corrente fixed-delay (ADR-0035):
     * conclusão terminal de ocorrência {@code afterFinish} grava
     * {@code next_fire_at = fim + interval} na mesma transação do CAS —
     * mesmo racional do {@code retryAt}: escrita separada entre o CAS e um
     * crash se perde. Quem calcula é quem tem a definição em mãos
     * (Dispatcher/reaper); só se aplica se o CAS vencer, guardado por
     * {@code next_fire_at IS NULL} ({@code JobStore#armNextFire}).
     * {@code RETRY_SCHEDULED} nunca rearma — a corrente continua pelo
     * retry.
     */
    record CompletionRequest(ExecutionId id, JobKey jobKey, Attempt attempt, ExecutionState newState,
            @Nullable Instant retryAt, @Nullable Instant expectedLeaseExpiresAt, @Nullable Instant rearmNextFireAt) {

        public CompletionRequest {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(newState, "newState");
            if (newState == ExecutionState.RETRY_SCHEDULED && retryAt == null) {
                throw new IllegalArgumentException("RETRY_SCHEDULED requires retryAt — the rescheduled scheduled_at is the backoff");
            }
            if (newState != ExecutionState.RETRY_SCHEDULED && retryAt != null) {
                throw new IllegalArgumentException("retryAt only applies to RETRY_SCHEDULED, got " + newState);
            }
            if (newState == ExecutionState.RETRY_SCHEDULED && rearmNextFireAt != null) {
                throw new IllegalArgumentException("rearmNextFireAt only applies to terminal states — the retry chain is still alive");
            }
        }

        /** Conclusão sem retry (terminal) e sem fence — a forma do dispatcher pra sucesso/falha terminal. */
        public CompletionRequest(ExecutionId id, JobKey jobKey, Attempt attempt, ExecutionState newState) {
            this(id, jobKey, attempt, newState, null, null, null);
        }

        /** Forma com fence e sem rearme — quem conclui sem agenda {@code afterFinish} em mãos. */
        public CompletionRequest(ExecutionId id, JobKey jobKey, Attempt attempt, ExecutionState newState,
                @Nullable Instant retryAt, @Nullable Instant expectedLeaseExpiresAt) {
            this(id, jobKey, attempt, newState, retryAt, expectedLeaseExpiresAt, null);
        }

        /** Retry sem fence — a forma do dispatcher pra falha com orçamento. */
        public CompletionRequest(ExecutionId id, JobKey jobKey, Attempt attempt, ExecutionState newState, @Nullable Instant retryAt) {
            this(id, jobKey, attempt, newState, retryAt, null, null);
        }
    }

    /**
     * Renova a lease das execuções {@code RUNNING} deste node (ADR-0012:
     * "o motor renova a lease de toda Execution RUNNING a cada ciclo de
     * poll" — em lote, cluster-wide, nunca por job). O guard
     * {@code state = 'RUNNING' AND node_id = :nodeId} é o fence natural:
     * renovação perde para qualquer reclaim/conclusão concorrente e nunca
     * ressuscita a lease de uma execução que já saiu deste node.
     *
     * @return os ids efetivamente renovados — subconjunto de {@code ids};
     *         um id ausente perdeu a posse (reclaim do reaper ou conclusão
     *         concorrente) e o chamador decide o que logar.
     */
    Set<ExecutionId> renewLeases(String nodeId, List<ExecutionId> ids, Instant leaseExpiresAt);

    /**
     * Cancela uma execução que ainda não roda — CAS
     * {@code ENQUEUED/RETRY_SCHEDULED → CANCELLED}, o mesmo padrão que
     * {@code JobStore.remove} aplica por job (ADR-0034, por id). Perde para
     * qualquer claim concorrente (a linha já virou {@code RUNNING}) — o
     * chamador então cai no caminho da flag ({@link #requestCancellation}).
     *
     * @return {@code true} se ESTA chamada transicionou a linha
     */
    boolean cancelIfPending(ExecutionId id);

    /**
     * Pede cancelamento cooperativo de uma execução {@code RUNNING} —
     * grava {@code cancel_requested}, o único canal que alcança o node dono
     * em outro processo (ADR-0034). O node dono observa via
     * {@link #findCancelRequested} a cada tick e levanta o sinal
     * {@code MANUAL}; nada é interrompido — cancel é cooperativo por
     * contrato. A flag não é limpa nunca: sobrevive como registro histórico
     * mesmo se a conclusão vencer a corrida.
     *
     * @return {@code true} se havia uma linha {@code RUNNING} pra marcar
     */
    boolean requestCancellation(ExecutionId id);

    /**
     * Os ids de {@code ids} com {@code cancel_requested} ligado — o poll em
     * lote do tick sobre o próprio in-flight (ADR-0034); staleness efetiva
     * do cancel cooperativo ≤ 1 poll-interval.
     */
    Set<ExecutionId> findCancelRequested(List<ExecutionId> ids);

    /**
     * Busca várias execuções por id numa única consulta — evita N+1 quando
     * o chamador já tem a lista de ids em mãos (ex.: hidratar o resultado
     * de um claim em lote). Limitado pelo tamanho de {@code ids}, por isso
     * {@code List}, não {@code Stream} (ver {@link #findAll} pra leitura
     * não limitada). Ordem do retorno não é garantida — quem chama reordena
     * se precisar.
     */
    List<Execution> findByIds(List<ExecutionId> ids);

    /**
     * Stream sobre um cursor aberto — não materializa em memória de uma
     * vez. Quem chama é dono do ciclo de vida (try-with-resources).
     * DBTUNE-7: no Postgres, isso só é verdade se a chamada rodar dentro de
     * uma transação (autocommit desligado) — fora dela, o driver
     * materializa o resultado inteiro antes de devolver o primeiro item,
     * apesar do {@code fetchSize} configurado do lado do template.
     */
    Stream<Execution> findByJobKey(JobKey jobKey);

    /** Ver {@link #findByJobKey} sobre ciclo de vida do stream. */
    Stream<Execution> findAll();

    /**
     * Página filtrada, ordenada por {@code id} (UUIDv7, ordenável
     * cronologicamente) decrescente — mais recente primeiro. {@code
     * cursor} restringe a {@code id < cursor} quando presente (keyset
     * pagination, uma coluna só: nenhuma chave composta precisa, porque o
     * próprio id já ordena no tempo). Todo filtro é opcional; {@code
     * limit} não — quem chama decide se pede {@code size + 1} pra
     * detectar se há próxima página (esta porta não sabe de {@code
     * CursorPage}, isso é decisão de {@code io.mohs.rest}).
     */
    List<Execution> findPage(@Nullable JobKey jobKey, @Nullable ExecutionState status, @Nullable Instant from,
            @Nullable Instant to, @Nullable ExecutionId cursor, int limit);
}
