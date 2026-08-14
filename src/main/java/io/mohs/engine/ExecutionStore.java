package io.mohs.engine;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

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
 * de {@link Execution} (não é parte do contrato M1); carregá-lo de volta
 * pra dispatch é decisão de quem consome, não desta porta.
 */
public interface ExecutionStore {

    /** Grava a execução e o payload serializado; {@code execution.attempts()} deve estar vazio (ainda não disparou). */
    Execution insert(Execution execution, Object payload);

    Optional<Execution> find(ExecutionId id);

    /**
     * Grava {@code fired_at} — metadado, não transição de estado (sem CAS
     * guardado, ao contrário de {@link #complete}). {@link Execution#firedAt}
     * fica {@code null} enquanto a execução não disparou de verdade
     * (claim/lease não conta); {@link Dispatcher} chama isto no início do
     * dispatch, antes de invocar o handler.
     */
    void markFired(ExecutionId id, Instant firedAt);

    /**
     * Transiciona uma {@code Execution RUNNING} para um estado terminal
     * (ou, quando a claim query reconhecer o estado como candidato de
     * novo — ainda não, ADR-0026 — {@code RETRY_SCHEDULED}), grava o
     * {@link Attempt} e libera a vaga de concorrência do job na mesma
     * operação ({@code JobStore.decrementRunningExecutions}, ADR-0025) —
     * único caminho de conclusão, usado tanto pelo reclaim do reaper
     * (ADR-0012) quanto pelo dispatch normal quando existir.
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
    boolean complete(ExecutionId id, JobKey jobKey, Attempt attempt, ExecutionState newState, JobStore jobStore);

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

    /** Um pedido de conclusão dentro de {@link #completeAll} — mesmos campos de {@link #complete}, menos o id (chave do lote). */
    record CompletionRequest(ExecutionId id, JobKey jobKey, Attempt attempt, ExecutionState newState) {

        public CompletionRequest {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(jobKey, "jobKey");
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(newState, "newState");
        }
    }

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
}
