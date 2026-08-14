package io.mohs.engine;

import java.util.List;

import io.mohs.core.execution.Execution;

/**
 * Aquisição sem contenção de execuções devidas — a metade "claim" de M3
 * (dispatch fica pra frente, quando invocação de handler tiver uma porta
 * definida). Não é Repository de uma entidade só: cruza
 * {@code mohs_executions} e {@code mohs_job_definitions} numa única
 * transação, então não entra em nenhum {@code *Store} — porta própria,
 * mesmo padrão porta/adaptador dos stores ({@code io.mohs.jdbc} implementa).
 *
 * <p>Claim e a transição para {@code RUNNING} são um único statement
 * atômico (ADR-0016); mutex por job é decidido dentro da mesma transação
 * (ADR-0017/0018/0020) — não há um "claim provisório" visível de fora
 * desta chamada.
 */
public interface Claimer {

    /**
     * Reivindica até {@code batchSize} execuções {@code ENQUEUED} e devidas,
     * respeitando exclusão mútua por job ({@code allowConcurrentExecutions}/
     * {@code maxConcurrentExecutions}), ordenadas por prioridade e depois
     * por horário agendado. As execuções devolvidas já estão
     * {@code RUNNING}, com lease e {@code nodeId} gravados.
     *
     * @param nodeId    identidade do node chamador — de onde ela vem
     *                  (estável entre reinícios ou não) é decisão da etapa
     *                  de liveness, não desta porta.
     * @param batchSize teto de execuções reivindicadas nesta chamada.
     */
    List<Execution> claim(String nodeId, int batchSize);
}
