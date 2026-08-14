package io.mohs.engine;

import java.util.List;

import io.mohs.core.execution.Execution;

/**
 * Reclama {@code Execution RUNNING} cuja lease expirou — nó morto,
 * partição de rede, ou Handler zumbi que ignora cancelamento (ADR-0012).
 * Não é Repository de uma entidade só: orquestra {@link ExecutionStore}
 * (grava o Attempt e transiciona o estado, via {@link
 * ExecutionStore#complete}) e {@link JobStore} (libera a vaga de
 * concorrência do job, ADR-0025) — mesmo motivo de {@link Claimer} não
 * ser um {@code *Store}.
 *
 * <p>Sem {@code batchSize}: consulta uma tabela só, sem {@code JOIN},
 * caminho triste/infrequente — não é hot path como {@link
 * Claimer#claim}, não precisa da abstração de dialeto que o claim
 * precisa para {@code LIMIT}/{@code TOP}.
 */
public interface Reaper {

    /**
     * Reclama toda {@code Execution RUNNING} com lease expirada: grava um
     * {@code Attempt} {@code FAILED} sintético ("lease expirada — node
     * presumido morto") e transiciona a execução para {@code FAILED}
     * terminal — nunca {@code RETRY_SCHEDULED} nesta etapa (ADR-0026: a
     * claim query ainda não reconhece esse estado como candidato).
     *
     * @return as execuções reclamadas nesta chamada, já no estado final.
     */
    List<Execution> reclaimExpired();
}
